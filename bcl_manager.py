import logging
import ntpath
import argparse
import os
from os.path import basename
import shutil
from pathlib import Path
import subprocess
import re
import glob
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from s3_logging_handler import S3LoggingHandler

import utils

"""
bcl_manager.py is a file-watcher that runs on wey-001 for automated:

- Backup of raw .bcl data locally
- Conversion of raw .bcl data into .fastq
- Upload of .fastq files to S3 according to project code

"""

SALMONELLA_PROJECT_CODES = ["FZ2000"]


def convert_to_fastq(src_dir, dest_dir):
    """
        Converts an Illumina Bcl Run to Fastq using bcl-convert
    """
    return_code = subprocess.run([
        "bcl-convert",
        "--output-directory", dest_dir,
        "--bcl-input-directory", src_dir,
        "--sample-sheet", f"{src_dir}/SampleSheet.csv",
        "--bcl-sampleproject-subdirectories", "true",
        "--no-lane-splitting", "true"
    ]).returncode

    if return_code:
        raise Exception('bcl-convert failed: %s'%(return_code))   

def copy(src_dir, dest_dir):
    """
        Backup BclFiles to another directory
    """
    # Make sure we are not overwriting anything!
    if os.path.isdir(os.path.abspath(dest_dir)):
        raise Exception('Cannot backup Bcl, path exists: %s'%dest_dir)    
   
    shutil.copytree(src_dir, dest_dir)


def upload(fastq_bucket, s3_endpoint_url, key, project_code, instrument_id,
           run_number, run_id, flowcell_id, sequence_date, dirname):
    """
        Upload every subdirectory under src_dir that contains fastq.gz files to S3.
        Files are stored with URI: s3://{bucket}/{prefix}/{project_code}/{run_number}/{project_code}/

        The src_path should reference a directory with format yymmdd_instrumentID_runnumber_flowcellID/

        The project_code is the name of the subdirectory that contains the fastq files.

        A meta.json file is also uploaded to each project_code with schema:
        {
            "project_code": string,
            "instrument_id": string,
            "run_number": string,
            "run_id": string",
            "flowcell_id": string,
            "sequence_date": string,
            "upload_time": string
        }       
    """
    # Upload
    utils.upload_json(fastq_bucket,
                        f"{key}/meta.json",
                        s3_endpoint_url, 
                        {"project_code": project_code,
                        "instrument_id": instrument_id,
                        "run_number": run_number,
                        "run_id": run_id,
                        "flowcell_id": flowcell_id, 
                        "sequence_date": str(sequence_date.date()),
                        "upload_time": str(datetime.now())})
    utils.s3_sync(dirname, fastq_bucket, key, s3_endpoint_url)


def run_salmonella_pipeline(run_id, fastq_bucket, key, s3_endpoint_url):
    """
        Runs salmonella pipelines for a single plate in AWS batch
    """
    # Construct submission form for SCE-batch
    submission_form = \
        ({"Name": run_id,
            "JobQueue": "ec2-p1-0-1-1",
            "JobDefinition": "csu-wgsprocessing-0-1-1:2",
            "Quantity": 1,
            "CPU": 4,
            "RAM_MB": 8192,
            "Command":
                ["python",
                 "./plate/batch_process_plate.py",
                 "-s",
                 f"s3://{fastq_bucket}/{key}"
                 "-t",
                 f"s3://s3-csu-004/salmonella/{run_id}/"],
            "ENV": [],
            "PARAM": {}})
    # Submit job to SCE-batch
    utils.upload_json("s3-batch-gbgc-csu-wgsprocessing-0-1-1",
                        f"{run_id}.scebatch", s3_endpoint_url,
                        submission_form)


def monitor_disk_usage(filepath):
    total, used, free = shutil.disk_usage(filepath)
    return (total, free)
        

def log_disk_usage(filepath):
    """
        Logs the level of free space in gb for the fileystem the filepath is mounted on
    """
    total, free = monitor_disk_usage(filepath)
    free_gb = free / 1024**3

    logging.info(f"Free space: (%.1f Gb) %s"%(free_gb, filepath))


def remove_plate(plate_paths):
    """
        Deletes the directory tree at the paths in each element of 
        'plate_paths' (list)
    """
    try:
        for path in plate_paths:
            shutil.rmtree(path)
            logging.info(f"Removing old data: '{path}'")
    except PermissionError as e:
        logging.info(f"Cannot delete. {e}")


class BclEventHandler(FileSystemEventHandler):
    """
        Handles CopyComplete.txt created events 
    """

    def __init__(self, 
        watch_dir,
        backup_dir, 
        fastq_dir, 
        fastq_bucket, 
        fastq_key, 
        s3_endpoint_url,
        copy_complete_filename='CopyComplete.txt'
        ):
        super(BclEventHandler, self).__init__()

        # Creation of this file indicates that an Illumina Machine has finished transferring
        # a plate of raw bcl reads
        self.copy_complete_filename = copy_complete_filename

        # Directory to watch for new incoming bcl data
        self.watch_dir = watch_dir + os.path.join('')

        # Raw Bcl Data backed up here (one dir for each plate)
        self.backup_dir = backup_dir + os.path.join('')

        # Converted Fastq (one dir for each plate)
        self.fastq_dir = fastq_dir + os.path.join('')

        # Where fastq files should be stored on S3
        self.fastq_bucket = fastq_bucket
        self.fastq_key = fastq_key
        self.s3_endpoint_url = s3_endpoint_url

        # Make sure backup and fastq dirs exist
        if not os.path.isdir(self.backup_dir):
            raise Exception("Backup Directory does not exist: %s" % self.backup_dir)

        if not os.path.isdir(self.fastq_dir):
            raise Exception("Fastq Directory does not exist: %s" % self.fastq_dir)

        # Log disk usage
        log_disk_usage(self.watch_dir)
        log_disk_usage(self.fastq_dir)
        log_disk_usage(self.backup_dir)

    def process_bcl_plate(self, event):
        """
            Processes a bcl plate.
            Copies, converts to fastq and uploads to AWS
        """
        backup_path = self.backup_dir + event.src_name + '/'

        # Process
        logging.info(f'Backing up Raw Bcl Run: {backup_path}')
        copy(event.abs_src_path, backup_path)

        logging.info(f'Converting to fastq: {event.fastq_path}')
        convert_to_fastq(event.abs_src_path, event.fastq_path)
        
        self.process_projects(event)

        # remove all plates where the processed data is older than 30 days
        self.clean_up()
    
    def process_projects(self, event):
        # Extract metadata
        match = re.search(r'(.+)_((.+)_(.+))_(.+)',
                          basename(os.path.dirname(event.fastq_path)))

        if not match:
            raise Exception(f'Could not extract run number from \
                            {event.fastq_path}')

        sequence_date = datetime.strptime(match.group(1), r'%y%m%d')
        run_id = match.group(2)
        instrument_id = match.group(3)
        run_number = match.group(4)
        flowcell_id = match.group(5)
        
        logging.info(f'Uploading {event.fastq_path} to s3://{self.fastq_bucket}/{self.fastq_key}')
        # Upload each directory that contains fastq files
        for dirname in glob.glob(event.fastq_path + '*/'):
            # Skip if no fastq.gz in the directory
            if not glob.glob(dirname + '*.fastq.gz'):
                continue        
            # S3 target
            project_code = basename(os.path.dirname(dirname))
            key = f'{self.fastq_key}{project_code}/{run_id}'
            upload(key, project_code, instrument_id, run_number, run_id,
                   flowcell_id, sequence_date, dirname)
            if project_code in SALMONELLA_PROJECT_CODES:
                logging.info(f'Running Salmonella WGS pipeline on plate\
                              {run_id}')
                run_salmonella_pipeline(run_id, self.fastq_bucket, key,
                                        self.s3_endpoint_url)

    def clean_up(self):
        """
            Runs through all fully processed plates and deletes bcl data
            from watch-dir (IncomingRuns). Also deletes fastq data from 
            fastq_dir and backup bcl data from backup_dir (OutputFastq) 
            ONLY if any processed plate is older than 30 days. 
            NOTE: this will only delete data if that plate has been 
            fully processed.
        """
        today = datetime.today()
        for plate in os.listdir(self.fastq_dir):
            # ensure that plate is a folder
            try:
                # backup & fastq plates
                fastq_plate = os.path.join(self.fastq_dir, plate)
                fastq_contents = os.listdir(fastq_plate)
                # ensure the folder matches processed plate format 
                if "Logs" in fastq_contents and "Reports" in fastq_contents:
                    # bcl data
                    bcl_plate = os.path.join(self.watch_dir, plate)
                    if os.path.isdir(bcl_plate):
                        # delete processed bcl data
                        remove_plate([bcl_plate])
                    # datetime of fastq processing for each plate
                    modified_date = \
                            datetime.fromtimestamp(\
                                os.path.getmtime(fastq_plate))
                    # age of the processed plate
                    age = today - modified_date
                    # delete processed, raw and backup files if processed plate 
                    # is older than 21 days
                    if age.days > 21:
                        backup_plate = os.path.join(self.backup_dir, plate)
                        remove_plate([fastq_plate, backup_plate])
            except NotADirectoryError:
                pass

    def on_created(self, event):
        """Called when a file or directory is created.

        Returns true if a new bcl plate is found, False otherwise

        :param event:
            Event representing file/directory creation.
        :type event:
            :class:`DirCreatedEvent` or :class:`FileCreatedEvent`
        """

        # Check the filename
        if (ntpath.basename(event.src_path) != self.copy_complete_filename):
            return        

        # Extract run number of the plate 
        event.abs_src_path = \
            os.path.join(os.path.dirname(os.path.abspath(event.src_path)), "")
        event.src_name = basename(event.abs_src_path[:-1])
        # Output path for fastq data of the plate
        event.fastq_path = os.path.join(self.fastq_dir, event.src_name, "")

        # log if anything fails
        try:
            logging.info('Processing new plate: %s' % event.src_path)
            self.process_bcl_plate(event.src_path)
        except Exception as e:
            logging.exception(e)
            raise e

        # Log remaining disk space
        logging.info('New Illumina Plate Processed: %s' % event.src_path)
        log_disk_usage(self.watch_dir)
        log_disk_usage(self.fastq_dir)
        log_disk_usage(self.backup_dir)  

def is_subdirectory(filepath1, filepath2):
    """
        Checks if path1 is a subdirectory of path2
    """
    path1 = Path(os.path.abspath(filepath1))
    path2 = Path(os.path.abspath(filepath2))

    return path2 in path1.parents or path1 == path2

class SubdirectoryException(Exception):
    """ Use in start to signal errors that proctect against recursive file watching behaviour """
    pass

def start(watch_dir, backup_dir, fastq_dir, fastq_bucket, fastq_key, s3_endpoint_url):
    """
        Watches a directory for CopyComplete.txt files
    """
    #  Ensure backup/fastq dirs are not subdirectories of watch_dir.
    #    This causes catastrophic recursive behaviours
    if is_subdirectory(backup_dir, watch_dir):
        raise SubdirectoryException('Backup directory cannot be a subdirectory of the watch directory')

    if is_subdirectory(fastq_dir, watch_dir):
        raise SubdirectoryException('Fastq directory cannot be a subdirectory of the watch directory')

    # Setup file watcher in a new thread
    observer = Observer()
    handler = BclEventHandler(watch_dir, backup_dir, fastq_dir, fastq_bucket, fastq_key, s3_endpoint_url)
    observer.schedule(handler, watch_dir, recursive=True)

    # Start File Watcher
    observer.start()
    logging.info(f"""
        --------------------
        BCL Manager Started
        --------------------
        
        Bcl Watch Directory: {watch_dir}
        Backup Directory: {handler.backup_dir}
        Fastq Directory: {handler.fastq_dir}
    """)    

    # Sleep till exit
    observer.join()

if __name__ == "__main__":
    # Parse
    parser = argparse.ArgumentParser(description='Watch a directory for a creation of CopyComplete.txt files')
    parser.add_argument('dir', nargs='?', default='/Illumina/IncomingRuns/', help='Watch directory')
    parser.add_argument('--backup-dir', default='/Illumina/OutputFastq/BclRuns/', help='Where to backup data to')
    parser.add_argument('--fastq-dir', default='/Illumina/OutputFastq/FastqRuns/', help='Where to put converted fastq data')
    parser.add_argument('--s3-log-bucket', default='s3-csu-001', help='S3 Bucket to upload log file')
    parser.add_argument('--s3-log-key', default='logs/bcl-manager.log', help='S3 Key to upload log file')
    parser.add_argument('--s3-fastq-bucket', default='s3-csu-001', help='S3 Bucket to upload fastq files')
    parser.add_argument('--s3-fastq-key', default='', help='S3 Key to upload fastq data')
    parser.add_argument('--s3-endpoint-url', default='https://bucket.vpce-0a9b8c4b880602f6e-w4s7h1by.s3.eu-west-1.vpce.amazonaws.com', help='aws s3 endpoint url')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            S3LoggingHandler('./bcl-manager.log', args.s3_log_bucket, args.s3_log_key, args.s3_endpoint_url)
        ]
    )

    # Run
    start(
        args.dir, 
        args.backup_dir, 
        args.fastq_dir,
        args.s3_fastq_bucket,
        args.s3_fastq_key,
        args.s3_endpoint_url
    )
