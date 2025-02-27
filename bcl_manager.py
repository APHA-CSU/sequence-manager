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
        raise Exception('bcl-convert failed: %s' % (return_code))


def copy(src_dir, dest_dir):
    """
        Backup BclFiles to another directory
    """
    # Make sure we are not overwriting anything!
    if os.path.isdir(os.path.abspath(dest_dir)):
        raise Exception('Cannot backup Bcl, path exists: %s' % dest_dir)

    shutil.copytree(src_dir, dest_dir)


def monitor_disk_usage(filepath):
    total, used, free = shutil.disk_usage(filepath)
    return (total, free)


def log_disk_usage(filepath):
    """
        Logs the level of free space in gb for the filesystem the
        filepath is mounted on
    """
    total, free = monitor_disk_usage(filepath)
    free_gb = free / 1024**3

    logging.info("Free space: (%.1f Gb) %s" % (free_gb, filepath))


def clean_up(fastq_dir, watch_dir, backup_dir):
    """
        Runs through all fully processed plates and deletes bcl data
        from watch-dir (IncomingRuns). Also deletes fastq data from
        fastq_dir and backup bcl data from backup_dir (OutputFastq)
        ONLY if any processed plate is older than 30 days. NOTE: this
        will only delete data if that plate has been fully processed.
    """
    today = datetime.today()
    for plate in os.listdir(fastq_dir):
        # ensure that plate is a folder
        try:
            # backup & fastq plates
            fastq_plate = os.path.join(fastq_dir, plate)
            fastq_contents = os.listdir(fastq_plate)
            # ensure the folder matches processed plate format
            if "Logs" in fastq_contents and "Reports" in fastq_contents:
                # bcl data
                bcl_plate = os.path.join(watch_dir, plate)
                if os.path.isdir(bcl_plate):
                    # delete processed bcl data
                    remove_plate([bcl_plate])
                # datetime of fastq processing for each plate
                modified_date = \
                    datetime.fromtimestamp(os.path.getmtime(fastq_plate))
                # age of the processed plate
                age = today - modified_date
                # delete processed, raw and backup files if processed
                # plate is older than 21 days
                if age.days > 21:
                    backup_plate = os.path.join(backup_dir, plate)
                    remove_plate([fastq_plate, backup_plate])
        except NotADirectoryError:
            pass


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


def is_subdirectory(filepath1, filepath2):
    """
        Checks if path1 is a subdirectory of path2
    """
    path1 = Path(os.path.abspath(filepath1))
    path2 = Path(os.path.abspath(filepath2))

    return path2 in path1.parents or path1 == path2


def submit_batch_job(reads_bucket, reads_key, results_bucket, name,
                     submission_bucket, s3_endpoint_url):
    """
        Submits the Salmonella WGS pipeline to AWS batch running within
        'SCE-batch' infrastructure. Results are uploaded to
        s3://s3-ranch-050/{plate_name}_YYYYmmddHMS.

        Parameters:
            reads_bucket (str): the s3 bucket where salmonella reads are
                                stored
            reads_key (str): the s3 key for the plate of reads
            results_bucket (str): the s3 bucket for storing results
            name (str): the s3 key to store the results under
            submission_bucket (str): the s3 bucket for receiving aws
                                     batch job submissions
            s3_endpoint_url (str): the s3 endpoint url
    """
    reads_uri = f"s3://{os.path.join(reads_bucket, reads_key)}"
    results_uri = f"s3://{os.path.join(results_bucket, name)}"
    logging.info(f"Submitting to AWS batch: {reads_uri}")
    submission_dict = {"SystemName": "salmonella-ec2-v2",
                       "Name": name,
                       "Quantity": 1,
                       "CPU": 32,
                       "RAM_MB": 125952,
                       "ENV": [],
                       "Command": ["python",
                                   "./plate/batch_process_plate.py",
                                   reads_uri,
                                   results_uri],
                       "PARAM": {}}
    utils.upload_json(submission_bucket, f"{name}.scebatch", s3_endpoint_url,
                      submission_dict, profile="batch")


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
                 salm_submission_bucket,
                 salm_results_bucket,
                 copy_complete_filename='CopyComplete.txt'):
        super(BclEventHandler, self).__init__()

        # Creation of this file indicates that an Illumina Machine has
        # finished transferring a plate of raw bcl reads
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

        # For running Salmonella pipeline in AWS batch
        self.salm_submission_bucket = salm_submission_bucket
        self.salm_results_bucket = salm_results_bucket

        # Make sure backup and fastq dirs exist
        if not os.path.isdir(self.backup_dir):
            raise Exception("Backup Directory does not exist: %s"
                            % self.backup_dir)

        if not os.path.isdir(self.fastq_dir):
            raise Exception("Fastq Directory does not exist: %s"
                            % self.fastq_dir)

        # Log disk usage
        log_disk_usage(self.watch_dir)
        log_disk_usage(self.fastq_dir)
        log_disk_usage(self.backup_dir)

    def process_bcl_plate(self, event):
        """
            Processes a bcl plate.
            Copies, converts to fastq, uploads to SCE and runs the
            Salmonella pipeline in AWS batch
        """
        backup_path = os.path.join(self.backup_dir, event.src_name, "")

        # Process
        logging.info(f'Backing up Raw Bcl Run: {backup_path}')
        copy(event.abs_src_path, backup_path)

        logging.info(f'Converting to fastq: {event.fastq_path}')
        convert_to_fastq(event.abs_src_path, event.fastq_path)

        # upload to SCE and run Salmonella pipeline
        self.upload(event)

        # remove all plates where the processed data is older than 30
        # days
        clean_up(self.fastq_dir, self.watch_dir, self.backup_dir)

    def upload(self, event):
        """
            Upload every subdirectory under src_dir that contains
            fastq.gz files to S3.
            Files are stored with URI:
            s3://{bucket}/{prefix}/{project_code}/{run_id}/

            The src_path should reference a directory with format
            yymmdd_instrumentID_runnumber_flowcellID/

            The project_code is the name of the subdirectory that
            contains the fastq files.

            A meta.json file is also uploaded to each project_code with
            schema:
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
        # Extract metadata
        match = re.search(r'(.+)_((.+)_(.+))_(.+)',
                          basename(os.path.dirname(event.fastq_path)))
        if not match:
            raise Exception("Could not extract run number from "
                            f"{event.fastq_path}")
        sequence_date = datetime.strptime(match.group(1), r'%y%m%d')
        run_id = match.group(2)
        instrument_id = match.group(3)
        run_number = match.group(4)
        flowcell_id = match.group(5)
        logging.info(f"Uploading {event.fastq_path} to "
                     f"s3://{self.fastq_bucket}/{self.fastq_key}")
        # Upload each directory that contains fastq files
        for dirname in glob.glob(event.fastq_path + '*/'):
            # Skip if no fastq.gz in the directory
            if not glob.glob(dirname + '*.fastq.gz'):
                continue
            # S3 target
            project_code = basename(os.path.dirname(dirname))
            key = os.path.join(self.fastq_key, project_code, run_id)
            # Upload
            utils.upload_json(self.fastq_bucket,
                              f"{key}/meta.json",
                              self.s3_endpoint_url,
                              {"project_code": project_code,
                               "instrument_id": instrument_id,
                               "run_number": run_number,
                               "run_id": run_id,
                               "flowcell_id": flowcell_id,
                               "sequence_date": str(sequence_date.date()),
                               "upload_time": str(datetime.now())})
            utils.s3_sync(dirname, self.fastq_bucket, key, self.s3_endpoint_url)
            if project_code in SALMONELLA_PROJECT_CODES:
                # submit salmonella Nextflow pipeline to AWS batch
                submit_batch_job(self.fastq_bucket, key,
                                 self.salm_results_bucket,
                                 f"{run_id}_{datetime.today().strftime('%Y%m%d%H%M%S')}",
                                 self.salm_submission_bucket,
                                 self.s3_endpoint_url)

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
            self.process_bcl_plate(event)
        except Exception as e:
            logging.exception(e)
            raise e

        # Log remaining disk space
        logging.info('New Illumina Plate Processed: %s' % event.src_path)
        log_disk_usage(self.watch_dir)
        log_disk_usage(self.fastq_dir)
        log_disk_usage(self.backup_dir)


class SubdirectoryException(Exception):
    """
        Use in start to signal errors that protect against recursive
        file watching behavior
    """
    pass


def start(watch_dir,
          backup_dir,
          fastq_dir,
          fastq_bucket,
          fastq_key,
          s3_endpoint_url,
          salm_submission_bucket,
          salm_results_bucket):
    """
        Watches a directory for CopyComplete.txt files
    """
    #  Ensure backup/fastq dirs are not subdirectories of watch_dir.
    #    This causes catastrophic recursive behaviors
    if is_subdirectory(backup_dir, watch_dir):
        raise SubdirectoryException("Backup directory cannot be a subdirectory \
                                     of the watch directory")

    if is_subdirectory(fastq_dir, watch_dir):
        raise SubdirectoryException("Fastq directory cannot be a subdirectory \
                                     of the watch directory")

    # Setup file watcher in a new thread
    observer = Observer()
    handler = BclEventHandler(watch_dir, backup_dir, fastq_dir, fastq_bucket,
                              fastq_key, s3_endpoint_url,
                              salm_submission_bucket, salm_results_bucket)
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
    parser = argparse.ArgumentParser(description="Watch a directory for a \
                                     creation of CopyComplete.txt files")
    parser.add_argument('dir', nargs='?',
                        default='/Illumina/IncomingRuns/',
                        help='Watch directory')
    parser.add_argument('--backup-dir',
                        default='/Illumina/OutputFastq/BclRuns/',
                        help='Where to backup data to')
    parser.add_argument('--fastq-dir',
                        default='/Illumina/OutputFastq/FastqRuns/',
                        help='Where to put converted fastq data')
    parser.add_argument('--s3-log-bucket',
                        default='s3-csu-001',
                        help='S3 Bucket to upload log file')
    parser.add_argument('--s3-log-key',
                        default='logs/bcl-manager.log',
                        help='S3 Key to upload log file')
    parser.add_argument('--s3-fastq-bucket',
                        default='s3-csu-001',
                        help='S3 Bucket to upload fastq files')
    parser.add_argument('--s3-fastq-key',
                        default='',
                        help='S3 Key to upload fastq data')
    parser.add_argument('--s3-endpoint-url',
                        default='https://bucket.vpce-0a9b8c4b880602f6e-w4s7h1by.s3.eu-west-1.vpce.amazonaws.com',
                        help='aws s3 endpoint url for fastq file uploads')
    parser.add_argument('--salmonella-submission-bucket',
                        default='s3-batch-9ut9-salmonella-ec2-v2-2-4-5',
                        help='S3 bucket for AWS batch submission forms')
    parser.add_argument('--salmonella-results-bucket',
                        default='s3-ranch-050',
                        help='S3 bucket for Salmonella pipeline results')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(),
                  S3LoggingHandler('./bcl-manager.log',
                                   args.s3_log_bucket,
                                   args.s3_log_key,
                                   args.s3_endpoint_url)])

    # Run
    start(args.dir,
          args.backup_dir,
          args.fastq_dir,
          args.s3_fastq_bucket,
          args.s3_fastq_key,
          args.s3_endpoint_url,
          args.salmonella_submission_bucket,
          args.salmonella_results_bucket)
