import sys
import time
import logging
import ntpath
import argparse
import os
import shutil
from pathlib import Path
import subprocess

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileCreatedEvent, FileSystemEventHandler

from s3_logging_handler import S3LoggingHandler

def convert_to_fastq(src_dir, dest_dir):
    return_code = subprocess.run([
        "bcl-convert"
        "--output-directory", dest_dir,
        "--bcl-input-directory", src_dir,
        "--sample-sheet", f"{src_dir}/SampleSheet.csv"
        "--bcl-sampleproject-subdirectories", "true",
        "--no-lane-splitting", "true"
    ]).returncode

    if return_code:
        raise Exception('bcl Convert failed: %s'%(return_code))   

def copy(src_dir, dest_dir):
    """
        Backup BclFiles to another directory
    """
    # Make sure we are not overwriting anything!
    if os.path.isdir(os.path.abspath(dest_dir)):
        raise Exception('Cannot backup Bcl, path exists: %s'%dest_dir)    
   
    shutil.copytree(src_dir, dest_dir)

def upload():
    """ TODO """
    pass

def log_disk_usage(filepath):
    """
        Logs the level of free space in gb for the fileystem the filepath is mounted on
    """
    total, used, free = shutil.disk_usage(filepath)
    free_gb = free / 1024**3

    logging.info(f"Free space: (%.1f Gb) %s"%(free_gb, filepath))

class BclEventHandler(FileSystemEventHandler):
    """
        Handles CopyComplete.txt created events 
    """

    def __init__(self, backup_dir, fastq_dir, copy_complete_filename='CopyComplete.txt'):
        super(BclEventHandler, self).__init__()

        # Creation of this file indicates that an Illumina Machine has finished transferring
        # a plate of raw bcl reads
        self.copy_complete_filename = copy_complete_filename

        # Raw Bcl Data backed up here (one dir for each plate)
        self.backup_dir = backup_dir + os.path.join('')

        # Converted Fastq (one dir for each plate)
        self.fastq_dir = fastq_dir + os.path.join('')

        # Make sure backup and fastq dirs exist
        if not os.path.isdir(self.backup_dir):
            raise Exception("Backup Directory does not exist: %s" % self.backup_dir)

        if not os.path.isdir(self.fastq_dir):
            raise Exception("Fastq Directory does not exist: %s" % self.fastq_dir)

        # Log disk usage
        log_disk_usage(self.fastq_dir)
        log_disk_usage(self.backup_dir)

    def process_bcl_plate(self, src_path):
        """
            Processes a bcl plate.
            Copies, converts to fastq and uploads to AWS
        """
        # Get the name of the plate
        bcl_directory = os.path.dirname(os.path.abspath(src_path))
        plate_id = os.path.basename(bcl_directory)
        
        # Process
        copy(bcl_directory, self.backup_dir + plate_id)
        convert_to_fastq(bcl_directory, self.fastq_dir + plate_id)
        upload()

        # TODO: Remove old plates     

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

        # log if anything fails
        try:
            logging.info('Processing new plate: %s' % event.src_path)
            self.process_bcl_plate(event.src_path)

        except Exception as e:
            logging.exception(e)
            raise e

        # Log remaining disk space
        logging.info('New Illumina Plate Processed: %s' % event.src_path)
        log_disk_usage(event.src_path)
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

def start(watch_dir, backup_dir, fastq_dir):
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
    handler = BclEventHandler(backup_dir, fastq_dir)
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
    input('Press return to quit\n')
    observer.stop()
    observer.join()

if __name__ == "__main__":
    # Parse
    parser = argparse.ArgumentParser(description='Watch a directory for a creation of CopyComplete.txt files')
    parser.add_argument('dir', nargs='?', default='./watch/', help='Watch directory')
    parser.add_argument('--backup-dir', default='./backup/', help='Where to backup data to')
    parser.add_argument('--fastq-dir', default='./fastq/', help='Where to put converted fastq data')
    parser.add_argument('--s3-log-bucket', default='s3-csu-003', help='S3 Bucket to upload log file')
    parser.add_argument('--s3-log-key', default='aaron/logs/bcl-manager.log', help='S3 Key to upload log file')

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),
            S3LoggingHandler('./bcl-manager.log', args.s3_log_bucket, args.s3_log_key)
        ]
    )

    # Run
    start(args.dir, args.backup_dir, args.fastq_dir)