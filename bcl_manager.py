import sys
import time
import logging
import ntpath
import argparse
import os
import shutil

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileCreatedEvent, FileSystemEventHandler

def convert_to_fastq(src_dir, dest_dir):
    """ TODO """
    pass

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
            self.process_bcl_plate(event.src_path)

        except Exception as e:
            logging.exception(e)
            raise e

        logging.info('New Illumina Plate Processed: %s' % event.src_path)

def main(watch_dir, backup_dir, fastq_dir):
    """
        Watches a directory for CopyComplete.txt files
    """
    # Setup logging
    # TODO: handler that logs straight to S3
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('bcl-manager.log'),
            logging.StreamHandler()
        ]
    )

    # Setup file watcher in a new thread
    observer = Observer()
    handler = BclEventHandler(backup_dir, fastq_dir)
    observer.schedule(handler, watch_dir, recursive=True)

    # Start
    logging.info('Starting BCL File Watcher: %s' % watch_dir)
    observer.start()

    # Sleep till exit
    input('Press return to quit')
    observer.stop()
    observer.join()

if __name__ == "__main__":
    # Parse
    parser = argparse.ArgumentParser(description='Watch a directory for a creation of CopyComplete.txt files')
    parser.add_argument('dir', nargs='?', default='./', help='Watch directory')
    parser.add_argument('--backup-dir', default='./data/', help='Where to backup data to')
    parser.add_argument('--fastq-dir', default='./fastq-data/', help='Where to put converted fastq data')

    args = parser.parse_args()

    # Run
    main(args.dir, args.backup_dir, args.fastq_dir)