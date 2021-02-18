import sys
import time
import logging
import ntpath
import argparse

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileCreatedEvent, FileSystemEventHandler


def convert_to_fastq(from_path, to_path):
    """ TODO """
    pass

def copy(from_path, to_path):
    """ TODO """
    pass

def upload():
    """ TODO """
    pass


class BclEventHandler(FileSystemEventHandler):
    """
        Handles CopyComplete.txt created events 
    """

    def __init__(self, copy_complete_filename='CopyComplete.txt'):
        super(BclEventHandler, self).__init__()

        # Creation of this file indicates that an Illumina Machine has finished transferring
        # a plate of raw bcl reads
        self.copy_complete_filename = copy_complete_filename

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
            return False

        logging.info('New Illumina Plate Transferred')

        from_path = ""
        to_path = ""

        copy(from_path, to_path)
        convert_to_fastq(from_path, to_path)
        upload()

        return True


def main(path):
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
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    # Start the watcher in a new thread
    observer = Observer()
    observer.schedule(BclEventHandler(), path, recursive=True)

    logging.info('Starting BCL File Watcher: %s' % path)
    observer.start()

    # Sleep till exit
    # TODO: I prefer 'press return to quit'
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Watch a directory for a creation of CopyComplete.txt files')
    parser.add_argument('dir', nargs='?', default='./', help='Watch directory')

    args = parser.parse_args()

    main(args.dir)