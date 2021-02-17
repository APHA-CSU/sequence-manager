import sys
import time
import logging
import ntpath

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileCreatedEvent, FileSystemEventHandler


class IlluminaPlateTransferredEvent(FileSystemEventHandler):
    """File system event representing file creation on the file system."""

    def __init__(self, copy_complete_filename='CopyComplete.txt'):
        super(IlluminaPlateTransferredEvent, self).__init__()

        self.copy_complete_filename = copy_complete_filename

    def on_created(self, event):
        """Called when a file or directory is created.

        :param event:
            Event representing file/directory creation.
        :type event:
            :class:`DirCreatedEvent` or :class:`FileCreatedEvent`
        """

        print('event: ', event)

        # Check the file
        if (ntpath.basename(event.src_path) != self.copy_complete_filename):
            return

        print('New Illumina Plate Transferred')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    event_handler = LoggingEventHandler()
    event_handler = IlluminaPlateTransferredEvent()


    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()