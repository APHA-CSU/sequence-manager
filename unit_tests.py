import unittest
import unittest.mock
from unittest.mock import Mock, MagicMock
import argparse
import sys
import errno

import watchdog
import bcl_manager
import launch
from bcl_manager import SubdirectoryException

class TestBclManager(unittest.TestCase):
    def test_handler_construction(self):
        # Succeeds when output directories exist
        bcl_manager.BclEventHandler('./', './', '', '', '')

        # Raises exceptions when output directories do not exist 
        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./DOES_NOT_EXIST', './')

        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./', './DOES_NOT_EXIST')

    def test_on_create(self):
        """
            Assert the handler processes the event src_path correctly
        """
        # Mocking logging allows us to test that exceptions are logged
        bcl_manager.logging = MagicMock()

        # Mocking disk_usage to return 0 free space every time it's called 
        #    (it'd throw an exception on non-existent paths otherwise)
        bcl_manager.shutil.disk_usage = Mock()
        bcl_manager.shutil.disk_usage.return_value = (0,0,0)

        # Test handler
        handler = bcl_manager.BclEventHandler('./', './', '', '', '')

        # Mocking process_bcl_plate allows us to test on_create without actually doing any processing
        handler.process_bcl_plate = Mock()

        # Ignores non-CopyComplete events
        self.assertOnCreatedProcessing(handler, False, './notCopyComplete.txt')
        self.assertOnCreatedProcessing(handler, False, 'CopyComplete.txt/')
        
        # Processes CopyComplete events        
        self.assertOnCreatedProcessing(handler, True, '/some/absolute/path/to/CopyComplete.txt')
        self.assertOnCreatedProcessing(handler, True, './CopyComplete.txt')

        # Logs exceptions when bcl processing fails
        handler.process_bcl_plate.side_effect = Exception('Error processing Bcl plate')

        with self.assertRaises(Exception):
            event = watchdog.events.FileCreatedEvent('./CopyComplete.txt')
            handler.on_created(event)

        self.assertTrue(bcl_manager.logging.exception.called)

    def test_copy(self):
        """
            Asserts the copy method does not overwrite directories
        """
        # Mocking shutil.copytree prevents any actual data from being copied during testing
        bcl_manager.shutil.copytree = Mock() 

        with self.assertRaises(Exception):
            bcl_manager.copy('./', './')

        bcl_manager.copy('./', './DOES/NOT/EXIST/')

    def assertOnCreatedProcessing(self, handler, bcl_plate_processing_expected, src_path):
        """ 
            Asserts whether BclEventHandler.on_created() calls process_bcl_plate()
        """
        # Mocking process_bcl_plate allows us to test if it was called
        handler.process_bcl_plate = MagicMock()

        # Run a FileCreated event
        event = watchdog.events.FileCreatedEvent(src_path)
        handler.on_created(event)        

        # Assert if process_bcl_plate() was called
        self.assertTrue(bcl_plate_processing_expected == handler.process_bcl_plate.called)

    def test_start(self):
        """
            Test the start
        """
        # Mocking allows us to stop logging and s3 uploads during testing
        bcl_manager.logging = Mock()
        bcl_manager.Observer = Mock()
        bcl_manager.input = Mock()
        bcl_manager.BclEventHandler = Mock()

        # This case should pass
        bcl_manager.start('./watch_dir/', './fastq_dir/', './backup_dir/', '', '', '')

        # These cases would cause catastrophic recursion and should throw pre-emptive exceptions
        with self.assertRaises(SubdirectoryException):
            bcl_manager.start('./', './', './', '', '', '')

        with self.assertRaises(SubdirectoryException):
            bcl_manager.start('./', './another/subdirectory/', './', '', '', '')

        with self.assertRaises(SubdirectoryException):
            bcl_manager.start('./', '/absolute/path/', './another/subdirectory/that/doesnt/exist/', '', '', '')

        with self.assertRaises(SubdirectoryException):
            bcl_manager.start('./', '/absolute/path/', './subdirectory/doesnt/exist/', '', '', '')

    def test_convert_to_fastq(self):
        # Mock subprocess
        bcl_manager.subprocess.run = Mock()

        # Successful conversion
        bcl_manager.subprocess.run.return_value = Mock(returncode=0)
        bcl_manager.convert_to_fastq('./', './')

        # Unsuccessful conversion
        bcl_manager.subprocess.run.return_value = Mock(returncode=1)
        with self.assertRaises(Exception):
            bcl_manager.convert_to_fastq('./', './')

    def test_upload(self):
        # Test cases
        good_src_path = '220401_instrumentID_runnumber_flowcellID'
        bad_src_path = 'incorrectly-formatted'

        # Mocks
        s3_sync_mock = Mock()
        bcl_manager.utils.s3_sync = s3_sync_mock
        bcl_manager.subprocess.run = Mock()
        bcl_manager.utils.boto3 = Mock()

        bcl_manager.glob.glob = Mock(return_value=["directory_name"])

        # Successful upload
        bcl_manager.upload(good_src_path, '', '', '')

        # Raises error if src_path is incorrectly formatted
        with self.assertRaises(Exception):
            bcl_manager.upload(bad_src_path, '', '', '')

class TestLaunch(unittest.TestCase):
    def test_extract_submission_no(self):
        test_input = ["AFxx-nn-nnnnn-yy",
                      "ATxx-nn-nnnnn-yy",
                      "AFx-nn-nnnnn-yy",
                      "Ax-nn-nnnnn-yy",
                      "AF-nn-nnnnn-yy",
                      "AFxnn-nnnnn-yy",
                      "A-nn-nnnnn-yy",
                      "-nn-nnnnn-yy",
                      "nn-nnnnn-yy",
                      "12345678",
                      "ABCDEFGH",
                      ""]
        test_output = ["nn-nnnnn-yy",
                       "ATxx-nn-nnnnn-yy",
                       "nn-nnnnn-yy",
                       "Ax-nn-nnnnn-yy",
                       "nn-nnnnn-yy",
                       "AFxnn-nnnnn-yy",
                       "A-nn-nnnnn-yy",
                       "-nn-nnnnn-yy",
                       "nn-nnnnn-yy",
                       "12345678",
                       "ABCDEFGH",
                       ""]
        for input, output in zip(test_input, test_output):
            self.assertEqual(launch.extract_submission_no(input), output)

def test_suit(test_objs):
    suit = unittest.TestSuite(test_objs)
    return suit

if __name__ == '__main__':
    bcl_manager_test = [TestBclManager('test_handler_construction'),
                        TestBclManager('test_on_create'),
                        TestBclManager('test_copy'),
                        # TODO: need to work out how to add positional arguments
                        TestBclManager('assertOnCreatedProcessing'),
                        TestBclManager('test_start'),
                        TestBclManager('test_convert_to_fastq'),
                        TestBclManager('test_upload')]
    launch_test = [TestLaunch('test_extract_submission_no')]
    runner = unittest.TextTestRunner()
    parser = argparse.ArgumentParser(description='Test code')
    module_arg = parser.add_argument('--module', '-m', nargs=1, 
                                     help="module to test: 'bcl_manager' or 'launch'",
                                     default=None)
    args = parser.parse_args()
    try:
        if args.module:
            if args.module[0] == 'bcl_manager':
                runner.run(test_suit(bcl_manager_test)) 
            elif args.module[0] == 'launch':
                runner.run(test_suit(launch_test)) 
            else:
                raise argparse.ArgumentError(module_arg, "Invalid argument. Please use 'forward', 'reverse' or 'interface'")
        else:
            unittest.main()
    except argparse.ArgumentError as e:
        print(e)
        sys.exit(errno.ENOENT)
