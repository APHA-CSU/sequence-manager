import unittest
import unittest.mock
from unittest.mock import Mock, MagicMock, patch
import os
import tempfile
import shutil

from pyfakefs import fake_filesystem_unittest

import watchdog
import bcl_manager
from bcl_manager import SubdirectoryException

class TestBclManager(fake_filesystem_unittest.TestCase):
    def setUp(self):
        """
            Set up method
        """
        # use "fake" in-memory filesystem
        self.setUpPyfakefs()

    def tearDown(self):
        """
            Tear down method
        """
        pass

    def test_handler_construction(self):
        # Succeeds when output directories exist
        bcl_manager.BclEventHandler('./', './', './', '', '', '')

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
        handler = bcl_manager.BclEventHandler('./', './', './', '', '', '')

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

    @patch("bcl_manager.monitor_disk_usage")
    def test_remove_old_plates(self, mock_monitor_disk_usage):
        """
            Test removing old plates
        """
        # mock bcl_manager.shutil.rmtree - but retain functionality
        bcl_manager.shutil.rmtree = Mock(wraps=shutil.rmtree)
        # mock bcl_manager.monitor_disk_usage with side effects
        mock_monitor_disk_usage.side_effect = [(100, 0), (100, 20), (100, 40), (100, 60), (100, 80), (100, 100)]
        # use a temporary directory as a mock filesystem
        with tempfile.TemporaryDirectory() as temp_filesystem:
            # make directories sequentially - mock plates
            os.mkdir(os.path.join(temp_filesystem, "plate_1"))
            os.mkdir(os.path.join(temp_filesystem, "plate_2"))
            os.mkdir(os.path.join(temp_filesystem, "plate_3"))
            os.mkdir(os.path.join(temp_filesystem, "plate_4"))
            os.mkdir(os.path.join(temp_filesystem, "plate_5"))
            os.mkdir(os.path.join(temp_filesystem, "plate_6"))
            # remove old plates
            bcl_manager.remove_old_plates(temp_filesystem)
        # expected calls to bcl_manager.shutil.rmtree
        rmtree_calls = [unittest.mock.call(os.path.join(temp_filesystem, "plate_1")),
                        unittest.mock.call(os.path.join(temp_filesystem, "plate_2")),
                        unittest.mock.call(os.path.join(temp_filesystem, "plate_3"))]
        # assert bcl_manager.shutil.rmtreee has expected calls
        bcl_manager.shutil.rmtree.assert_has_calls(rmtree_calls)

if __name__ == '__main__':
    unittest.main()
