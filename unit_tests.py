import unittest
from unittest.mock import Mock, MagicMock, patch, call
import time
import os
import tempfile
import pathlib

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

    def test_clean_up(self):#, mock_remove_plate):
        """
            Test removing old plates
        """

        def mock_getmtime_returns(dirname):
            """
                #Inner function for return value of mocked 
                #os.path.getmtime(). The return value (age of the dir) is
                #dependent on the argument name of the directory. Return
                #values simulate directories of ages 32, 31 and 29 days.
            """
            # assert that bcl_manager.os.path.getmtime() is called on 
            # existing path
            self.assertTrue(os.path.exists(dirname))
            now = time.time()
            plate_name = os.path.basename(dirname)
            if plate_name == "plate_1":
                return now - 2851200
            elif plate_name == "plate_2":
                return now - 2764801
            elif plate_name == "plate_3":
                return now - 2505601
            else:
                raise Exception("not a valid mock plate name")

        # mock remove_plate but retain functionality
        bcl_manager.remove_plate = Mock(wraps=bcl_manager.remove_plate)
        bcl_manager.os.path.getmtime = Mock()

        # Test removing 2 fully processed plates and leaving the 3rd as it is <
        # 30 days old

        with patch("bcl_manager.os.path.getmtime") as mock_getmtime:
            # side effects: current time less 32 days; 31 days; 29 days (in 
            # sconds), i.e. simulating plates of 32, 31 and 29 days old
            mock_getmtime.side_effect = lambda x: mock_getmtime_returns(x)
            # use a temporary directory as a 'sandbox'
            with tempfile.TemporaryDirectory() as temp_directory:
                # 'mock-up' plates - raw bcl data
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_1"))
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_2"))
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_3"))
                # 'mock-up' plates - backup bcl data
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_1"))
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_2"))
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_3"))
                # 'mock-up' plates - processed data
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Logs"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_2/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_2/Logs"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_3/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_3/Logs"))
                # Test handler
                handler = bcl_manager.BclEventHandler(os.path.join(temp_directory, "watch_dir"),
                                                        os.path.join(temp_directory, "backup_dir"),
                                                        os.path.join(temp_directory, "fastq_dir"),
                                                        '', 
                                                        '', 
                                                        '')
                # call clean_up
                handler.clean_up()
        correct_calls = [call([os.path.join(temp_directory, "fastq_dir/plate_1"), os.path.join(temp_directory, "backup_dir/plate_1")]),
                         call([os.path.join(temp_directory, "fastq_dir/plate_2"), os.path.join(temp_directory, "backup_dir/plate_2")])]
        # assert correct calls regardless of order
        self.assertCountEqual(correct_calls, bcl_manager.remove_plate.mock_calls)
        # reset call attributes of bcl_manager.remove_plate mock
        bcl_manager.remove_plate.reset_mock()
        # reset call attributes of bcl_manager.os.path.getmtime mock
        bcl_manager.os.path.getmtime.reset_mock()

        # Test removing no plates

        with patch("bcl_manager.os.path.getmtime") as mock_getmtime:
            now = time.time()
            # side effects: current time less 29 days in seconds, i.e. 29 days 
            # old
            mock_getmtime.return_value = now - 2505601
            # use a temporary directory as a 'sandbox'
            with tempfile.TemporaryDirectory() as temp_directory:
                # 'mock-up' plates - raw bcl data
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_1"))
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_2"))
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_3"))
                # 'mock-up' plates - backup bcl data
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_1"))
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_2"))
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_3"))
                # 'mock-up' plates - processed data
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Logs"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_2/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_2/Logs"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_3/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_3/Logs"))
                # Test handler
                handler = bcl_manager.BclEventHandler(os.path.join(temp_directory, "watch_dir"), 
                                                      os.path.join(temp_directory, "backup_dir"), 
                                                      os.path.join(temp_directory, "fastq_dir"),
                                                      '', 
                                                      '', 
                                                      '')
                # call clean_up
                handler.clean_up()
        # assert bcl_manager.remove_plate 
        assert not bcl_manager.remove_plate.called
        # reset call attributes of bcl_manager.remove_plate mock
        bcl_manager.remove_plate.reset_mock()
        # reset call attributes of bcl_manager.os.path.getmtime mock
        bcl_manager.os.path.getmtime.reset_mock()

        # Test skipping files (files in the plate directories, i.e. that don't
        # fit the plate format)

        with patch("bcl_manager.os.path.getmtime") as mock_getmtime:
            now = time.time()
            # side effects: current time less 32 days in seconds, i.e. 32 days
            # old 
            mock_getmtime.return_value = now - 2851200
            # use a temporary directory as a 'sandbox'
            with tempfile.TemporaryDirectory() as temp_directory:
                os.makedirs(os.path.join(temp_directory, "watch_dir/plate_1"))
                os.makedirs(os.path.join(temp_directory, "backup_dir/plate_1"))
                # 'mock-up' plate - processed data
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Reports"))
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_1/Logs"))
                # mock directory without correct plate format
                os.makedirs(os.path.join(temp_directory, "fastq_dir/plate_2"))
                # mock file instead of directory
                pathlib.Path(os.path.join(temp_directory, "fastq_dir/plate_3")).touch()
                # Test handler
                handler = bcl_manager.BclEventHandler(os.path.join(temp_directory, "watch_dir"), 
                                                      os.path.join(temp_directory, "backup_dir"), 
                                                      os.path.join(temp_directory, "fastq_dir"),
                                                      '', 
                                                      '', 
                                                      '')
                # call clean_up
                handler.clean_up()
        # assert bcl_manager.shutil.rmtree() is called only once with 
        # 'fastq_dir/plate_1' filepath, i.e. skips filepaths that do not match
        # plate format 
        bcl_manager.remove_plate.assert_called_once_with([os.path.join(temp_directory, "fastq_dir/plate_1"),
                                                          os.path.join(temp_directory, "backup_dir/plate_1")])
        # assert NotADirectoryError is raised (plate_3)
        self.assertRaises(NotADirectoryError)

if __name__ == '__main__':
    unittest.main()
