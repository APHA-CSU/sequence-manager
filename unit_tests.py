import unittest
import unittest.mock
from unittest.mock import Mock, MagicMock
import random

import watchdog
import bcl_manager

class TestBclManager(unittest.TestCase):
    def test_handler_construction(self):
        # Succeeds when output directories exist
        bcl_manager.BclEventHandler('./', './', copy_complete_filename='CopyComplete.txt')

        # Raises exception when output directories do not exist 
        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./DOES_NOT_EXIST', './', copy_complete_filename='CopyComplete.txt')

        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./', './DOES_NOT_EXIST', copy_complete_filename='CopyComplete.txt')

    def test_on_create(self):
        """
            Assert the handler processes the event src_path correctly
        """
        # Mocking shutil.copytree ensures we don't actually copy anything to disk during testing
        bcl_manager.shutil.copytree = Mock()

        # Mocking logging allows to test exceptions are logged
        bcl_manager.logging = MagicMock()

        # Test handler
        handler = bcl_manager.BclEventHandler('./', './', copy_complete_filename='CopyComplete.txt')

        # Ignores non-CopyComplete events
        self.assertEventOutput(handler, False, './notCopyComplete.txt')
        self.assertEventOutput(handler, False, 'CopyComplete.txt/')
        
        # Processes CopyComplete events        
        self.assertEventOutput(handler, True, '/some/absolute/path/to/CopyComplete.txt')
        self.assertEventOutput(handler, True, './CopyComplete.txt')

        # Logs exceptions when bcl processing fails
        mock = Mock()
        mock.side_effect = Exception('Error processing Bcl plate')
        handler.process_bcl_plate = mock

        with self.assertRaises(Exception):
            event = watchdog.events.FileCreatedEvent('./CopyComplete.txt')
            handler.on_created(event)
            
        self.assertTrue(bcl_manager.logging.exception.called)

    def test_copy(self):
        bcl_manager.shutil.copytree = Mock() 

        with self.assertRaises(Exception):
            bcl_manager.copy('./', './')

        bcl_manager.copy('./', './DOES/NOT/EXIST/')

    def assertEventOutput(self, handler, expected_output, src_path):
        """ 
            Asserts the actual_output of a BclEventHandler matches the expected_output
        """
        # Create new event
        event = watchdog.events.FileCreatedEvent(src_path)

        # Test Output
        actual_output = handler.on_created(event)        
        self.assertEqual(actual_output, expected_output)

if __name__ == '__main__':
    unittest.main()
