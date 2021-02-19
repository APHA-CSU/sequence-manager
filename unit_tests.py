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

        # Raises exceptions when output directories do not exist 
        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./DOES_NOT_EXIST', './', copy_complete_filename='CopyComplete.txt')

        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./', './DOES_NOT_EXIST', copy_complete_filename='CopyComplete.txt')

    def test_on_create(self):
        """
            Assert the handler processes the event src_path correctly
        """
        # Mocking logging allows to test exceptions are logged
        bcl_manager.logging = MagicMock()

        # Test handler
        handler = bcl_manager.BclEventHandler('./', './', copy_complete_filename='CopyComplete.txt')

        # Mocking process_bcl_plate allows us to test on_create without actually doing any processing
        handler.process_bcl_plate = Mock()

        # Ignores non-CopyComplete events
        self.assertEventOutput(handler, False, './notCopyComplete.txt')
        self.assertEventOutput(handler, False, 'CopyComplete.txt/')
        
        # Processes CopyComplete events        
        self.assertEventOutput(handler, True, '/some/absolute/path/to/CopyComplete.txt')
        self.assertEventOutput(handler, True, './CopyComplete.txt')

        # Logs exceptions when bcl processing fails
        handler.process_bcl_plate.side_effect = Exception('Error processing Bcl plate')

        with self.assertRaises(Exception):
            event = watchdog.events.FileCreatedEvent('./CopyComplete.txt')
            handler.on_created(event)

        self.assertTrue(bcl_manager.logging.exception.called)

    def test_copy(self):
        """
            Asserts the copy method does not overwrite
        """
        # Mocking shutil.copytree prevents any actual data from being copy
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

        # Ensure we log successes
        if actual_output:
            self.assertTrue(bcl_manager.logging.info.called)

if __name__ == '__main__':
    unittest.main()
