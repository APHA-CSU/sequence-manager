import unittest

import watchdog
import bcl_manager

class TestBclManager(unittest.TestCase):

    def test_bcl_event_handler(self):
        """
            Assert the handler processes the event src_path correctly
        """
        handler = bcl_manager.BclEventHandler('./', './', copy_complete_filename='CopyComplete.txt')

        self.assertEventOutput(handler, False, './notCopyComplete.txt')
        self.assertEventOutput(handler, False, 'CopyComplete.txt/')
        self.assertEventOutput(handler, True, 'CopyComplete.txt')
        self.assertEventOutput(handler, True, '/some/absolute/path/to/CopyComplete.txt')

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