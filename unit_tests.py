import unittest
import unittest.mock
from unittest.mock import Mock

import watchdog
import bcl_manager

class TestBclManager(unittest.TestCase):
    def test_handler_construction(self):
        # Output directories exist
        bcl_manager.BclEventHandler('./', './', copy_complete_filename='CopyComplete.txt')

        # Output directories do not exist 
        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./DOES_NOT_EXIST', './', copy_complete_filename='CopyComplete.txt')

        with self.assertRaises(Exception):
            bcl_manager.BclEventHandler('./', './DOES_NOT_EXIST', copy_complete_filename='CopyComplete.txt')

    def test_bcl_event_handler(self):
        """
            Assert the handler processes the event src_path correctly
        """
        bcl_manager.shutil.copy = Mock()

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


#     ... @patch('module.ClassName1')
# ... def test(MockClass1, MockClass2):
# ...     module.ClassName1()
# ...     module.ClassName2()
# ...     assert MockClass1 is module.ClassName1
# ...     assert MockClass2 is module.ClassName2
# ...     assert MockClass1.called
# ...     assert MockClass2.called