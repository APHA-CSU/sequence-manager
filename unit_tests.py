import unittest
import unittest.mock

import watchdog
import bcl_manager

class TestBclManager(unittest.TestCase):

    def test_bcl_event_handler(self):
        copy_complete_filename = 'CopyComplete.txt'

        handler = bcl_manager.BclEventHandler(copy_complete_filename=copy_complete_filename)

        # Test CopyComplete.txt events
        event = watchdog.events.FileCreatedEvent(copy_complete_filename)
        result = handler.on_created(event)
        self.assertEqual(result, True)

        # Test not CopyComplete.txt events
        event = watchdog.events.FileCreatedEvent('./notCopyComplete.txt')
        result = handler.on_created(event)
        self.assertEqual(result, False)

        print()


if __name__ == '__main__':
    unittest.main()