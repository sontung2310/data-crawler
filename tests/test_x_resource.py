from __future__ import annotations

import threading
import time
import unittest

from x_resource import x_session_slot


class TestXResource(unittest.TestCase):
    def test_x_session_slot_never_overlaps(self):
        active = 0
        max_active = 0
        lock = threading.Lock()

        def worker():
            nonlocal active, max_active
            with x_session_slot():
                with lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.01)
                with lock:
                    active -= 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(max_active, 1)


if __name__ == "__main__":
    unittest.main()
