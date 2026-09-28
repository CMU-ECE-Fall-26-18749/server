import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from server import OrderedCounter
from gfd import Sequence


class OrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_out_of_order_arrival_waits_for_missing_request(self):
        sequencer = Sequence()
        first = sequencer.allocate("C1", 1, "one")
        second = sequencer.allocate("C2", 1, "two")
        replica = OrderedCounter()
        later = asyncio.create_task(replica.apply(second))
        await asyncio.sleep(0)
        self.assertFalse(later.done())
        self.assertEqual(await replica.apply(first), (1, False))
        self.assertEqual(await asyncio.wait_for(later, 1), (2, False))

    async def test_retry_after_lost_reply_does_not_increment_state(self):
        sequencer = Sequence()
        request = sequencer.allocate("C1", 1, "one")
        replica = OrderedCounter()
        self.assertEqual(await replica.apply(request), (1, False))
        self.assertEqual(await replica.apply(request), (1, True))
        self.assertEqual(replica.value, 1)
        self.assertEqual(sequencer.allocate("C1", 1, "one"), request)
        self.assertEqual(len(sequencer.requests), 1)

    async def test_conflicting_payload_is_rejected(self):
        sequencer = Sequence()
        request = sequencer.allocate("C1", 1, "one")
        replica = OrderedCounter()
        await replica.apply(request)
        with self.assertRaises(ValueError):
            sequencer.allocate("C1", 1, "changed")
        with self.assertRaises(ValueError):
            await replica.apply({**request, "payload": "changed"})

    async def test_new_gfd_epoch_is_rejected(self):
        replica = OrderedCounter()
        await replica.apply(Sequence().allocate("C1", 1, "one"))
        with self.assertRaises(ValueError):
            await replica.apply(Sequence().allocate("C1", 1, "one"))


if __name__ == "__main__":
    unittest.main()
