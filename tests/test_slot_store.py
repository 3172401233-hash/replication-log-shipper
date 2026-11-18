import os
import tempfile
import unittest

from replication_log_shipper import (
    SEGMENT_SIZE,
    Frame,
    MemoryWalStore,
    RECORD_DATA,
    SlotStore,
    WalRecord,
    WalStore,
)
from replication_log_shipper.slot import ReplicationSlot


def frame_at(lsn: int, timeline_id: int = 1, epoch: int = 1) -> Frame:
    return Frame(
        timeline_id=timeline_id,
        epoch=epoch,
        seqno=lsn,
        record=WalRecord(lsn, lsn + 5, RECORD_DATA, 1, b"data!"),
    )


class ReplicationSlotTests(unittest.TestCase):
    def test_restart_lsn_monotonic(self) -> None:
        slot = ReplicationSlot("s1", "r1")
        self.assertTrue(slot.advance(100))
        self.assertEqual(slot.restart_lsn, 100)
        self.assertFalse(slot.advance(50))
        self.assertEqual(slot.restart_lsn, 100)
        with self.assertRaises(ValueError):
            slot.advance(-1)


class SlotStoreTests(unittest.TestCase):
    def test_persist_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "slots.json")
            store = SlotStore(path)
            store.get_or_create("slot-a", "replica-a").advance(2048)
            store.persist()

            reopened = SlotStore(path)
            slot = reopened.get("slot-a")
            self.assertIsNotNone(slot)
            assert slot is not None
            self.assertEqual(slot.replica_id, "replica-a")
            self.assertEqual(slot.restart_lsn, 2048)
            self.assertEqual(reopened.min_restart_lsn(), 2048)


class WalStoreTests(unittest.TestCase):
    def test_segment_persistence_and_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = WalStore(tmp)
            store.append_frame(frame_at(0))
            store.append_frame(frame_at(5))
            store.append_frame(frame_at(SEGMENT_SIZE + 1))
            self.assertEqual(store.list_segments(1), [0, 1])
            records = store.load_segment(1, 0)
            self.assertEqual([r.lsn for r in records], [0, 5])

            reopened = WalStore(tmp)
            self.assertEqual(
                [r.lsn for r in reopened.load_segment(1, 1)],
                [SEGMENT_SIZE + 1],
            )

    def test_recycle_keeps_retained_segments(self) -> None:
        store = MemoryWalStore()
        store.append_frame(frame_at(0))
        store.append_frame(frame_at(SEGMENT_SIZE))
        store.append_frame(frame_at(2 * SEGMENT_SIZE))
        removed = store.recycle_before(2 * SEGMENT_SIZE)
        self.assertEqual(len(removed), 2)
        self.assertEqual(store.list_segments(1), [2])


if __name__ == "__main__":
    unittest.main()
