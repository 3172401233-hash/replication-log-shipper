"""End-to-end flow backed by real files (WAL store + slot store)."""

import os
import tempfile
import unittest

from replication_log_shipper import (
    PrimaryNode,
    ReplicaReceiver,
    SlotStore,
    WalStore,
    WalWriter,
)

from tests.helpers import append_transaction


class PersistentFlowTests(unittest.TestCase):
    def test_primary_restart_recovers_slot_and_wal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wal_dir = os.path.join(tmp, "wal")
            slot_path = os.path.join(tmp, "slots.json")

            primary = PrimaryNode(
                "p",
                wal_store=WalStore(wal_dir),
                slot_store=SlotStore(slot_path),
            )
            writer = WalWriter()
            primary.add_replica("s1", "slot-s1")
            _begin, end = append_transaction(
                primary, writer, xid=1, payloads=[b"durable-row"]
            )

            receiver = ReplicaReceiver(
                "s1", WalStore(os.path.join(tmp, "r1-wal"))
            )
            for frame in primary.frames_for("s1"):
                receiver.handle_frame(frame)
            primary.receive_ack(receiver.build_ack())
            self.assertEqual(primary.safe_lsn, end)

            # Simulate primary restart: reopen stores from disk.
            restarted = PrimaryNode(
                "p",
                wal_store=WalStore(wal_dir),
                slot_store=SlotStore(slot_path),
                flush_lsn=end,
            )
            slot = restarted.slot_store.get("slot-s1")
            self.assertIsNotNone(slot)
            assert slot is not None
            self.assertEqual(slot.restart_lsn, end)
            records = restarted.wal.load_segment(1, 0)
            self.assertTrue(records)
            self.assertEqual(records[-1].end_lsn, end)

            # New transaction after restart appends contiguously.
            new_writer = WalWriter(start_lsn=end)
            restarted.add_replica("s1", "slot-s1")
            _b2, end2 = append_transaction(restarted, new_writer, xid=2)
            self.assertEqual(restarted.flush_lsn, end2)


if __name__ == "__main__":
    unittest.main()
