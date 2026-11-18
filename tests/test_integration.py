"""End-to-end scenarios spanning segments, lag, slots and failover."""

import unittest

from replication_log_shipper import (
    SEGMENT_SIZE,
    Ack,
    MemoryWalStore,
    PrimaryNode,
    ReplicaReceiver,
    WalWriter,
)

from tests.helpers import append_transaction


def _ship(primary: PrimaryNode, replica_id: str, receiver: ReplicaReceiver) -> None:
    for frame in primary.frames_for(replica_id):
        result = receiver.handle_frame(frame)
        if result.outcome == "gap":
            raise AssertionError(result.detail)


class CrossSegmentTransactionTests(unittest.TestCase):
    def test_commit_across_segment_boundary(self) -> None:
        primary = PrimaryNode("p", wal_store=MemoryWalStore())
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")

        # Fill WAL up to three bytes before the segment boundary.
        _b0, end0 = append_transaction(
            primary, writer, xid=0, payloads=[b"a" * (SEGMENT_SIZE - 5)]
        )
        self.assertEqual(end0, SEGMENT_SIZE - 3)

        # Second transaction is deliberately large enough to straddle the
        # 16 MiB segment boundary.
        big_payload = b"x" * (SEGMENT_SIZE // 2)
        begin2, end2 = append_transaction(
            primary, writer, xid=2, payloads=[big_payload, big_payload]
        )
        self.assertLess(begin2, SEGMENT_SIZE)
        self.assertGreater(end2, SEGMENT_SIZE)

        receiver = ReplicaReceiver("s1", MemoryWalStore())
        _ship(primary, "s1", receiver)

        # ACK an offset inside segment 2 but still before COMMIT: no advance.
        partial_ack = Ack(
            timeline_id=1,
            epoch=1,
            replica_id="s1",
            durable_lsn=end2 - 1,
            seqno=1,
        )
        primary.receive_ack(partial_ack)
        self.assertEqual(primary.safe_lsn, end0)

        full_ack = Ack(
            timeline_id=1,
            epoch=1,
            replica_id="s1",
            durable_lsn=receiver.durable_lsn,
            seqno=2,
        )
        primary.receive_ack(full_ack)
        self.assertEqual(primary.safe_lsn, end2)
        self.assertGreaterEqual(primary.replica("s1").slot.restart_lsn, end2)

    def test_wal_recycling_respects_slot_and_safe_lsn(self) -> None:
        primary = PrimaryNode("p", wal_store=MemoryWalStore())
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        primary.add_replica("s2", "slot-s2")
        for xid in range(1, 4):
            append_transaction(primary, writer, xid=xid)

        r1 = ReplicaReceiver("s1", MemoryWalStore())
        r2 = ReplicaReceiver("s2", MemoryWalStore())
        _ship(primary, "s1", r1)
        _ship(primary, "s2", r2)
        primary.receive_ack(r1.build_ack())
        primary.receive_ack(r2.build_ack())
        self.assertGreater(primary.safe_lsn, 0)

        # Nothing recycled while slots still need segment 0.
        removed = primary.recycle_wal()
        self.assertEqual(removed, [])


class FullFailoverCycleTests(unittest.TestCase):
    def test_quorum_lost_then_recovered_after_switchover(self) -> None:
        primary = PrimaryNode(
            "p", required_syncs=2, wal_store=MemoryWalStore()
        )
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        primary.add_replica("s2", "slot-s2")
        _b, end = append_transaction(primary, writer, xid=1)

        r1 = ReplicaReceiver("s1", MemoryWalStore())
        r2 = ReplicaReceiver("s2", MemoryWalStore())
        _ship(primary, "s1", r1)
        _ship(primary, "s2", r2)
        primary.receive_ack(r1.build_ack())
        primary.receive_ack(r2.build_ack())
        self.assertEqual(primary.safe_lsn, end)

        # s2 falls behind; a second transaction cannot reach safe LSN.
        _b2, end2 = append_transaction(primary, writer, xid=2)
        _ship(primary, "s1", r1)
        primary.receive_ack(r1.build_ack())
        self.assertEqual(primary.safe_lsn, end)

        # Old primary dies; s1 (fully caught up) promotes.
        new_primary = r1.promote("new-primary", new_timeline_id=2, new_epoch=2)
        self.assertEqual(new_primary.safe_lsn, end2)

        # Delayed stale ACK from the demoted topology is isolated.
        new_primary.add_replica("s2", "slot-s2")
        stale = Ack(
            timeline_id=1,
            epoch=1,
            replica_id="s2",
            durable_lsn=end,
            seqno=1,
        )
        self.assertEqual(
            new_primary.receive_ack(stale).reason, "stale_term"
        )
        self.assertEqual(new_primary.safe_lsn, end2)


if __name__ == "__main__":
    unittest.main()
