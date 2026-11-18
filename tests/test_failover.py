import unittest

from replication_log_shipper import (
    Ack,
    Frame,
    MemoryWalStore,
    PrimaryNode,
    RECORD_BEGIN,
    RECORD_DATA,
    ReplicaReceiver,
    WalWriter,
)

from tests.helpers import append_transaction


def ship_to_receiver(
    primary: PrimaryNode, replica_id: str, receiver: ReplicaReceiver
) -> None:
    for frame in primary.frames_for(replica_id):
        result = receiver.handle_frame(frame)
        if result.outcome == "gap":
            raise AssertionError(f"unexpected gap: {result.detail}")


class TermIsolationTests(unittest.TestCase):
    def test_old_timeline_and_old_epoch_acks_quarantined(self) -> None:
        primary = PrimaryNode(
            "p", timeline_id=2, epoch=3, wal_store=MemoryWalStore()
        )
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        append_transaction(primary, writer, xid=1)
        ship_to_receiver(primary, "s1", _RecordingReplica("s1"))

        stale_timeline = Ack(
            timeline_id=1, epoch=3, replica_id="s1", durable_lsn=100, seqno=1
        )
        stale_epoch = Ack(
            timeline_id=2, epoch=2, replica_id="s1", durable_lsn=100, seqno=2
        )
        self.assertEqual(
            primary.receive_ack(stale_timeline).reason, "stale_term"
        )
        self.assertEqual(
            primary.receive_ack(stale_epoch).reason, "stale_term"
        )
        self.assertEqual(primary.quarantine_count("stale_term"), 2)
        self.assertEqual(primary.safe_lsn, 0)
        self.assertEqual(primary.replica("s1").durable_lsn, 0)

    def test_future_term_ack_quarantined(self) -> None:
        primary = PrimaryNode("p", wal_store=MemoryWalStore())
        primary.add_replica("s1", "slot-s1")
        future = Ack(
            timeline_id=99, epoch=99, replica_id="s1", durable_lsn=1, seqno=1
        )
        self.assertEqual(primary.receive_ack(future).reason, "future_term")


class _RecordingReplica(ReplicaReceiver):
    def __init__(self, replica_id: str) -> None:
        super().__init__(replica_id, MemoryWalStore())


class PromotionTests(unittest.TestCase):
    def test_promote_discards_incomplete_tail_and_resets_ack_state(self) -> None:
        old_primary = PrimaryNode(
            "old", timeline_id=1, epoch=1, wal_store=MemoryWalStore()
        )
        writer = WalWriter()
        old_primary.add_replica("s1", "slot-s1")

        # Transaction 1 fully shipped and ACKed -> safe LSN advanced.
        _b1, end1 = append_transaction(old_primary, writer, xid=1)
        receiver = ReplicaReceiver("s1", MemoryWalStore())
        ship_to_receiver(old_primary, "s1", receiver)
        old_primary.receive_ack(receiver.build_ack())
        self.assertEqual(old_primary.safe_lsn, end1)

        # Transaction 2 is only partially shipped when the old primary dies.
        begin2 = old_primary.append(writer.write(RECORD_BEGIN, 2))
        partial = old_primary.append(
            writer.write(RECORD_DATA, 2, b"never-committed")
        )
        receiver.handle_frame(
            Frame(
                timeline_id=1,
                epoch=1,
                seqno=begin2.seqno,
                record=begin2.record,
            )
        )
        receiver.handle_frame(
            Frame(
                timeline_id=1,
                epoch=1,
                seqno=partial.seqno,
                record=partial.record,
            )
        )

        # Standby promotes. Its durable prefix contains an open txn; the
        # recovered safe LSN must stop at the last complete commit.
        new_primary = receiver.promote(
            "new", new_timeline_id=2, new_epoch=2
        )
        self.assertEqual(new_primary.timeline_id, 2)
        self.assertEqual(new_primary.epoch, 2)
        self.assertEqual(new_primary.safe_lsn, end1)
        self.assertLessEqual(new_primary.safe_lsn, end1)
        self.assertEqual(new_primary.flush_lsn, receiver.durable_lsn)
        self.assertEqual(new_primary.tracker.open_xids, frozenset({2}))

        # A delayed ACK from the old term must never count on the new primary.
        delayed = Ack(
            timeline_id=1,
            epoch=1,
            replica_id="s2",
            durable_lsn=receiver.durable_lsn,
            seqno=99,
        )
        new_primary.add_replica("s2", "slot-s2")
        result = new_primary.receive_ack(delayed)
        self.assertEqual(result.reason, "stale_term")
        self.assertEqual(new_primary.quarantine_count("stale_term"), 1)
        self.assertEqual(new_primary.safe_lsn, end1)

    def test_new_primary_resumes_with_fresh_term_acks(self) -> None:
        old_primary = PrimaryNode("old", wal_store=MemoryWalStore())
        writer = WalWriter()
        old_primary.add_replica("s1", "slot-s1")
        _b, end = append_transaction(old_primary, writer, xid=1)
        receiver = ReplicaReceiver("s1", MemoryWalStore())
        ship_to_receiver(old_primary, "s1", receiver)

        new_primary = receiver.promote(
            "new", new_timeline_id=2, new_epoch=2
        )
        new_primary.add_replica("standby", "slot-standby")
        new_writer = WalWriter(start_lsn=new_primary.flush_lsn)
        _b2, end2 = append_transaction(new_primary, new_writer, xid=2)

        standby = ReplicaReceiver("standby", MemoryWalStore())
        for frame in new_primary.frames_for("standby"):
            self.assertEqual(frame.timeline_id, 2)
            self.assertEqual(frame.epoch, 2)
            standby.handle_frame(frame)
        result = new_primary.receive_ack(standby.build_ack())
        self.assertEqual(result.reason, "applied")
        self.assertEqual(new_primary.safe_lsn, end2)
        self.assertGreaterEqual(end2, end)

    def test_old_primary_promote_resets_replica_progress(self) -> None:
        primary = PrimaryNode("p", wal_store=MemoryWalStore())
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        _b, end = append_transaction(primary, writer, xid=1)
        receiver = _RecordingReplica("s1")
        ship_to_receiver(primary, "s1", receiver)
        primary.receive_ack(
            Ack(1, 1, "s1", end, seqno=1)
        )
        self.assertEqual(primary.safe_lsn, end)

        primary.promote(new_timeline_id=2, new_epoch=2, safe_lsn=end)
        state = primary.replica("s1")
        self.assertEqual(state.durable_lsn, 0)
        self.assertEqual(state.shipped_lsn, 0)
        self.assertEqual(state.last_ack_seqno, -1)
        # An ACK stamped with the previous term is now quarantined.
        result = primary.receive_ack(Ack(1, 1, "s1", end, seqno=2))
        self.assertEqual(result.reason, "stale_term")


if __name__ == "__main__":
    unittest.main()
