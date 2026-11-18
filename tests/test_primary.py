import unittest

from replication_log_shipper import (
    Ack,
    MemoryWalStore,
    PrimaryNode,
    WalWriter,
)
from replication_log_shipper.records import RECORD_BEGIN, RECORD_DATA

from tests.helpers import append_transaction


def build_primary(required_syncs: int = 1) -> PrimaryNode:
    primary = PrimaryNode(
        node_id="primary",
        timeline_id=1,
        epoch=1,
        required_syncs=required_syncs,
        wal_store=MemoryWalStore(),
    )
    return primary


def ship_all(primary: PrimaryNode, replica_id: str) -> None:
    frames = primary.frames_for(replica_id)
    primary.replica(replica_id)
    for _frame in frames:
        pass


def ack(
    primary: PrimaryNode,
    replica_id: str,
    durable_lsn: int,
    seqno: int,
    timeline_id: int | None = None,
    epoch: int | None = None,
) -> Ack:
    return Ack(
        timeline_id=primary.timeline_id if timeline_id is None else timeline_id,
        epoch=primary.epoch if epoch is None else epoch,
        replica_id=replica_id,
        durable_lsn=durable_lsn,
        seqno=seqno,
    )


class SafeLsnTests(unittest.TestCase):
    def setUp(self) -> None:
        self.primary = build_primary()
        self.writer = WalWriter()
        self.primary.add_replica("s1", "slot-s1")

    def test_safe_lsn_waits_for_complete_committed_transaction(self) -> None:
        begin, end = append_transaction(
            self.primary, self.writer, xid=1, payloads=[b"row-1"]
        )
        ship_all(self.primary, "s1")

        # ACK only the in-segment offset up to mid-transaction DATA record.
        state = self.primary.replica("s1")
        mid = begin + 1  # inside the transaction, before COMMIT
        result = self.primary.receive_ack(
            ack(self.primary, "s1", mid, seqno=1)
        )
        self.assertTrue(result.accepted)
        self.assertEqual(self.primary.safe_lsn, 0)

        # Full durability through COMMIT now advances the safe LSN.
        result = self.primary.receive_ack(
            ack(self.primary, "s1", end, seqno=2)
        )
        self.assertEqual(result.safe_lsn, end)
        self.assertEqual(state.slot.restart_lsn, end)

    def test_safe_lsn_never_passes_open_transaction(self) -> None:
        append_transaction(self.primary, self.writer, xid=1)
        first_end = self.primary.flush_lsn
        ship_all(self.primary, "s1")
        self.primary.receive_ack(ack(self.primary, "s1", first_end, 1))
        self.assertEqual(self.primary.safe_lsn, first_end)

        # Second transaction stays open (BEGIN + DATA, no COMMIT).
        self.primary.append(self.writer.write(RECORD_BEGIN, 2))
        self.primary.append(self.writer.write(RECORD_DATA, 2, b"partial"))
        ship_all(self.primary, "s1")
        self.primary.receive_ack(
            ack(self.primary, "s1", self.primary.flush_lsn, 2)
        )
        self.assertEqual(self.primary.safe_lsn, first_end)

    def test_aborted_transaction_does_not_advance_safe_lsn(self) -> None:
        append_transaction(self.primary, self.writer, xid=1, abort=True)
        ship_all(self.primary, "s1")
        self.primary.receive_ack(
            ack(self.primary, "s1", self.primary.flush_lsn, 1)
        )
        self.assertEqual(self.primary.safe_lsn, 0)

    def test_safe_lsn_is_monotonic_on_repeated_acks(self) -> None:
        _begin, end = append_transaction(self.primary, self.writer, xid=1)
        ship_all(self.primary, "s1")
        self.primary.receive_ack(ack(self.primary, "s1", end, 1))
        before = self.primary.safe_lsn
        # Replayed/duplicate ACK must not move anything backwards.
        self.primary.receive_ack(ack(self.primary, "s1", end, 1))
        self.assertEqual(self.primary.safe_lsn, before)


class QuorumTests(unittest.TestCase):
    def test_quorum_requires_enough_sync_replicas(self) -> None:
        primary = build_primary(required_syncs=2)
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        primary.add_replica("s2", "slot-s2")
        _begin, end = append_transaction(primary, writer, xid=1)
        ship_all(primary, "s1")
        ship_all(primary, "s2")

        primary.receive_ack(ack(primary, "s1", end, 1))
        self.assertEqual(primary.safe_lsn, 0)
        primary.receive_ack(ack(primary, "s2", end, 1))
        self.assertEqual(primary.safe_lsn, end)

    def test_async_replica_does_not_count(self) -> None:
        primary = build_primary(required_syncs=1)
        writer = WalWriter()
        primary.add_replica("sync-1", "slot-sync-1", sync=True)
        primary.add_replica("async-1", "slot-async-1", sync=False)
        _begin, end = append_transaction(primary, writer, xid=1)
        ship_all(primary, "sync-1")
        ship_all(primary, "async-1")

        primary.receive_ack(ack(primary, "async-1", end, 1))
        self.assertEqual(primary.safe_lsn, 0)
        primary.receive_ack(ack(primary, "sync-1", end, 1))
        self.assertEqual(primary.safe_lsn, end)

    def test_sync_set_changes_shrink_and_regrow_quorum(self) -> None:
        primary = build_primary(required_syncs=2)
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        primary.add_replica("s2", "slot-s2")
        _begin, end = append_transaction(primary, writer, xid=1)
        ship_all(primary, "s1")
        ship_all(primary, "s2")
        primary.receive_ack(ack(primary, "s1", end, 1))
        primary.receive_ack(ack(primary, "s2", end, 1))
        self.assertEqual(primary.safe_lsn, end)

        # s2 drops out of the synchronous set: with only one sync standby,
        # required_syncs=2 can no longer be met; safe LSN must stay frozen.
        primary.set_sync_replicas({"s1"})
        _b2, end2 = append_transaction(primary, writer, xid=2)
        ship_all(primary, "s1")
        primary.receive_ack(ack(primary, "s1", end2, 2))
        self.assertEqual(primary.safe_lsn, end)

        # A new synchronous standby joins and catches up.
        primary.add_replica("s3", "slot-s3")
        primary.set_sync_replicas({"s1", "s3"})
        ship_all(primary, "s3")
        primary.receive_ack(ack(primary, "s3", end2, 1))
        self.assertEqual(primary.safe_lsn, end2)


class AckValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.primary = build_primary()
        self.writer = WalWriter()
        self.primary.add_replica("s1", "slot-s1")
        append_transaction(self.primary, self.writer, xid=1)
        ship_all(self.primary, "s1")

    def test_duplicate_ack_is_idempotent(self) -> None:
        first = self.primary.receive_ack(
            ack(self.primary, "s1", self.primary.flush_lsn, 5)
        )
        second = self.primary.receive_ack(
            ack(self.primary, "s1", self.primary.flush_lsn, 5)
        )
        self.assertEqual(first.reason, "applied")
        self.assertEqual(second.reason, "duplicate")
        self.assertEqual(
            self.primary.replica("s1").last_ack_seqno, 5
        )

    def test_overclaim_ack_is_quarantined(self) -> None:
        result = self.primary.receive_ack(
            ack(self.primary, "s1", self.primary.flush_lsn + 999, 1)
        )
        self.assertFalse(result.accepted)
        self.assertEqual(result.reason, "overclaim")
        self.assertEqual(self.primary.quarantine_count("overclaim"), 1)
        self.assertEqual(self.primary.safe_lsn, 0)

    def test_unknown_replica_ack_is_quarantined(self) -> None:
        result = self.primary.receive_ack(
            ack(self.primary, "ghost", 10, 1)
        )
        self.assertEqual(result.reason, "unknown_replica")
        self.assertEqual(self.primary.quarantine_count(), 1)

    def test_lagging_replica_does_not_hold_back_others(self) -> None:
        self.primary.add_replica("s2", "slot-s2")
        end = self.primary.flush_lsn
        ship_all(self.primary, "s2")
        # s1 fully durable, s2 only partially.
        self.primary.receive_ack(ack(self.primary, "s1", end, 1))
        self.primary.receive_ack(ack(self.primary, "s2", 1, 1))
        self.assertEqual(self.primary.safe_lsn, end)


class GapAndSegmentTests(unittest.TestCase):
    def test_resend_from_slot_after_restart(self) -> None:
        primary = build_primary()
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        append_transaction(primary, writer, xid=1)
        first_batch = primary.frames_for("s1")
        self.assertTrue(first_batch)
        # Replica crashes before ACKing: slot still at 0, frames re-shipped.
        second_batch = primary.frames_for("s1")
        self.assertEqual(
            [f.seqno for f in first_batch], [f.seqno for f in second_batch]
        )

    def test_frames_are_gap_free_in_lsn_order(self) -> None:
        primary = build_primary()
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        for xid in range(1, 4):
            append_transaction(primary, writer, xid=xid)
        frames = primary.frames_for("s1")
        lsns = [f.record.lsn for f in frames]
        self.assertEqual(lsns, sorted(lsns))
        for earlier, later in zip(frames, frames[1:]):
            self.assertEqual(earlier.record.end_lsn, later.record.lsn)

    def test_max_bytes_pagination_is_gap_free(self) -> None:
        primary = build_primary()
        writer = WalWriter()
        primary.add_replica("s1", "slot-s1")
        for xid in range(1, 5):
            append_transaction(
                primary, writer, xid=xid, payloads=[b"x" * 128]
            )
        collected = []
        while True:
            batch = primary.frames_for("s1", max_bytes=200)
            if not batch:
                break
            collected.extend(batch)
            # Pretend the replica ACKs the batch so the slot advances.
            end = batch[-1].record.end_lsn
            primary.receive_ack(
                ack(primary, "s1", end, seqno=len(collected))
            )
        full = primary.frames_for("s1")
        self.assertEqual(full, [])
        self.assertEqual(len(collected), primary._next_seqno)
        for earlier, later in zip(collected, collected[1:]):
            self.assertEqual(earlier.record.end_lsn, later.record.lsn)


if __name__ == "__main__":
    unittest.main()
