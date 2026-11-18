import os
import tempfile
import unittest

from replication_log_shipper import (
    Frame,
    MemoryWalStore,
    PrimaryNode,
    RECORD_BEGIN,
    RECORD_COMMIT,
    RECORD_DATA,
    ReplicaReceiver,
    StreamDecoder,
    WalRecord,
    WalStore,
    WalWriter,
    encode_frame,
)

from tests.helpers import append_transaction

class ReplicaReceiverTests(unittest.TestCase):
    def test_apply_duplicate_and_gap(self) -> None:
        receiver = ReplicaReceiver("r1", MemoryWalStore())
        begin = Frame(
            timeline_id=1,
            epoch=1,
            seqno=0,
            record=WalRecord(0, 1, RECORD_BEGIN, 1),
        )
        frame = Frame(
            timeline_id=1,
            epoch=1,
            seqno=1,
            record=WalRecord(1, 6, RECORD_DATA, 1, b"hello"),
        )
        self.assertEqual(receiver.handle_frame(begin).outcome, "applied")
        first = receiver.handle_frame(frame)
        self.assertEqual(first.outcome, "applied")
        self.assertEqual(first.durable_lsn, 6)

        duplicate = receiver.handle_frame(frame)
        self.assertEqual(duplicate.outcome, "duplicate")

        jumped = Frame(
            timeline_id=1,
            epoch=1,
            seqno=2,
            record=WalRecord(10, 13, RECORD_DATA, 1, b"gap"),
        )
        gap = receiver.handle_frame(jumped)
        self.assertEqual(gap.outcome, "gap")
        self.assertEqual(receiver.durable_lsn, 6)

    def test_stale_term_frames_ignored(self) -> None:
        receiver = ReplicaReceiver("r1", MemoryWalStore())
        frame = Frame(
            timeline_id=2,
            epoch=2,
            seqno=0,
            record=WalRecord(0, 1, RECORD_BEGIN, 1),
        )
        receiver.handle_frame(frame)
        stale = Frame(
            timeline_id=1,
            epoch=1,
            seqno=0,
            record=WalRecord(0, 1, RECORD_BEGIN, 1),
        )
        result = receiver.handle_frame(stale)
        self.assertEqual(result.outcome, "stale_term")

    def test_ack_carries_current_term_and_durable_lsn(self) -> None:
        receiver = ReplicaReceiver("r1", MemoryWalStore())
        receiver.handle_frame(
            Frame(
                timeline_id=1,
                epoch=1,
                seqno=0,
                record=WalRecord(0, 1, RECORD_BEGIN, 1),
            )
        )
        ack = receiver.build_ack()
        self.assertEqual(ack.timeline_id, 1)
        self.assertEqual(ack.epoch, 1)
        self.assertEqual(ack.durable_lsn, 1)
        self.assertEqual(ack.replica_id, "r1")

    def test_streaming_half_packets_then_ack(self) -> None:
        primary = PrimaryNode("p", wal_store=MemoryWalStore())
        writer = WalWriter()
        primary.add_replica("r1", "slot-r1")
        append_transaction(primary, writer, xid=1, payloads=[b"payload"])
        frames = primary.frames_for("r1")

        receiver = ReplicaReceiver("r1", MemoryWalStore())
        decoder = StreamDecoder()
        for frame in frames:
            raw = encode_frame(frame)
            # Deliver one byte at a time to force half-packet buffering.
            for index in range(0, len(raw), 7):
                for decoded in decoder.feed(raw[index:index + 7]):
                    result = receiver.handle_frame(decoded)
                    self.assertNotEqual(result.outcome, "gap")
        self.assertEqual(receiver.durable_lsn, primary.flush_lsn)
        primary.receive_ack(receiver.build_ack())
        self.assertEqual(primary.safe_lsn, primary.flush_lsn)


class ReplicaRestartTests(unittest.TestCase):
    def test_state_and_wal_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            wal_dir = os.path.join(tmp, "wal")
            state_path = os.path.join(tmp, "state.json")
            receiver = ReplicaReceiver(
                "r1", WalStore(wal_dir), state_path=state_path
            )
            frame = Frame(
                timeline_id=1,
                epoch=1,
                seqno=0,
                record=WalRecord(0, 5, RECORD_BEGIN, 1),
            )
            receiver.handle_frame(frame)
            receiver.handle_frame(
                Frame(
                    timeline_id=1,
                    epoch=1,
                    seqno=1,
                    record=WalRecord(5, 10, RECORD_COMMIT, 1),
                )
            )
            receiver.build_ack()

            reopened = ReplicaReceiver(
                "r1", WalStore(wal_dir), state_path=state_path
            )
            self.assertEqual(reopened.durable_lsn, 10)
            self.assertEqual(reopened.expected_lsn, 10)
            self.assertEqual(reopened.tracker.committed_prefix_end(10), 10)

            # Next contiguous frame is accepted; replays are duplicates.
            cont = reopened.handle_frame(
                Frame(
                    timeline_id=1,
                    epoch=1,
                    seqno=2,
                    record=WalRecord(10, 11, RECORD_BEGIN, 2),
                )
            )
            self.assertEqual(cont.outcome, "applied")
            replay = reopened.handle_frame(frame)
            self.assertEqual(replay.outcome, "duplicate")


if __name__ == "__main__":
    unittest.main()
