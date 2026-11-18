import unittest

from replication_log_shipper import (
    SEGMENT_SIZE,
    RECORD_ABORT,
    RECORD_BEGIN,
    RECORD_COMMIT,
    RECORD_DATA,
    TransactionTracker,
    WalRecord,
    WalWriter,
    format_lsn,
    parse_lsn,
    segment_name,
    segment_no,
    segment_offset,
    segment_start_lsn,
)


class LsnTests(unittest.TestCase):
    def test_parse_format_roundtrip(self) -> None:
        for text in ("0/00000000", "0/016B3748", "1/FFFFFFFF", "AB/CD000000"):
            self.assertEqual(format_lsn(parse_lsn(text)), text)
        self.assertEqual(parse_lsn("0/0"), 0)

    def test_parse_rejects_garbage(self) -> None:
        with self.assertRaises(ValueError):
            parse_lsn("not-an-lsn")

    def test_segment_geometry(self) -> None:
        lsn = SEGMENT_SIZE + 7
        self.assertEqual(segment_no(lsn), 1)
        self.assertEqual(segment_offset(lsn), 7)
        self.assertEqual(segment_start_lsn(lsn), SEGMENT_SIZE)

    def test_segment_name_roundtrip(self) -> None:
        name = segment_name(1, SEGMENT_SIZE)
        self.assertEqual(name, "000000010000000000000001")


class TransactionTrackerTests(unittest.TestCase):
    def _txn(self, xid: int, begin: int, end: int) -> list[WalRecord]:
        return [
            WalRecord(begin, begin + 1, RECORD_BEGIN, xid),
            WalRecord(begin + 1, end - 1, RECORD_DATA, xid, b"x"),
            WalRecord(end - 1, end, RECORD_COMMIT, xid),
        ]

    def test_only_complete_transactions_visible(self) -> None:
        tracker = TransactionTracker()
        for record in self._txn(10, 0, 10):
            tracker.handle(record)
        self.assertEqual(tracker.committed_prefix_end(100), 10)

        # Open transaction contributes nothing.
        tracker.handle(WalRecord(10, 11, RECORD_BEGIN, 11))
        tracker.handle(WalRecord(11, 20, RECORD_DATA, 11, b"y"))
        self.assertEqual(tracker.committed_prefix_end(100), 10)

        tracker.handle(WalRecord(20, 25, RECORD_COMMIT, 11))
        self.assertEqual(tracker.committed_prefix_end(100), 25)

    def test_open_transaction_blocks_later_commit_prefix(self) -> None:
        tracker = TransactionTracker()
        tracker.handle(WalRecord(0, 1, RECORD_BEGIN, 1))
        tracker.handle(WalRecord(1, 5, RECORD_DATA, 1, b"a"))
        for record in self._txn(2, 5, 12):
            tracker.handle(record)
        # xid 2 committed, but xid 1 is still open and precedes it.
        self.assertEqual(tracker.open_xids, frozenset({1}))
        self.assertEqual(tracker.committed_prefix_end(100), 0)

    def test_aborted_transaction_never_commits(self) -> None:
        tracker = TransactionTracker()
        tracker.handle(WalRecord(0, 1, RECORD_BEGIN, 7))
        tracker.handle(WalRecord(1, 4, RECORD_DATA, 7, b"z"))
        tracker.handle(WalRecord(4, 5, RECORD_ABORT, 7))
        self.assertEqual(tracker.committed_prefix_end(100), 0)
        self.assertIn(7, tracker.aborted_xids)

    def test_commit_without_begin_rejected(self) -> None:
        tracker = TransactionTracker()
        with self.assertRaises(ValueError):
            tracker.handle(WalRecord(0, 1, RECORD_COMMIT, 9))

    def test_transaction_spanning_segments(self) -> None:
        writer = WalWriter(start_lsn=SEGMENT_SIZE - 2)
        tracker = TransactionTracker()
        begin = writer.write(RECORD_BEGIN, 3)
        data = writer.write(RECORD_DATA, 3, b"payload")
        commit = writer.write(RECORD_COMMIT, 3)
        self.assertNotEqual(segment_no(begin.lsn), segment_no(commit.end_lsn))
        for record in (begin, data, commit):
            tracker.handle(record)
        self.assertEqual(
            tracker.committed_prefix_end(commit.end_lsn), commit.end_lsn
        )


class WalWriterTests(unittest.TestCase):
    def test_allocates_contiguous_lsns(self) -> None:
        writer = WalWriter(100)
        first = writer.write(RECORD_BEGIN, 1, b"")
        second = writer.write(RECORD_DATA, 1, b"abcd")
        self.assertEqual(first.lsn, 100)
        self.assertEqual(first.end_lsn, 101)
        self.assertEqual(second.lsn, 101)
        self.assertEqual(second.end_lsn, 105)
        self.assertEqual(writer.next_lsn, 105)


if __name__ == "__main__":
    unittest.main()
