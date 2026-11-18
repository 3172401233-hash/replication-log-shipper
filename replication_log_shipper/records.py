"""WAL record model and transaction-boundary tracking.

The library is agnostic to real on-disk record encodings.  A :class:`WalRecord`
is the unit that gets framed and shipped; records carry an explicit type so
that transaction boundaries (BEGIN / COMMIT / ABORT) can be recognised
independently of WAL segment boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass

RECORD_BEGIN = 1
RECORD_DATA = 2
RECORD_COMMIT = 3
RECORD_ABORT = 4

RECORD_TYPES = frozenset(
    {RECORD_BEGIN, RECORD_DATA, RECORD_COMMIT, RECORD_ABORT}
)


@dataclass(frozen=True)
class WalRecord:
    """One WAL record.

    ``lsn`` is the record's start LSN and ``end_lsn`` is the first LSN after
    its payload, so adjacent records tile the WAL byte stream without gaps.
    """

    lsn: int
    end_lsn: int
    record_type: int
    xid: int
    payload: bytes = b""

    def __post_init__(self) -> None:
        if self.lsn < 0 or self.end_lsn <= self.lsn:
            raise ValueError("record must satisfy 0 <= lsn < end_lsn")
        if self.record_type not in RECORD_TYPES:
            raise ValueError(f"unknown record type: {self.record_type}")
        if not isinstance(self.payload, (bytes, bytearray)):
            raise TypeError("payload must be bytes")

    @property
    def length(self) -> int:
        return self.end_lsn - self.lsn


class TransactionTracker:
    """Tracks which LSN ranges belong to *complete* transactions.

    Only a COMMIT record finalises a transaction.  DATA records emitted before
    the matching COMMIT (potentially across many WAL segments) are explicitly
    not durable-visible, which is what prevents the safe LSN from advancing
    past an in-flight transaction.
    """

    def __init__(self) -> None:
        self._open: dict[int, int] = {}
        # Ordered list of (commit_end_lsn, xid) for committed transactions.
        self._committed: list[tuple[int, int]] = []
        self._aborted: set[int] = set()

    def handle(self, record: WalRecord) -> None:
        xid = record.xid
        if record.record_type == RECORD_BEGIN:
            if xid in self._open:
                raise ValueError(f"xid {xid} already in progress")
            self._open[xid] = record.lsn
        elif record.record_type == RECORD_DATA:
            if xid not in self._open:
                raise ValueError(f"data record for unknown xid {xid}")
        elif record.record_type == RECORD_COMMIT:
            if xid not in self._open:
                raise ValueError(f"commit without begin for xid {xid}")
            del self._open[xid]
            self._committed.append((record.end_lsn, xid))
        elif record.record_type == RECORD_ABORT:
            if xid not in self._open:
                raise ValueError(f"abort without begin for xid {xid}")
            del self._open[xid]
            self._aborted.add(xid)

    @property
    def open_xids(self) -> frozenset[int]:
        return frozenset(self._open)

    @property
    def committed_end_lsns(self) -> tuple[int, ...]:
        return tuple(end for end, _ in self._committed)

    def committed_prefix_end(self, horizon: int) -> int:
        """End LSN of the longest fully-committed prefix not past ``horizon``.

        Committed transactions are processed in commit order.  An open
        transaction whose BEGIN is before a later commit blocks the prefix:
        its changes are interleaved in the byte stream and are not known to
        be durable, so the safe LSN must stop before the earliest open BEGIN.
        """
        blocking = min(self._open.values(), default=horizon + 1)
        result = 0
        for end_lsn, _xid in self._committed:
            if end_lsn > horizon or end_lsn > blocking:
                break
            result = end_lsn
        return result

    def truncate_before(self, lsn: int) -> None:
        """Drop history at or below ``lsn`` (after promotion/restart).

        Open transactions that began before the divergence point cannot
        survive the timeline switch, so their state is discarded too.
        """
        self._committed = [
            (end, xid) for end, xid in self._committed if end > lsn
        ]
        self._open = {
            xid: begin
            for xid, begin in self._open.items()
            if begin >= lsn
        }

    @property
    def aborted_xids(self) -> frozenset[int]:
        return frozenset(self._aborted)


class WalWriter:
    """Allocates contiguous, gap-free LSNs for newly appended records."""

    def __init__(self, start_lsn: int = 0) -> None:
        if start_lsn < 0:
            raise ValueError("start_lsn must be non-negative")
        self._next_lsn = start_lsn

    @property
    def next_lsn(self) -> int:
        return self._next_lsn

    def write(
        self, record_type: int, xid: int, payload: bytes = b""
    ) -> WalRecord:
        length = max(1, len(payload))
        record = WalRecord(
            lsn=self._next_lsn,
            end_lsn=self._next_lsn + length,
            record_type=record_type,
            xid=xid,
            payload=payload,
        )
        self._next_lsn = record.end_lsn
        return record
