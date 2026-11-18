"""Shared helpers for building WAL streams in tests."""

from __future__ import annotations

from replication_log_shipper.primary import PrimaryNode
from replication_log_shipper.records import (
    RECORD_ABORT,
    RECORD_BEGIN,
    RECORD_COMMIT,
    RECORD_DATA,
    WalWriter,
)


def append_transaction(
    primary: PrimaryNode,
    writer: WalWriter,
    xid: int,
    payloads: list[bytes] | None = None,
    abort: bool = False,
) -> tuple[int, int]:
    """Append BEGIN / DATA* / COMMIT (or ABORT); return (begin, end) LSN."""
    begin = primary.append(writer.write(RECORD_BEGIN, xid)).record
    for payload in payloads or []:
        primary.append(writer.write(RECORD_DATA, xid, payload))
    end_type = RECORD_ABORT if abort else RECORD_COMMIT
    last = primary.append(writer.write(end_type, xid))
    return begin.lsn, last.record.end_lsn


def new_writer_at_segment_boundary(
    segment_size: int, segment_index: int = 1
) -> WalWriter:
    return WalWriter(start_lsn=segment_size * segment_index)
