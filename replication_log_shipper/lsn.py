"""LSN helpers and WAL segment geometry.

An LSN is represented internally as a plain non-negative ``int`` (a byte
offset into the infinite WAL byte stream of a timeline).  16 MiB segments
follow the usual physical-replication convention.
"""

from __future__ import annotations

SEGMENT_SIZE = 1 << 24  # 16 MiB


def parse_lsn(text: str) -> int:
    """Parse ``"0/16B3748"`` style LSN strings into integer byte offsets."""
    if not isinstance(text, str) or "/" not in text:
        raise ValueError(f"invalid LSN literal: {text!r}")
    high, low = text.split("/", 1)
    value = (int(high, 16) << 32) | int(low, 16)
    if value < 0:
        raise ValueError(f"invalid LSN literal: {text!r}")
    return value


def format_lsn(lsn: int) -> str:
    """Inverse of :func:`parse_lsn`."""
    if lsn < 0:
        raise ValueError("LSN must be non-negative")
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def segment_no(lsn: int) -> int:
    """Zero-based index of the segment containing ``lsn``."""
    return lsn // SEGMENT_SIZE


def segment_start_lsn(lsn: int) -> int:
    """First LSN of the segment containing ``lsn``."""
    return (lsn // SEGMENT_SIZE) * SEGMENT_SIZE


def next_segment_lsn(lsn: int) -> int:
    """First LSN of the segment after the one containing ``lsn``."""
    return segment_start_lsn(lsn) + SEGMENT_SIZE


def segment_offset(lsn: int) -> int:
    """Byte offset of ``lsn`` inside its segment."""
    return lsn % SEGMENT_SIZE


def segment_name(timeline_id: int, lsn: int) -> str:
    """PostgreSQL-style segment file name, e.g. ``"000000010000000000000001"``."""
    if timeline_id < 0:
        raise ValueError("timeline_id must be non-negative")
    return f"{timeline_id:08X}{segment_no(lsn):016X}"


def parse_segment_name(name: str) -> tuple[int, int]:
    """Return ``(timeline_id, segment_no)`` for a segment file name."""
    if len(name) != 24:
        raise ValueError(f"invalid segment name: {name!r}")
    return int(name[:8], 16), int(name[8:], 16)
