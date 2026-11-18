"""Replication slots and persistent WAL segment storage."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from .lsn import SEGMENT_SIZE, segment_no, segment_name
from .protocol import Frame, StreamDecoder, encode_frame
from .records import WalRecord


@dataclass
class ReplicationSlot:
    """A physical replication slot.

    ``restart_lsn`` is the oldest LSN a consumer still needs.  It may only
    move forward; a restarting replica therefore re-requests from exactly the
    point it previously persisted, and the primary must retain WAL back to
    the minimum restart LSN across all slots.
    """

    slot_name: str
    replica_id: str
    restart_lsn: int = 0

    def advance(self, lsn: int) -> bool:
        """Move the restart LSN forward to ``lsn``; never backwards.

        Returns ``True`` when the value actually changed.
        """
        if lsn < 0:
            raise ValueError("restart_lsn must be non-negative")
        if lsn > self.restart_lsn:
            self.restart_lsn = lsn
            return True
        return False


class SlotStore:
    """File-persisted collection of replication slots."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._slots: dict[str, ReplicationSlot] = {}
        if os.path.exists(path):
            self._load()

    def _load(self) -> None:
        with open(self.path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        for item in data["slots"]:
            slot = ReplicationSlot(**item)
            self._slots[slot.slot_name] = slot

    def persist(self) -> None:
        payload = {
            "slots": [
                {
                    "slot_name": slot.slot_name,
                    "replica_id": slot.replica_id,
                    "restart_lsn": slot.restart_lsn,
                }
                for slot in self._slots.values()
            ]
        }
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, self.path)

    def get_or_create(
        self, slot_name: str, replica_id: str
    ) -> ReplicationSlot:
        slot = self._slots.get(slot_name)
        if slot is None:
            slot = ReplicationSlot(slot_name=slot_name, replica_id=replica_id)
            self._slots[slot_name] = slot
            self.persist()
        return slot

    def get(self, slot_name: str) -> ReplicationSlot | None:
        return self._slots.get(slot_name)

    def all(self) -> tuple[ReplicationSlot, ...]:
        return tuple(self._slots.values())

    def min_restart_lsn(self) -> int:
        if not self._slots:
            return 0
        return min(slot.restart_lsn for slot in self._slots.values())


class WalStore:
    """Persistent storage of framed WAL records grouped by segment file.

    Each segment is stored as a stream of length-prefixed :class:`Frame`
    messages, which makes local durability/recovery reuse the exact same
    codec as the network transport (including CRC verification).
    """

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, timeline_id: int, seg_no: int) -> str:
        name = f"{timeline_id:08X}{seg_no:016X}"
        return os.path.join(self.directory, name)

    def append_frame(self, frame: Frame) -> None:
        seg_no = segment_no(frame.record.lsn)
        path = self._path(frame.timeline_id, seg_no)
        with open(path, "ab") as handle:
            handle.write(encode_frame(frame))
            handle.flush()
            os.fsync(handle.fileno())

    def has_segment(self, timeline_id: int, seg_no: int) -> bool:
        return os.path.exists(self._path(timeline_id, seg_no))

    def list_segments(self, timeline_id: int) -> list[int]:
        prefix = f"{timeline_id:08X}"
        result = []
        for name in os.listdir(self.directory):
            if name.startswith(prefix) and len(name) == 24:
                result.append(int(name[8:], 16))
        return sorted(result)

    def load_segment(
        self, timeline_id: int, seg_no: int
    ) -> list[WalRecord]:
        path = self._path(timeline_id, seg_no)
        if not os.path.exists(path):
            return []
        with open(path, "rb") as handle:
            raw = handle.read()
        decoder = StreamDecoder()
        messages = decoder.feed(raw)
        records: list[WalRecord] = []
        for message in messages:
            if not isinstance(message, Frame):
                continue
            records.append(message.record)
        return records

    def load_segment_frames(
        self, timeline_id: int, seg_no: int
    ) -> list[Frame]:
        path = self._path(timeline_id, seg_no)
        if not os.path.exists(path):
            return []
        with open(path, "rb") as handle:
            raw = handle.read()
        decoder = StreamDecoder()
        return [m for m in decoder.feed(raw) if isinstance(m, Frame)]

    def retained_horizon_lsn(self) -> int:
        """Smallest LSN still retained (across all timelines), else 0."""
        smallest = None
        for name in os.listdir(self.directory):
            if len(name) != 24:
                continue
            seg_no = int(name[8:], 16)
            lsn = seg_no * SEGMENT_SIZE
            smallest = lsn if smallest is None else min(smallest, lsn)
        return 0 if smallest is None else smallest

    def recycle_before(self, lsn: int) -> list[str]:
        """Delete whole segments entirely below ``lsn``; return removed names."""
        cutoff_seg = segment_no(lsn)
        removed: list[str] = []
        for name in list(os.listdir(self.directory)):
            if len(name) != 24:
                continue
            seg_no = int(name[8:], 16)
            if seg_no < cutoff_seg:
                os.remove(os.path.join(self.directory, name))
                removed.append(segment_name(int(name[:8], 16), seg_no * SEGMENT_SIZE))
        return removed


class MemoryWalStore:
    """In-memory twin of :class:`WalStore` with the same frame-level API."""

    def __init__(self) -> None:
        self._segments: dict[tuple[int, int], list[Frame]] = {}

    def append_frame(self, frame: Frame) -> None:
        key = (frame.timeline_id, segment_no(frame.record.lsn))
        self._segments.setdefault(key, []).append(frame)

    def has_segment(self, timeline_id: int, seg_no: int) -> bool:
        return (timeline_id, seg_no) in self._segments

    def list_segments(self, timeline_id: int) -> list[int]:
        return sorted(
            seg_no for (tl, seg_no) in self._segments if tl == timeline_id
        )

    def load_segment(self, timeline_id: int, seg_no: int) -> list[WalRecord]:
        return [f.record for f in self.load_segment_frames(timeline_id, seg_no)]

    def load_segment_frames(
        self, timeline_id: int, seg_no: int
    ) -> list[Frame]:
        return list(self._segments.get((timeline_id, seg_no), []))

    def retained_horizon_lsn(self) -> int:
        if not self._segments:
            return 0
        smallest_seg = min(seg_no for _tl, seg_no in self._segments)
        return smallest_seg * SEGMENT_SIZE

    def recycle_before(self, lsn: int) -> list[str]:
        cutoff_seg = segment_no(lsn)
        removed: list[str] = []
        for key in list(self._segments):
            timeline_id, seg_no = key
            if seg_no < cutoff_seg:
                del self._segments[key]
                removed.append(
                    segment_name(timeline_id, seg_no * SEGMENT_SIZE)
                )
        return removed
