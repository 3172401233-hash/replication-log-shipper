"""Replica-side WAL receiver.

The receiver enforces a gap-free durable prefix.  Frames arriving ahead of
the next expected LSN are rejected (the primary must resend from the slot
boundary); duplicate frames at or below the durable LSN are ignored.  Every
ACK is tagged with the term on which the WAL was received, so a demoted
primary can identify and quarantine delayed ACKs after a switchover.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from .primary import PrimaryNode
from .protocol import Ack, Frame
from .records import TransactionTracker
from .slot import MemoryWalStore, SlotStore, WalStore


@dataclass
class ReceiveResult:
    outcome: str  # "applied" | "duplicate" | "gap" | "stale_term"
    durable_lsn: int
    detail: str = ""


class ReplicaReceiver:
    def __init__(
        self,
        replica_id: str,
        wal_store: WalStore | MemoryWalStore | None = None,
        state_path: str | None = None,
    ) -> None:
        self.replica_id = replica_id
        self.wal = wal_store if wal_store is not None else MemoryWalStore()
        self.state_path = state_path
        self.durable_lsn = 0
        self.expected_lsn = 0
        self.current_timeline_id: int | None = None
        self.current_epoch: int | None = None
        self._last_frame_seqno = -1
        self._ack_seqno = 0
        self.tracker = TransactionTracker()
        if state_path is not None and os.path.exists(state_path):
            self._load_state()

    # ------------------------------------------------------------- frame ingest

    def handle_frame(self, frame: Frame) -> ReceiveResult:
        # Term filtering: frames from an older primary incarnation are
        # ignored.  The first frame pins the receiver to its term.
        if self.current_timeline_id is not None:
            if (frame.timeline_id, frame.epoch) < (
                self.current_timeline_id,
                self.current_epoch,
            ):
                return ReceiveResult(
                    outcome="stale_term",
                    durable_lsn=self.durable_lsn,
                    detail="frame belongs to an older term",
                )

        record = frame.record

        # Re-delivery of already durable WAL (slot restart after a crash).
        if record.end_lsn <= self.durable_lsn:
            return ReceiveResult(
                outcome="duplicate", durable_lsn=self.durable_lsn
            )

        # Gap protection: never confirm durability past missing bytes.
        if record.lsn != self.expected_lsn:
            return ReceiveResult(
                outcome="gap",
                durable_lsn=self.durable_lsn,
                detail=(
                    f"expected LSN {self.expected_lsn}, "
                    f"got LSN {record.lsn}"
                ),
            )

        if self.current_timeline_id is None:
            self.current_timeline_id = frame.timeline_id
            self.current_epoch = frame.epoch

        self.wal.append_frame(frame)  # fsyncs for the file-backed store
        self.tracker.handle(record)
        self._last_frame_seqno = frame.seqno
        self.durable_lsn = record.end_lsn
        self.expected_lsn = record.end_lsn
        self._persist_state()
        return ReceiveResult(
            outcome="applied", durable_lsn=self.durable_lsn
        )

    # -------------------------------------------------------------------- ACKs

    def build_ack(self) -> Ack:
        if self.current_timeline_id is None or self.current_epoch is None:
            raise RuntimeError("cannot ACK before receiving any frame")
        self._ack_seqno += 1
        self._persist_state()
        return Ack(
            timeline_id=self.current_timeline_id,
            epoch=self.current_epoch,
            replica_id=self.replica_id,
            durable_lsn=self.durable_lsn,
            seqno=self._ack_seqno,
        )

    # --------------------------------------------------------------- promotion

    def promote(
        self,
        node_id: str,
        new_timeline_id: int,
        new_epoch: int,
        required_syncs: int = 1,
        slot_store: SlotStore | None = None,
    ) -> PrimaryNode:
        """Promote this standby into a primary on a new term.

        Only the gap-free durable prefix becomes the new primary's WAL.
        Incomplete transactions above the last committed prefix are carried
        as open tracker state but never advance the safe LSN.
        """
        if (
            self.current_timeline_id is not None
            and new_timeline_id <= self.current_timeline_id
        ):
            raise ValueError("promotion requires a greater timeline_id")
        primary = PrimaryNode(
            node_id=node_id,
            timeline_id=new_timeline_id,
            epoch=new_epoch,
            required_syncs=required_syncs,
            wal_store=self.wal,
            slot_store=slot_store,
            safe_lsn=self.durable_lsn,
        )
        primary.flush_lsn = self.durable_lsn
        # Replay durable records to reconstruct transaction state.
        migrated: list[Frame] = []
        for seg_no in self.wal.list_segments(self.current_timeline_id or 0):
            for record in self.wal.load_segment(
                self.current_timeline_id or 0, seg_no
            ):
                if record.lsn < self.durable_lsn:
                    primary.tracker.handle(record)
                    migrated.append(
                        Frame(
                            timeline_id=new_timeline_id,
                            epoch=new_epoch,
                            seqno=len(migrated),
                            record=record,
                        )
                    )
        # Re-tag the recovered prefix onto the new term so the new primary
        # can ship it from replication slots on the new timeline.
        for frame in migrated:
            primary.wal.append_frame(frame)
        primary._next_seqno = len(migrated)
        recovered_safe = primary.tracker.committed_prefix_end(
            self.durable_lsn
        )
        primary.safe_lsn = recovered_safe
        return primary

    # ------------------------------------------------------------- persistence

    def _state_file(self) -> str:
        assert self.state_path is not None
        return self.state_path

    def _persist_state(self) -> None:
        if self.state_path is None:
            return
        payload = {
            "replica_id": self.replica_id,
            "durable_lsn": self.durable_lsn,
            "expected_lsn": self.expected_lsn,
            "timeline_id": self.current_timeline_id,
            "epoch": self.current_epoch,
            "last_frame_seqno": self._last_frame_seqno,
            "ack_seqno": self._ack_seqno,
        }
        os.makedirs(os.path.dirname(os.path.abspath(self.state_path)), exist_ok=True)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, self.state_path)

    def _load_state(self) -> None:
        with open(self._state_file(), "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        self.durable_lsn = payload["durable_lsn"]
        self.expected_lsn = payload["expected_lsn"]
        self.current_timeline_id = payload["timeline_id"]
        self.current_epoch = payload["epoch"]
        self._last_frame_seqno = payload["last_frame_seqno"]
        self._ack_seqno = payload["ack_seqno"]
        # Replay WAL to rebuild transaction boundary state.
        if self.current_timeline_id is not None:
            for seg_no in self.wal.list_segments(self.current_timeline_id):
                for record in self.wal.load_segment(
                    self.current_timeline_id, seg_no
                ):
                    if record.lsn < self.durable_lsn:
                        self.tracker.handle(record)
