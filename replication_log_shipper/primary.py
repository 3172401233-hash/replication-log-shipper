"""Primary-side physical replication log shipper.

The central correctness rule implemented here:

    The synchronous-commit (safe) LSN may only advance to the end LSN of a
    *fully committed* transaction whose WAL has been made durable on at least
    ``required_syncs`` synchronous replicas.  It is bounded by what actually
    exists on the current timeline/epoch, never jumps over an in-flight
    transaction, and never moves backwards.

ACKs carry a ``(timeline_id, epoch)`` term.  Any ACK from an older term is
quarantined: counted nowhere and unable to influence the safe LSN of the
current primary.
"""

from __future__ import annotations

from dataclasses import dataclass

from .records import TransactionTracker, WalRecord
from .protocol import Ack, Frame
from .lsn import SEGMENT_SIZE
from .slot import MemoryWalStore, ReplicationSlot, SlotStore, WalStore


class WALUnavailableError(RuntimeError):
    """Raised when a slot needs WAL that has already been recycled."""


@dataclass
class ReplicaState:
    replica_id: str
    slot: ReplicationSlot
    sync: bool = True
    # Highest LSN shipped to this replica on the current term.
    shipped_lsn: int = 0
    # Highest LSN acknowledged durable on the current term.
    durable_lsn: int = 0
    # Highest ACK sequence number applied (duplicate/reorder guard).
    last_ack_seqno: int = -1


@dataclass
class AckResult:
    """Outcome of feeding one ACK to the primary."""

    accepted: bool
    reason: str
    safe_lsn: int
    replica: str
    durable_lsn: int = 0


@dataclass
class QuarantineEntry:
    ack: Ack
    reason: str


class PrimaryNode:
    def __init__(
        self,
        node_id: str,
        timeline_id: int = 1,
        epoch: int = 1,
        required_syncs: int = 1,
        wal_store: WalStore | MemoryWalStore | None = None,
        slot_store: SlotStore | None = None,
        safe_lsn: int = 0,
        flush_lsn: int | None = None,
    ) -> None:
        if required_syncs < 1:
            raise ValueError("required_syncs must be >= 1")
        self.node_id = node_id
        self.timeline_id = timeline_id
        self.epoch = epoch
        self.required_syncs = required_syncs
        self.wal = wal_store if wal_store is not None else MemoryWalStore()
        self.slot_store = slot_store
        self.safe_lsn = safe_lsn
        self.flush_lsn = safe_lsn if flush_lsn is None else flush_lsn
        if self.flush_lsn < self.safe_lsn:
            raise ValueError("flush_lsn cannot precede safe_lsn")
        self._next_seqno = 0
        self.tracker = TransactionTracker()
        self._replicas: dict[str, ReplicaState] = {}
        self.quarantine: list[QuarantineEntry] = []

    # ------------------------------------------------------------------ setup

    def add_replica(
        self,
        replica_id: str,
        slot_name: str,
        sync: bool = True,
        restart_lsn: int = 0,
    ) -> ReplicaState:
        if replica_id in self._replicas:
            raise ValueError(f"replica {replica_id!r} already registered")
        if self.slot_store is not None:
            slot = self.slot_store.get_or_create(slot_name, replica_id)
            slot.advance(restart_lsn)
            self.slot_store.persist()
        else:
            slot = ReplicationSlot(
                slot_name=slot_name,
                replica_id=replica_id,
                restart_lsn=restart_lsn,
            )
        state = ReplicaState(
            replica_id=replica_id, slot=slot, sync=sync
        )
        self._replicas[replica_id] = state
        return state

    def set_sync_replicas(self, replica_ids: set[str]) -> None:
        """Reconfigure the synchronous standby set.

        Replicas leaving the set stop counting towards quorum; the safe LSN
        is recomputed and can therefore move *forward only or stay put*.
        """
        unknown = replica_ids - set(self._replicas)
        if unknown:
            raise ValueError(f"unknown replicas: {sorted(unknown)}")
        for replica_id, state in self._replicas.items():
            state.sync = replica_id in replica_ids
        self._recompute_safe_lsn()

    @property
    def replicas(self) -> tuple[ReplicaState, ...]:
        return tuple(self._replicas.values())

    def replica(self, replica_id: str) -> ReplicaState:
        return self._replicas[replica_id]

    # ------------------------------------------------------------------- append

    def append(self, record: WalRecord) -> Frame:
        """Persist one locally generated WAL record and return its frame."""
        if record.lsn != self.flush_lsn:
            raise ValueError(
                f"non-contiguous record at LSN {record.lsn}, "
                f"expected {self.flush_lsn}"
            )
        frame = Frame(
            timeline_id=self.timeline_id,
            epoch=self.epoch,
            seqno=self._next_seqno,
            record=record,
        )
        self._next_seqno += 1
        self.wal.append_frame(frame)
        self.flush_lsn = record.end_lsn
        self.tracker.handle(record)
        return frame

    # ------------------------------------------------------------------ shipping

    def frames_for(self, replica_id: str, max_bytes: int | None = None) -> list[Frame]:
        """Return pending frames for a replica starting at its slot LSN.

        Delivery is gap-free: frames are returned in LSN order and the slot
        restart LSN is only advanced once the corresponding ACK arrives.
        Re-requesting after a crash therefore resends from the same boundary,
        and duplicate frames are harmless.
        """
        state = self._replicas[replica_id]
        start_lsn = state.slot.restart_lsn
        if start_lsn >= self.flush_lsn:
            return []
        frames: list[Frame] = []
        budget = max_bytes
        seg_no = start_lsn // SEGMENT_SIZE
        end_seg = self.flush_lsn // SEGMENT_SIZE
        while seg_no <= end_seg:
            if not self.wal.has_segment(self.timeline_id, seg_no):
                raise WALUnavailableError(
                    f"segment {seg_no} required by slot "
                    f"{state.slot.slot_name!r} has been recycled"
                )
            for frame in self.wal.load_segment_frames(self.timeline_id, seg_no):
                if frame.record.end_lsn <= start_lsn:
                    continue
                if budget is not None:
                    frame_cost = len(frame.record.payload) + 64
                    # Always allow the first frame of a call so a budget
                    # smaller than one frame cannot stall the pipeline.
                    if frames and budget < frame_cost:
                        state.shipped_lsn = max(
                            state.shipped_lsn,
                            frames[-1].record.end_lsn,
                        )
                        return frames
                    budget -= frame_cost
                frames.append(frame)
            seg_no += 1
        if frames:
            state.shipped_lsn = max(state.shipped_lsn, frames[-1].record.end_lsn)
        return frames

    # --------------------------------------------------------------------- ACKs

    def receive_ack(self, ack: Ack) -> AckResult:
        state = self._replicas.get(ack.replica_id)
        if state is None:
            self.quarantine.append(
                QuarantineEntry(ack=ack, reason="unknown_replica")
            )
            return AckResult(
                accepted=False,
                reason="unknown_replica",
                safe_lsn=self.safe_lsn,
                replica=ack.replica_id,
            )

        # Term isolation: old timeline or old epoch ACKs belong to a previous
        # primary incarnation and must never touch current-term state.
        if (ack.timeline_id, ack.epoch) < (self.timeline_id, self.epoch):
            self.quarantine.append(
                QuarantineEntry(ack=ack, reason="stale_term")
            )
            return AckResult(
                accepted=False,
                reason="stale_term",
                safe_lsn=self.safe_lsn,
                replica=ack.replica_id,
            )
        if (ack.timeline_id, ack.epoch) > (self.timeline_id, self.epoch):
            self.quarantine.append(
                QuarantineEntry(ack=ack, reason="future_term")
            )
            return AckResult(
                accepted=False,
                reason="future_term",
                safe_lsn=self.safe_lsn,
                replica=ack.replica_id,
            )

        # A replica cannot durably confirm WAL it was never sent, nor WAL
        # that does not exist locally on this term.  Checked before the
        # sequence guard so a replayed ACK cannot smuggle an overclaim.
        if ack.durable_lsn > state.shipped_lsn or ack.durable_lsn > self.flush_lsn:
            self.quarantine.append(
                QuarantineEntry(ack=ack, reason="overclaim")
            )
            return AckResult(
                accepted=False,
                reason="overclaim",
                safe_lsn=self.safe_lsn,
                replica=ack.replica_id,
            )

        # Replayed/duplicate ACK: sequence numbers are per replica and
        # monotonic.  Its information is already applied (a retransmitted
        # ACK may carry the same durable LSN, never a larger valid one).
        if ack.seqno <= state.last_ack_seqno:
            return AckResult(
                accepted=True,
                reason="duplicate",
                safe_lsn=self.safe_lsn,
                replica=ack.replica_id,
                durable_lsn=state.durable_lsn,
            )

        state.last_ack_seqno = ack.seqno
        if ack.durable_lsn > state.durable_lsn:
            state.durable_lsn = ack.durable_lsn
        state.slot.advance(ack.durable_lsn)
        if self.slot_store is not None:
            self.slot_store.persist()
        self._recompute_safe_lsn()
        return AckResult(
            accepted=True,
            reason="applied",
            safe_lsn=self.safe_lsn,
            replica=ack.replica_id,
            durable_lsn=state.durable_lsn,
        )

    # ------------------------------------------------------------- safe LSN core

    def _sync_durable_horizon(self) -> int:
        """Minimum durable LSN across the synchronous standbys.

        Only *current-term* durability counts; replica state is reset on
        promotion, so an un-ACKed new-term replica contributes zero.
        """
        durable = [
            state.durable_lsn
            for state in self._replicas.values()
            if state.sync
        ]
        if len(durable) < self.required_syncs:
            return 0
        durable.sort(reverse=True)
        # Best ``required_syncs`` replicas form the quorum; their minimum is
        # the LSN guaranteed durable on enough synchronous standbys.
        return min(durable[: self.required_syncs])

    def _recompute_safe_lsn(self) -> int:
        horizon = min(self._sync_durable_horizon(), self.flush_lsn)
        candidate = self.tracker.committed_prefix_end(horizon)
        if candidate > self.safe_lsn:
            self.safe_lsn = candidate
        return self.safe_lsn

    # ------------------------------------------------------------- WAL retention

    def recycle_wal(self) -> list[str]:
        """Drop segments no slot needs and older than the safe LSN.

        A segment is recyclable only when every slot has advanced past it
        *and* it lies below the current safe LSN.
        """
        slot_floor = (
            self.slot_store.min_restart_lsn()
            if self.slot_store is not None
            else min(
                (s.slot.restart_lsn for s in self._replicas.values()),
                default=0,
            )
        )
        floor = min(slot_floor, self.safe_lsn)
        return self.wal.recycle_before(floor)

    # ---------------------------------------------------------------- promotion

    def promote(
        self,
        new_timeline_id: int,
        new_epoch: int,
        safe_lsn: int | None = None,
    ) -> None:
        """Switch this node to primary on a new timeline/epoch term.

        ``safe_lsn`` is the committed LSN recovered from the old primary's
        durable state (the divergence point).  WAL above it belonged to
        transactions that never completed quorum durability and is discarded.
        All replica current-term progress is reset so that stale ACKs from
        the old term can never be counted here.
        """
        if new_timeline_id <= self.timeline_id:
            raise ValueError("promotion requires a greater timeline_id")
        if new_epoch <= self.epoch:
            raise ValueError("promotion requires a greater epoch")
        recovered = self.safe_lsn if safe_lsn is None else safe_lsn
        if recovered > self.flush_lsn:
            raise ValueError("cannot recover above locally flushed LSN")
        old_timeline_id = self.timeline_id
        migrated: list[Frame] = []
        for seg_no in self.wal.list_segments(old_timeline_id):
            for frame in self.wal.load_segment_frames(
                old_timeline_id, seg_no
            ):
                if frame.record.lsn < recovered:
                    migrated.append(
                        Frame(
                            timeline_id=new_timeline_id,
                            epoch=new_epoch,
                            seqno=len(migrated),
                            record=frame.record,
                        )
                    )
        self.timeline_id = new_timeline_id
        self.epoch = new_epoch
        self.flush_lsn = recovered
        self.safe_lsn = recovered
        self.tracker.truncate_before(recovered)
        # Re-tag retained WAL onto the new term so it remains shippable after
        # the timeline switch; everything at/above the divergence point is
        # discarded by stopping replay at ``recovered``.
        for frame in migrated:
            self.wal.append_frame(frame)
        self._next_seqno = len(migrated)
        for state in self._replicas.values():
            state.shipped_lsn = 0
            state.durable_lsn = 0
            state.last_ack_seqno = -1
            # Keep the slot restart LSN: it still bounds required retention,
            # but re-shipping starts no earlier than the divergence point.
            if state.slot.restart_lsn > recovered:
                state.slot.restart_lsn = recovered

    # ------------------------------------------------------------- introspection

    def quarantine_count(self, reason: str | None = None) -> int:
        if reason is None:
            return len(self.quarantine)
        return sum(1 for entry in self.quarantine if entry.reason == reason)

    def latest_committed_lsn(self) -> int:
        committed = self.tracker.committed_end_lsns
        return committed[-1] if committed else 0
