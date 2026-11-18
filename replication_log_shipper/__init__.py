"""Physical replication log-shipping core.

Public API grouped by concern:

* LSN / segment geometry: :mod:`replication_log_shipper.lsn`
* WAL records & transaction boundaries: :mod:`replication_log_shipper.records`
* wire framing, half-packet decode, ACKs: :mod:`replication_log_shipper.protocol`
* slots & durable segment storage: :mod:`replication_log_shipper.slot`
* primary safe-LSN logic: :class:`replication_log_shipper.primary.PrimaryNode`
* replica receiver & promotion: :class:`replication_log_shipper.replica.ReplicaReceiver`
"""

from .lsn import (
    SEGMENT_SIZE,
    format_lsn,
    next_segment_lsn,
    parse_lsn,
    segment_name,
    segment_no,
    segment_offset,
    segment_start_lsn,
)
from .primary import (
    AckResult,
    PrimaryNode,
    QuarantineEntry,
    ReplicaState,
    WALUnavailableError,
)
from .protocol import (
    Ack,
    Frame,
    ProtocolError,
    StreamDecoder,
    decode_ack,
    decode_frame,
    encode_ack,
    encode_frame,
)
from .records import (
    RECORD_ABORT,
    RECORD_BEGIN,
    RECORD_COMMIT,
    RECORD_DATA,
    TransactionTracker,
    WalRecord,
    WalWriter,
)
from .replica import ReceiveResult, ReplicaReceiver
from .slot import (
    MemoryWalStore,
    ReplicationSlot,
    SlotStore,
    WalStore,
)

__all__ = [
    "SEGMENT_SIZE",
    "Ack",
    "AckResult",
    "Frame",
    "MemoryWalStore",
    "PrimaryNode",
    "ProtocolError",
    "QuarantineEntry",
    "RECORD_ABORT",
    "RECORD_BEGIN",
    "RECORD_COMMIT",
    "RECORD_DATA",
    "ReceiveResult",
    "ReplicaReceiver",
    "ReplicaState",
    "ReplicationSlot",
    "SlotStore",
    "StreamDecoder",
    "WALUnavailableError",
    "WalRecord",
    "WalStore",
    "WalWriter",
    "TransactionTracker",
    "decode_ack",
    "decode_frame",
    "encode_ack",
    "encode_frame",
    "format_lsn",
    "next_segment_lsn",
    "parse_lsn",
    "segment_name",
    "segment_no",
    "segment_offset",
    "segment_start_lsn",
]
