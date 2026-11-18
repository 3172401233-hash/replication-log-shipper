"""Wire protocol: length-prefixed framed WAL records and replica ACKs.

Every message is::

    magic(1) | type(1) | payload_len(4, BE) | payload(payload_len) | crc32(4, BE)

The CRC covers every byte preceding it (magic, type, length, payload).
A TCP/stream transport delivers arbitrary byte chunks, so :class:`StreamDecoder`
buffers partial messages ("half packets") and only emits complete, verified
messages.  Duplicate deliveries are tolerated: decoded messages are
idempotently re-applied by the upper layers.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

from .records import WalRecord

MAGIC = 0x57  # 'W'

MSG_FRAME = 1
MSG_ACK = 2

HEADER_LEN = 6
CRC_LEN = 4

_HEADER = struct.Struct(">BBI")
_CRC = struct.Struct(">I")

# FRAME payload header (the variable-length record payload follows):
#   timeline_id(4) epoch(4) seqno(8) lsn(8) end_lsn(8)
#   record_type(1) xid(8) payload_len(4)
_FRAME_HEAD = struct.Struct(">IIQQQBQI")
#
# ACK payload:
#   timeline_id(4) epoch(4) replica_id_len(2) replica_id(...)
#   durable_lsn(8) seqno(8)


class ProtocolError(ValueError):
    """Raised on framing, CRC or structural protocol violations."""


@dataclass(frozen=True)
class Frame:
    """A WAL record in flight, tagged with timeline/epoch and a sequence no."""

    timeline_id: int
    epoch: int
    seqno: int
    record: WalRecord


@dataclass(frozen=True)
class Ack:
    """Replica acknowledgement of everything durable up to ``durable_lsn``."""

    timeline_id: int
    epoch: int
    replica_id: str
    durable_lsn: int
    seqno: int


def _wrap(msg_type: int, payload: bytes) -> bytes:
    header = _HEADER.pack(MAGIC, msg_type, len(payload))
    crc = _CRC.pack(zlib.crc32(header + payload) & 0xFFFFFFFF)
    return header + payload + crc


def encode_frame(frame: Frame) -> bytes:
    record = frame.record
    payload = _FRAME_HEAD.pack(
        frame.timeline_id,
        frame.epoch,
        frame.seqno,
        record.lsn,
        record.end_lsn,
        record.record_type,
        record.xid,
        len(record.payload),
    ) + bytes(record.payload)
    return _wrap(MSG_FRAME, payload)


def decode_frame(payload: bytes) -> Frame:
    head_size = _FRAME_HEAD.size
    if len(payload) < head_size:
        raise ProtocolError("frame payload too short")
    timeline_id, epoch, seqno, lsn, end_lsn, record_type, xid, payload_len = (
        _FRAME_HEAD.unpack(payload[:head_size])
    )
    if len(payload) != head_size + payload_len:
        raise ProtocolError("frame payload length mismatch")
    body = bytes(payload[head_size:])
    record = WalRecord(
        lsn=lsn,
        end_lsn=end_lsn,
        record_type=record_type,
        xid=xid,
        payload=body,
    )
    return Frame(
        timeline_id=timeline_id, epoch=epoch, seqno=seqno, record=record
    )


def encode_ack(ack: Ack) -> bytes:
    replica = ack.replica_id.encode("utf-8")
    if len(replica) > 0xFFFF:
        raise ProtocolError("replica_id too long")
    payload = (
        struct.pack(">II", ack.timeline_id, ack.epoch)
        + struct.pack(">H", len(replica))
        + replica
        + struct.pack(">QQ", ack.durable_lsn, ack.seqno)
    )
    return _wrap(MSG_ACK, payload)


def decode_ack(payload: bytes) -> Ack:
    # Fixed overhead: timeline(4) + epoch(4) + name_len(2)
    # + durable_lsn(8) + seqno(8) = 26; replica name may be empty.
    if len(payload) < 26:
        raise ProtocolError("ack payload too short")
    timeline_id, epoch = struct.unpack(">II", payload[:8])
    (name_len,) = struct.unpack(">H", payload[8:10])
    # Fixed fields before the name: timeline(4) + epoch(4) + name_len(2)
    # = 10 bytes; trailing durable_lsn(8) + seqno(8) = 16 bytes.
    end_name = 10 + name_len
    if len(payload) != end_name + 16:
        raise ProtocolError("ack payload length mismatch")
    replica_id = payload[10:end_name].decode("utf-8")
    durable_lsn, seqno = struct.unpack(">QQ", payload[end_name:])
    return Ack(
        timeline_id=timeline_id,
        epoch=epoch,
        replica_id=replica_id,
        durable_lsn=durable_lsn,
        seqno=seqno,
    )


class StreamDecoder:
    """Incremental decoder for a byte stream.

    Feed arbitrary chunks via :meth:`feed`; receive back zero or more fully
    decoded :class:`Frame` / :class:`Ack` messages.  Incomplete trailing
    bytes are retained for the next feed, which transparently handles
    half-packet delivery and arbitrary packet coalescing.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    @property
    def buffered(self) -> int:
        return len(self._buf)

    def feed(self, chunk: bytes) -> list[Frame | Ack]:
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("chunk must be bytes")
        self._buf.extend(chunk)
        messages: list[Frame | Ack] = []
        while True:
            if len(self._buf) < HEADER_LEN:
                break
            magic, msg_type, payload_len = _HEADER.unpack(
                bytes(self._buf[:HEADER_LEN])
            )
            if magic != MAGIC:
                raise ProtocolError("bad magic byte; stream desynchronised")
            if msg_type not in (MSG_FRAME, MSG_ACK):
                raise ProtocolError(f"unknown message type {msg_type}")
            total_len = HEADER_LEN + payload_len + CRC_LEN
            if len(self._buf) < total_len:
                break  # half packet: wait for more bytes
            raw = bytes(self._buf[:total_len])
            del self._buf[:total_len]
            payload = raw[HEADER_LEN:HEADER_LEN + payload_len]
            (stored_crc,) = _CRC.unpack(raw[-CRC_LEN:])
            actual_crc = zlib.crc32(raw[:-CRC_LEN]) & 0xFFFFFFFF
            if stored_crc != actual_crc:
                raise ProtocolError("CRC mismatch; corrupt message")
            if msg_type == MSG_FRAME:
                messages.append(decode_frame(payload))
            else:
                messages.append(decode_ack(payload))
        return messages
