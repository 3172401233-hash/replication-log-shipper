import unittest

from replication_log_shipper import (
    Ack,
    Frame,
    ProtocolError,
    StreamDecoder,
    WalRecord,
    decode_ack,
    decode_frame,
    encode_ack,
    encode_frame,
    RECORD_DATA,
)


def make_frame() -> Frame:
    return Frame(
        timeline_id=2,
        epoch=3,
        seqno=42,
        record=WalRecord(10, 25, RECORD_DATA, 99, b"hello-wal"),
    )


class FrameCodecTests(unittest.TestCase):
    def test_frame_roundtrip(self) -> None:
        frame = make_frame()
        raw = encode_frame(frame)
        payload_len = int.from_bytes(raw[2:6], "big")
        decoded = decode_frame(raw[6:6 + payload_len])
        self.assertEqual(decoded, frame)

    def test_ack_roundtrip(self) -> None:
        ack = Ack(
            timeline_id=1,
            epoch=2,
            replica_id="standby-A",
            durable_lsn=123456,
            seqno=7,
        )
        raw = encode_ack(ack)
        payload_len = int.from_bytes(raw[2:6], "big")
        decoded = decode_ack(raw[6:6 + payload_len])
        self.assertEqual(decoded, ack)


class StreamDecoderTests(unittest.TestCase):
    def test_half_packets_buffered_until_complete(self) -> None:
        decoder = StreamDecoder()
        raw = encode_frame(make_frame())
        messages = []
        for index in range(len(raw)):
            messages.extend(decoder.feed(raw[index:index + 1]))
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0], make_frame())
        self.assertEqual(decoder.buffered, 0)

    def test_coalesced_packets_decode_together(self) -> None:
        decoder = StreamDecoder()
        ack = Ack(1, 1, "r", 8, 1)
        raw = encode_frame(make_frame()) + encode_ack(ack)
        messages = decoder.feed(raw)
        self.assertEqual(len(messages), 2)
        self.assertIsInstance(messages[0], Frame)
        self.assertEqual(messages[1], ack)

    def test_duplicate_frames_decode_idempotently(self) -> None:
        decoder = StreamDecoder()
        raw = encode_frame(make_frame())
        first = decoder.feed(raw)
        second = decoder.feed(raw)
        self.assertEqual(first, second)

    def test_bad_crc_rejected(self) -> None:
        decoder = StreamDecoder()
        raw = bytearray(encode_frame(make_frame()))
        raw[-1] ^= 0xFF
        with self.assertRaises(ProtocolError):
            decoder.feed(bytes(raw))

    def test_bad_magic_rejected(self) -> None:
        decoder = StreamDecoder()
        raw = bytearray(encode_frame(make_frame()))
        raw[0] = 0x00
        with self.assertRaises(ProtocolError):
            decoder.feed(bytes(raw))

    def test_unknown_message_type_rejected(self) -> None:
        decoder = StreamDecoder()
        raw = bytearray(encode_frame(make_frame()))
        raw[1] = 99
        with self.assertRaises(ProtocolError):
            decoder.feed(bytes(raw))


if __name__ == "__main__":
    unittest.main()
