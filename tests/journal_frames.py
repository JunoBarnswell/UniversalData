"""Inspect intact Native Journal frames in recovery fixtures."""

import hashlib
import json
import struct


def committed_records(state):
    for path in sorted(state.rglob("segment-*.udg")):
        data = path.read_bytes()
        offset = 0
        while offset < len(data):
            length, = struct.unpack_from("<Q", data, offset)
            frame = data[offset + 8:offset + 8 + length]
            assert len(frame) == length and frame[-8:] == b"UCMTEND5", path.name
            assert hashlib.sha256(frame[:-40]).digest() == frame[-40:-8], path.name
            count, = struct.unpack_from("<Q", frame, 48)
            at = 56
            for _ in range(count):
                kind, = struct.unpack_from("<Q", frame, at)
                at += 8
                fields = []
                for _ in range(3):
                    size, = struct.unpack_from("<Q", frame, at)
                    at += 8
                    fields.append(frame[at:at + size])
                    at += size
                if fields[2]:
                    yield kind, fields[0], json.loads(fields[2])
            assert at == len(frame) - 40
            offset += length + 8
