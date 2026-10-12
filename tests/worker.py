import pathlib
import struct
import subprocess
import sys

def integer(n): return struct.pack("<Q", n)
def text(s):
    b = s.encode("utf-8")
    return integer(len(b)) + b

payload = integer(2) + integer(1) + text("ud-schema 2\nvalue int64 required\n") + text("ud-plan 2\nsum value total\n")
payload += integer(2) + integer(1) + integer(2) + text("value") + integer(2) + integer(0) + integer(0) + integer(0) + text("") + integer(0)
payload += integer(0) + integer(10) + integer(0) + integer(20)
child = subprocess.run([sys.argv[1]], input=struct.pack("<I", len(payload)) + payload, capture_output=True, timeout=10)
if child.returncode: raise AssertionError((child.returncode, child.stderr))
size, = struct.unpack_from("<I", child.stdout)
body = memoryview(child.stdout)[4:]
assert len(body) == size
status, error_length = struct.unpack_from("<QQ", body)
assert status == 0 and error_length == 0
has_batch, version, columns, rows = struct.unpack_from("<QQQQ", body, 16)
assert (has_batch, version, columns, rows) == (1, 2, 1, 1)
name_length, = struct.unpack_from("<Q", body, 48)
offset = 56 + name_length
kind, nullable, precision, scale, timezone_length, child_count, null, value = struct.unpack_from("<QQQQQQQQ", body, offset)
assert (precision, scale, timezone_length, child_count) == (0, 0, 0, 0)
assert (kind, nullable, null, value) == (2, 1, 0, 30)
invalid = subprocess.run([sys.argv[1]], input=struct.pack("<I", 64*1024*1024+1), capture_output=True, timeout=10)
assert invalid.returncode == 2
print("worker: framed query and oversized input rejection passed")

legacy = integer(1) + integer(1)
child = subprocess.run([sys.argv[1]], input=struct.pack("<I", len(legacy)) + legacy, capture_output=True, timeout=10)
assert child.returncode == 0
assert struct.unpack_from("<Q", child.stdout, 4)[0] == 10
