import hashlib
import json
import struct
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-history-") as temporary:
    root = pathlib.Path(temporary) / "catalog"
    subprocess.run([sys.argv[1], str(root)], check=True)
    files = list(root.glob("*.udc"))
    assert len(list(root.glob("catalog-5-*.udc"))) == 8
    assert len(list(root.glob("catalog-6-*.udc"))) >= 8
    assert all(b"FixtureSensitive" not in file.read_bytes() for file in files)
    expected_hash = hashlib.sha256(b'{"secret":"FixtureSensitive"}').hexdigest()
    for file in root.glob("catalog-5-*.udc"):
        data = file.read_bytes()
        assert hashlib.sha256(data[:-32]).digest() == data[-32:]
        cursor = 16
        for _ in range(2):
            length = struct.unpack_from("<Q", data, cursor)[0]
            cursor += 8 + length
        cursor += 8
        length = struct.unpack_from("<Q", data, cursor)[0]
        record = json.loads(data[cursor + 8:cursor + 8 + length])
        assert record["parametersHash"] == expected_hash
        assert len(record["definitionHash"]) == 64
        assert record["projectId"] == "project"
        assert record["deployment"]["package"]["revision"] == 2
        assert record["deployment"]["environment"]["revision"] == 3

    print("Catalog history: Ready/terminal immutable snapshots, success/failure/cancel/CommitUnknown states, native metrics/fingerprints, Running BUSY, tenant checks, event sequence, reopen and no plaintext parameter persistence passed")
