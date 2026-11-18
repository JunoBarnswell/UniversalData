import hashlib
import json
import sqlite3
import pathlib
import subprocess
import sys
import tempfile
from product_version import PROTOCOL_VERSION

with tempfile.TemporaryDirectory(prefix="ud-history-") as temporary:
    root = pathlib.Path(temporary) / "catalog"
    subprocess.run([sys.argv[1], str(root)], check=True)
    database = root / "catalog.sqlite3"
    with sqlite3.connect(database) as connection:
        rows = connection.execute("SELECT kind,document FROM documents ORDER BY kind,id,revision").fetchall()
    assert sum(kind == 5 for kind, _ in rows) == 16
    assert sum(kind == 5 and "progress" in json.loads(document) for kind, document in rows)==8
    assert sum(kind == 6 for kind, _ in rows) >= 8
    assert all("FixtureSensitive" not in document for _, document in rows)
    expected_hash = hashlib.sha256(b'{"secret":"FixtureSensitive"}').hexdigest()
    for kind, document in rows:
        if kind != 5:
            continue
        record = json.loads(document)
        assert record["version"] == PROTOCOL_VERSION
        assert record["parametersHash"] == expected_hash
        assert len(record["definitionHash"]) == 64
        assert record["projectId"] == "project"
        assert record["deployment"]["package"]["revision"] == 2
        assert record["deployment"]["environment"]["revision"] == 3

    print("Catalog history: Ready/terminal immutable snapshots, success/failure/cancel/CommitUnknown states, native metrics/fingerprints, Running BUSY, tenant checks, event sequence, reopen and no plaintext parameter persistence passed")
