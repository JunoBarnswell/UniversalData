"""Exercise the actual native indexed Catalog across processes and crashes."""
import pathlib
import sqlite3
import subprocess
import sys
import tempfile


def run(folder, mode):
    subprocess.run([sys.argv[1], str(folder), mode], check=True)


with tempfile.TemporaryDirectory(prefix="ud-catalog-") as temporary:
    root = pathlib.Path(temporary) / "store"
    root.mkdir()
    legacy = root / "preserved-old-catalog.udc"
    legacy.write_bytes(b"original ABI 9 artifact; never migrated or deleted")
    run(root, "create")
    run(root, "read")
    run(root, "oldVersion")
    database = root / "catalog.sqlite3"
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == (10 << 16) | 2
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        original = connection.execute("SELECT digest FROM documents WHERE tenant='tenant-a' AND kind=2 AND id='test' AND revision='00000000000000000001'").fetchone()[0]
        connection.execute("UPDATE documents SET digest=zeroblob(32) WHERE tenant='tenant-a' AND kind=2 AND id='test' AND revision='00000000000000000001'")
    run(root, "corrupt")
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE documents SET digest=? WHERE tenant='tenant-a' AND kind=2 AND id='test' AND revision='00000000000000000001'", (original,))
        connection.execute("UPDATE documents SET id='tampered' WHERE tenant='tenant-a' AND kind=2 AND id='test' AND revision='00000000000000000001'")
    run(root, "renamed")
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE documents SET id='test' WHERE tenant='tenant-a' AND kind=2 AND id='tampered' AND revision='00000000000000000001'")
    run(root, "read")
    # Independent native handles/processes write the same immutable revisions.
    workers = [subprocess.Popen([sys.argv[1], str(root), "race"]) for _ in range(2)]
    assert all(worker.wait(timeout=30) == 0 for worker in workers)
    run(root, "raceRead")
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM documents WHERE tenant='tenant-a' AND kind=7 AND id='race'").fetchone()[0] == 100
        connection.execute("BEGIN IMMEDIATE")
        run(root, "busy")
        connection.rollback()
    # A killed process must not publish its uncommitted Catalog document.
    crashed = subprocess.Popen([sys.executable, "-c", "import sqlite3,sys,time; c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); c.execute(\"UPDATE documents SET document='{\\\"uncommitted\\\":true}' WHERE tenant='tenant-a' AND kind=2\"); print('staged',flush=True); time.sleep(30)", str(database)], stdout=subprocess.PIPE, text=True)
    try:
        assert crashed.stdout.readline().strip() == "staged"
        crashed.kill()
        crashed.wait(timeout=10)
    finally:
        if crashed.poll() is None:
            crashed.kill()
            crashed.wait(timeout=10)
    run(root, "read")
    assert legacy.read_bytes() == b"original ABI 9 artifact; never migrated or deleted"
    assert list(root.glob("*.udc")) == [legacy]
    print("Native SQLite Catalog: immutable revisions, bounded pagination, tenant isolation, revocation, protocol refusal, digest/key corruption, concurrent native processes, busy handling, killed-writer rollback and legacy artifact preservation passed")
