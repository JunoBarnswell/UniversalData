"""Exercise SQLite through the real Worker, including process death during a batch."""
import os
import pathlib
import subprocess
import sys
import tempfile

dotnet, assembly, worker, plugin = sys.argv[1:]
with tempfile.TemporaryDirectory(prefix="ud-sdk-worker-") as temporary:
    root = pathlib.Path(temporary)
    connection = root / "connection.json"
    connection.write_text(f"Data Source={root / 'data.sqlite'}", encoding="utf-8")
    subprocess.run([dotnet, assembly, "--worker-failure", str(connection), plugin],
                   env={**os.environ, "UD_WORKER_PATH": worker}, check=True, timeout=90)
