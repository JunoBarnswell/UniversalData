"""Run cursor regressions against an isolated local PostgreSQL cluster."""
import os
import pathlib
import socket
import subprocess
import sys
import tempfile

runner, plugin, initdb, pg_ctl = map(lambda p: str(pathlib.Path(p).resolve()), sys.argv[1:5])
with tempfile.TemporaryDirectory(prefix="ud-pg-cancel-") as folder:
    root = pathlib.Path(folder)
    data = root / "data"
    result = subprocess.run([initdb, "-D", str(data), "-A", "trust", "-U", "fixture", "--no-locale"],
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    if os.name == "nt":
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        options = f"-h 127.0.0.1 -p {port}"
        configuration = f"host=127.0.0.1 port={port} user=fixture dbname=postgres"
    else:
        options = f"-h '' -k '{root}'"
        configuration = f"host='{root}' user=fixture dbname=postgres"
    subprocess.run([pg_ctl, "-D", str(data), "-l", str(root / "server.log"), "-o", options, "-w", "start"],
                   check=True, timeout=30)
    try:
        subprocess.run([runner, plugin, configuration], check=True, timeout=15)
    finally:
        subprocess.run([pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop"],
                       check=True, timeout=15)
