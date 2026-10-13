import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-transaction-facts-") as root:
    for outcome in ("commit", "rollback"):
        directory = pathlib.Path(root, outcome)
        directory.mkdir()
        phases = (("stage", 86), ("publish", 86), ("complete", 0)) if outcome == "commit" else (("stage", 86), ("discard", 0))
        for phase, expected in phases:
            actual = subprocess.run([sys.argv[1], str(directory), phase], timeout=30).returncode
            if actual != expected:
                raise SystemExit(f"{outcome}/{phase}: expected {expected}, got {actual}")
