"""One isolated local store per run; actual execution is reserved for final acceptance."""
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix='ud-quarantine-') as folder:
    subprocess.run([str(pathlib.Path(sys.argv[1]).resolve()), folder], check=True)
