"""Legacy Native state rejects without rewriting the original durable records."""
import pathlib
import subprocess
import sys
import tempfile
with tempfile.TemporaryDirectory(prefix='ud-checkpoint-contract-') as directory:
    subprocess.run([sys.argv[1], directory], check=True)
    assert any(path.is_file() for path in pathlib.Path(directory).iterdir())
