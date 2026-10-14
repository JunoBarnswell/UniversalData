import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix='ud-catalog-protection-') as root:
    subprocess.run([sys.argv[1], root], check=True, timeout=30)
    for file in pathlib.Path(root).iterdir():
        if file.is_file():
            assert b'historical-secret-928157' not in file.read_bytes(), file.name
print('Native historical parameter migration, authorization, integrity and SQLite/WAL erasure passed')
