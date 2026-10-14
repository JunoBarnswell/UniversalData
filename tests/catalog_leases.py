"""Actual process death preserves a running record without granting replay authority."""
import pathlib
import subprocess
import sys
import tempfile
import time
with tempfile.TemporaryDirectory(prefix='ud-catalog-leases-') as directory:
    result = subprocess.run([sys.argv[1], directory, 'crash'], timeout=20)
    assert result.returncode == 86, result.returncode
    time.sleep(.4)
    subprocess.run([sys.argv[1], directory, 'recover'], timeout=25, check=True)
    assert (pathlib.Path(directory) / 'catalog.sqlite3').is_file()
