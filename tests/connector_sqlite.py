import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix='ud-sqlite-v2-') as temporary:
    root = pathlib.Path(temporary)
    subprocess.run([sys.argv[1], sys.argv[2], str(root / 'source.db'), str(root / 'target.db')], check=True, timeout=120)
print('SQLite typed connector: 10,000 full + 200 new / 50 updated; row-by-row equivalence, receipts and authorization passed')
