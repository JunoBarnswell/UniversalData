"""Real OS process termination at a Native committed epoch, then same-ID recovery."""
import json
import pathlib
import subprocess
import sys
import tempfile
import time

with tempfile.TemporaryDirectory(prefix='ud-managed-recovery-') as directory:
    root = pathlib.Path(directory)
    command = [sys.argv[1], sys.argv[2], sys.argv[3], directory]
    window = len(sys.argv)>4 and sys.argv[4].startswith('window')
    suffix=['window'] if window else []
    if len(sys.argv)>4 and sys.argv[4]=='window-pre':(root/'pre-barrier').touch()
    child = subprocess.Popen(command + ['new']+suffix, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 25
        while not (root / 'kill-ready').exists():
            if child.poll() is not None:
                raise AssertionError(child.communicate())
            if time.monotonic() > deadline:
                raise AssertionError('Managed epoch did not reach the real kill boundary')
            time.sleep(0.02)
        original = (root / 'identity').read_text()
        before = json.loads((root / 'rows.json').read_text()) if (root/'rows.json').exists() else {}
        assert not before if window else len(before) == 3 and sorted(before.values()) == [0, 1, 2]
        child.kill()
        child.communicate(timeout=10)
        (root / 'continue').write_text('resume')
        resumed = subprocess.run(command + ['resume']+suffix, capture_output=True, text=True, timeout=30)
        assert resumed.returncode == 0, (resumed.stdout, resumed.stderr)
        assert resumed.stdout.strip() == original
        rows = json.loads((root / 'rows.json').read_text())
        assert len(rows) == 12 and sorted(rows.values()) == list(range(12))
        assert {key: rows[key] for key in before} == before
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=10)
print('C# Source/Transform/Destination: Native epochs, actual process kill, stable RowIds and same-ID recovery passed')
