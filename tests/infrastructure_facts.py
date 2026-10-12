import pathlib
import subprocess
import sys
import tempfile
with tempfile.TemporaryDirectory(prefix="ud-facts-") as directory:
    result = subprocess.run([sys.argv[1], "facts", str(pathlib.Path(directory) / "state")],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, (result.stdout, result.stderr)
    print(result.stdout.strip())
