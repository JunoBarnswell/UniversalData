import pathlib, subprocess, sys, tempfile
with tempfile.TemporaryDirectory(prefix="ud-core-runtime-") as folder:
    subprocess.run([sys.argv[1], folder], check=True, timeout=60)
    assert not list(pathlib.Path(folder).glob("*.udw"))
