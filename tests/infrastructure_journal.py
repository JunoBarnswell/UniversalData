"""Run actual native segmented recovery, corruption and retirement acceptance."""
import pathlib
import shutil
import subprocess
import sys
import tempfile
runner = pathlib.Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="ud-journal-") as folder:
    root = pathlib.Path(folder)
    live = root / "live"
    def run(mode, directory):
        result = subprocess.run([runner, mode, directory], capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, (mode, result.stdout, result.stderr)
        return result.stdout.strip()
    print(run("journal", live))
    print(run("journal-read", live))
    assert len(list(live.glob("*.udg"))) <= 1
    assert 1 <= len(list(live.glob("*.uds"))) <= 2
    assert not list(live.glob("*.udj"))
    for scenario in ("tail", "missing-segment", "snapshot", "head", "acknowledgement"):
        target = root / scenario
        shutil.copytree(live, target)
        if scenario == "missing-segment":
            list(target.glob("*.udg"))[-1].unlink()
        else:
            path = (list(target.glob("*.udg"))[-1] if scenario == "tail" else
                    sorted(target.glob("*.uds"))[-1] if scenario == "snapshot" else
                    target / ("journal.head" if scenario == "head" else "journal.tail"))
            data = bytearray(path.read_bytes())
            if scenario == "tail":
                del data[-3:]
            else:
                data[-1] ^= 1
            path.write_bytes(data)
        run("journal-corrupt", target)
    print("Journal corruption: torn frame, missing acknowledged segment, altered snapshot/head/tail fail closed")
