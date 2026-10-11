import csv
import pathlib
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory() as temp:
    root = pathlib.Path(temp)
    source, output, spill = root / "input.csv", root / "output.csv", root / "spill"
    spill.mkdir()
    rows = [(f"row{i}", "\\N" if i % 7 == 0 else str(i % 11)) for i in range(99, -1, -1)]
    with source.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("team", "amount"))
        writer.writerows(rows)
    result = subprocess.run([sys.argv[1], str(source), str(output), str(spill)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with output.open(encoding="utf-8", newline="") as file:
        actual = list(csv.reader(file))[1:]
    expected = sorted(rows, key=lambda row: (row[1] != "\\N", 0 if row[1] == "\\N" else int(row[1])))
    assert actual == [list(row) for row in expected]
    assert not list(spill.glob("*.csv"))
    result = subprocess.run([sys.argv[1], str(source), str(output), str(spill)], capture_output=True, timeout=10)
    assert result.returncode == 2  # existing output is never overwritten
print("external sort: multi-pass merge, stable equal keys, null order, no overwrite passed")
