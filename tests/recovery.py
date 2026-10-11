"""Real child-process termination; no mocks or alternate recovery implementation."""
import csv
import hashlib
import pathlib
import struct
import subprocess
import sys
import tempfile
import unittest

RUNNER = sys.argv.pop(1)

class Recovery(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.input = self.root / "input"
        self.state = self.root / "state"
        self.input.mkdir()
        self.write("001.csv", 'team,amount\n"中,文\n组",10\na,20\na,5\na,-1\na,\\N\n')

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, content):
        (self.input / name).write_text(content, encoding="utf-8", newline="")

    def run_job(self, phase=0, mode="", expected=0):
        result = subprocess.run([RUNNER, str(self.input), str(self.state), str(phase), mode], capture_output=True, text=True, encoding="utf-8", timeout=20)
        self.assertEqual(expected, result.returncode, result.stderr)
        return result

    def snapshot(self):
        commits = sorted(self.state.glob("*.commit"))
        self.assertTrue(commits)
        commit = commits[-1]
        version, generation, state_hash, csv_hash = struct.unpack("<QQ32s32s", commit.read_bytes())
        self.assertEqual(1, version)
        prefix = self.state / f"generation-{generation:020d}"
        self.assertEqual(state_hash, hashlib.sha256(prefix.with_suffix(".state").read_bytes()).digest())
        self.assertEqual(csv_hash, hashlib.sha256(prefix.with_suffix(".csv").read_bytes()).digest())
        with prefix.with_suffix(".csv").open(encoding="utf-8", newline="") as stream:
            return {r["team"]: (int(r["total"]), int(r["rows"])) for r in csv.DictReader(stream)}

    def test_restarts_and_new_files(self):
        self.run_job()
        self.assertEqual({"中,文\n组": (10, 1), "a": (25, 2)}, self.snapshot())
        before = len(list(self.state.glob("*.commit")))
        self.run_job()
        self.assertEqual(before, len(list(self.state.glob("*.commit"))))
        self.write("002.csv", "team,amount\na,2\n")
        self.run_job()
        self.assertEqual((27, 3), self.snapshot()["a"])

    def test_crash_each_commit_boundary(self):
        for phase in (1, 2, 3, 4):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                self.state = pathlib.Path(tmp) / "state"
                self.run_job(phase=phase, expected=73)
                self.run_job()
                self.assertEqual({"中,文\n组": (10, 1), "a": (25, 2)}, self.snapshot())

    def test_modified_missing_and_replaced_inputs(self):
        self.run_job()
        self.write("001.csv", "team,amount\na,999\n")
        self.run_job(expected=9)

    def test_missing_input(self):
        self.run_job()
        (self.input / "001.csv").unlink()
        self.run_job(expected=3)

    def test_corruption_and_plan_change_fail_closed(self):
        self.run_job()
        self.run_job(mode="changed", expected=10)
        newest = sorted(self.state.glob("*.state"))[-1]
        newest.write_bytes(newest.read_bytes() + b"corrupt")
        self.run_job(expected=9)

    def test_single_writer(self):
        child = subprocess.Popen([RUNNER, str(self.input), str(self.state), "0", "hold"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
        try:
            self.assertEqual("READY\n", child.stdout.readline())
            self.run_job(expected=11)
        finally:
            child.communicate("\n", timeout=10)
        self.assertEqual(0, child.returncode)

    def test_state_cannot_alias_input(self):
        self.state = self.input / ".." / "input"
        self.run_job(expected=2)

    def test_bad_csv_and_overflow(self):
        cases = [('team,amount\na,9223372036854775808\n', 7), ('team,amount\n"broken,1\n', 6), ('team,amount\na,1,2\n', 5), ('wrong,amount\na,1\n', 5), ('team,amount\na,1\na,9223372036854775807\n', 7)]
        for index, (content, status) in enumerate(cases):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as tmp:
                self.state = pathlib.Path(tmp) / "state"
                self.write("001.csv", content)
                self.run_job(expected=status)

    def test_cancel_and_quota(self):
        self.run_job(mode="cancel", expected=8)
        self.write("001.csv", "team,amount\n" + "".join(f"group{i},1\n" for i in range(1024)))
        self.run_job(mode="quota", expected=4)

if __name__ == "__main__":
    unittest.main()
