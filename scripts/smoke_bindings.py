"""Compare actual .NET and Java consumers against identical committed results."""
import argparse
import csv
import os
import pathlib
import subprocess
import tempfile

p = argparse.ArgumentParser()
p.add_argument("--native-dir", required=True)
p.add_argument("--dotnet-dll", required=True)
p.add_argument("--java-classes", required=True)
p.add_argument("--java-jar", required=True)
a = p.parse_args()
native = str(pathlib.Path(a.native_dir).resolve())
env = os.environ.copy()
env["PATH"] = native + os.pathsep + env.get("PATH", "")
env["LD_LIBRARY_PATH"] = native + os.pathsep + env.get("LD_LIBRARY_PATH", "")
with tempfile.TemporaryDirectory() as temp:
    root = pathlib.Path(temp)
    source = root / "输入"
    source.mkdir()
    (source / "001.csv").write_text('team,amount\n"中,文\n组",10\na,20\na,5\na,-1\na,\\N\n', encoding="utf-8", newline="")
    commands = {
        "dotnet": ["dotnet", str(pathlib.Path(a.dotnet_dll).resolve())],
        "java": ["java", f"-Djava.library.path={native}", "-cp", os.pathsep.join([str(pathlib.Path(a.java_classes).resolve()), str(pathlib.Path(a.java_jar).resolve())]), "Consumer"],
    }
    for language, command in commands.items():
        state = root / language
        for iteration in range(2):
            result = subprocess.run(command + [str(source), str(state)], capture_output=True, text=True, encoding="utf-8", env=env, timeout=30)
            if result.returncode: raise RuntimeError(f"{language}: {result.stderr}")
        snapshots = sorted(state.glob("*.commit"))
        if len(snapshots) != 1: raise AssertionError(f"{language}: unexpected replay commit")
        with snapshots[-1].with_suffix(".csv").open(encoding="utf-8", newline="") as file:
            actual = {r["team"]: (int(r["total"]), int(r["rows"])) for r in csv.DictReader(file)}
        if actual != {"中,文\n组": (10, 1), "a": (25, 2)}: raise AssertionError((language, actual))
        print(f"{language}: Unicode, native ABI, snapshot and restart parity passed")
