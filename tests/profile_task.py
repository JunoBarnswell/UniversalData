import copy
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-profile-task-") as temporary:
    root = pathlib.Path(temporary)
    lit = lambda value, kind="int64": {"literal": value, "type": kind}
    expr = lambda op, *args: {"op": op, "args": list(args)}
    ref = lambda name: {"ref": name}
    def fixture(name, rows=None):
        folder = root / name
        folder.mkdir()
        records = rows if rows is not None else [{"x": 5, "t": "a"}, {"x": None, "t": "中文"}, {"x": -1, "t": "z"}, {"x": 9, "t": None}] * 13
        source = folder / "data.ndjson"
        source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in records))
        variables = {name: {"type": "int64", "default": -1} for name in ("rows", "nulls", "min", "max", "failures")}
        variables.update({"textMin": {"type": "utf8", "default": None}, "textMax": {"type": "utf8", "default": None}, "passed": {"type": "bool", "default": False}, "handled": {"type": "bool", "default": False}})
        statistics = [{"kind": "rows", "variable": "rows"}, {"kind": "nulls", "column": 0, "variable": "nulls"}, {"kind": "min", "column": 0, "variable": "min"}, {"kind": "max", "column": 0, "variable": "max"}, {"kind": "min", "column": 1, "variable": "textMin"}, {"kind": "max", "column": 1, "variable": "textMax"}]
        checks = [{"id": "rows", "expression": expr("eq", ref("rows"), lit(len(records)))}, {"id": "range", "expression": expr("ge", ref("min"), lit(-1))}]
        doc = {"version": 3, "id": "profile", "variables": variables,
               "connections": [{"id": "input", "driver": "file", "resource": str(source)}],
               "controlFlow": {"tasks": [{"id": "profile", "kind": "data.profile", "config": {"pipeline": "p", "checks": checks, "passedVariable": "passed", "failedChecksVariable": "failures"}}], "constraints": []},
               "dataFlows": [{"id": "p", "components": [{"id": "source", "component": "file.ndjson.source", "config": {"connection": "input", "schema": {"version": 3, "fields": [{"name": "x", "type": "int64", "nullable": True}, {"name": "t", "type": "utf8", "nullable": True}]}, "batchSize": 3}}, {"id": "stats", "component": "profile.statistics", "config": {"statistics": statistics}}], "paths": [{"from": "source", "output": 0, "to": "stats", "input": 0}]}]}
        expected = {"rows": len(records), "nulls": sum(row["x"] is None for row in records), "min": min((r["x"] for r in records if r["x"] is not None), default=None), "max": max((r["x"] for r in records if r["x"] is not None), default=None), "textMin": min((r["t"] for r in records if r["t"] is not None), default=None), "textMax": max((r["t"] for r in records if r["t"] is not None), default=None), "passed": True, "failures": 0}
        return folder, doc, expected
    def run(folder, doc, expected, status=0, mode=0, checkpoint=False, resume=0, exit=0):
        package = folder / "package.json"
        package.write_text(json.dumps(doc, ensure_ascii=False))
        outcomes = folder / "expected.json"
        outcomes.write_text(json.dumps(expected, ensure_ascii=False))
        result = subprocess.run([sys.argv[1], str(package), str(mode), str(status), str(outcomes), str(folder / "state") if checkpoint else "", str(resume)], capture_output=True, text=True, timeout=10)
        assert result.returncode == exit, (folder.name, result.returncode, result.stderr)
    defaults = {"rows": -1, "nulls": -1, "min": -1, "max": -1, "textMin": None, "textMax": None, "passed": False, "failures": -1}
    folder, doc, expected = fixture("cross-batch")
    run(folder, doc, expected)
    for name, records in (("empty", []), ("all-null", [{"x": None, "t": None}] * 9)):
        folder, doc, expected = fixture(name, records)
        doc["controlFlow"]["tasks"][0]["config"]["checks"][1]["expression"] = expr("isnull", ref("min"))
        run(folder, doc, expected)
    folder, doc, expected = fixture("float", [{"x": 1.5, "t": "a"}, {"x": -2.25, "t": "z"}])
    doc["dataFlows"][0]["components"][0]["config"]["schema"]["fields"][0]["type"] = "float64"
    for name in ("min", "max"):
        doc["variables"][name] = {"type": "float64", "default": None}
    doc["controlFlow"]["tasks"][0]["config"]["checks"][1]["expression"] = expr("ge", ref("min"), lit(-3, "float64"))
    run(folder, doc, expected)
    folder, doc, expected = fixture("failure")
    doc["controlFlow"]["tasks"][0]["config"]["checks"] = [{"id": "min", "expression": expr("gt", ref("min"), lit(0))}, {"id": "nulls", "expression": expr("eq", ref("nulls"), lit(0))}]
    expected.update(passed=False, failures=2)
    doc["controlFlow"]["tasks"].append({"id": "handle", "kind": "set", "config": {"name": "handled", "value": lit(True, "bool")}})
    doc["controlFlow"]["constraints"] = [{"from": "profile", "to": "handle", "on": "failure"}]
    expected["handled"] = True
    run(folder, doc, expected, status=14)
    folder, doc, expected = fixture("null-check", [])
    expected.update(passed=False, failures=1)
    run(folder, doc, expected, status=14)
    for name, mode, status in (("denied", 1, 13), ("live-cancel", 2, 8), ("deadline", 5, 8), ("revoked", 7, 13)):
        folder, doc, expected = fixture(name)
        run(folder, doc, defaults, status=status, mode=mode)
    folder, doc, expected = fixture("row-budget")
    doc["dataFlows"][0]["components"][1]["config"]["maximumRows"] = 4
    run(folder, doc, defaults, status=4)
    folder, doc, expected = fixture("memory-budget", [{"x": 1, "t": "x" * 100000}])
    run(folder, doc, {}, status=4, mode=6)
    for name, change, status in (("wrong-variable-type", "type", 5), ("readonly", "readonly", 13), ("duplicate-output", "duplicate", 5), ("bad-column", "column", 5), ("unknown-statistic", "kind", 12), ("empty-checks", "empty", 5), ("bad-rule-type", "rule", 5), ("duplicate-rule", "duplicate-rule", 5), ("side-effect-destination", "sink", 5), ("result-collision", "collision", 5)):
        folder, doc, expected = fixture(name)
        sink = doc["dataFlows"][0]["components"][1]["config"]["statistics"]
        task = doc["controlFlow"]["tasks"][0]["config"]
        if change == "type":
            doc["variables"]["rows"] = {"type": "utf8", "default": ""}
        elif change == "readonly":
            del doc["variables"]["rows"]
            doc["parameters"] = {"rows": {"type": "int64", "default": -1}}
        elif change == "duplicate": sink[1]["variable"] = "rows"
        elif change == "column": sink[1]["column"] = 2
        elif change == "kind": sink[1]["kind"] = "distinct"
        elif change == "empty": task["checks"] = []
        elif change == "rule": task["checks"][0]["expression"] = lit(5)
        elif change == "duplicate-rule": task["checks"].append(copy.deepcopy(task["checks"][0]))
        elif change == "collision": task["failedChecksVariable"] = "rows"
        else:
            doc["dataFlows"][0]["components"][1] = {"id": "stats", "component": "file.ndjson.destination", "config": {"connection": "input"}}
        run(folder, doc, {}, status=status)
    folder, doc, expected = fixture("actual-mid-scan-crash")
    run(folder, doc, {}, mode=4, checkpoint=True, exit=91)
    run(folder, doc, expected, checkpoint=True, resume=1)
    if len(sys.argv) > 2:
        folder, doc, expected = fixture("sqlite-source")
        database = folder / "input.db"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE input(x INTEGER,t TEXT)")
            connection.executemany("INSERT INTO input VALUES (?,?)", [(r["x"], r["t"]) for r in [{"x": 5, "t": "a"}, {"x": None, "t": "中文"}, {"x": -1, "t": "z"}, {"x": 9, "t": None}] * 13])
        doc["connections"] = [{"id": "input", "driver": str(pathlib.Path(sys.argv[2]).resolve()), "resource": str(database), "credentialReference": "source"}]
        source = doc["dataFlows"][0]["components"][0]
        source["component"] = "sql.source"
        source["config"]["sql"] = "SELECT x,t FROM input"
        run(folder, doc, expected)
        run(folder, doc, {}, status=12, checkpoint=True)
        print("Profiling SQL Source: same scoped Connection Manager/resolver, real bounded SQLite cursor and read-only statistics passed; persistent SQL snapshot explicitly Unsupported")
    folder, doc, expected = fixture("after-statistics-crash")
    run(folder, doc, {}, mode=3, checkpoint=True, exit=91)
    run(folder, doc, expected, checkpoint=True, resume=1)
    folder, doc, expected = fixture("source-change")
    run(folder, doc, {}, mode=4, checkpoint=True, exit=91)
    (folder / "data.ndjson").write_text('{"x":1,"t":"changed"}\n')
    run(folder, doc, {}, status=9, checkpoint=True, resume=1)
    folder, doc, expected = fixture("completed-resume")
    run(folder, doc, expected, checkpoint=True)
    run(folder, doc, expected, checkpoint=True, resume=1)
    print("Data Profiling: real cross-batch counts/nulls/int/float/UTF8 extrema, empty/all-null semantics, all rule failures plus failure branch, null check failure, authorization/revocation/live cancellation/deadline, row/memory quotas, pre-I/O validation, actual mid-scan and before-TaskDone crashes/replay, source mismatch and completed resume passed")
