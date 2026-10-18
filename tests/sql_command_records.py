"""Real SQLite row transactions, Native error identities and killed-process recovery."""
import copy
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile

runner, plugin = (str(pathlib.Path(value).resolve()) for value in sys.argv[1:])
with tempfile.TemporaryDirectory(prefix="ud-row-command-") as directory:
    root = pathlib.Path(directory)

    def fixture(name, policy="redirect", workers=1):
        folder = root / name
        folder.mkdir()
        database = folder / "business.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE commands(id INTEGER PRIMARY KEY, mirrored INTEGER NOT NULL CHECK(id=mirrored))")
        source = folder / "source.jsonl"
        source.write_text(''.join(json.dumps({"id": value}) + '\n' for value in [1, 2, 2, 3]))
        package = {"version": 12, "id": "row-command", "connections": [
            {"id": "source", "driver": "file", "resource": str(source)},
            {"id": "db", "driver": plugin, "resource": str(database), "credentialReference": "fixture"},
            {"id": "good", "driver": "file", "resource": str(folder / "good.jsonl")},
            {"id": "bad", "driver": "file", "resource": str(folder / "bad.jsonl")}],
            "controlFlow": {"tasks": [{"id": "run", "kind": "dataFlow", "config": {"pipeline": "rows"}}], "constraints": []},
            "dataFlows": [{"id": "rows", "components": [
                {"id": "source", "component": "file.ndjson.source", "config": {"connection": "source", "batchSize": 2,
                    "schema": {"version": 12, "fields": [{"name": "id", "type": "int64", "nullable": False}]}}},
                {"id": "command", "component": "sql.command", "config": {"connection": "db",
                    "statement": "INSERT INTO commands(id,mirrored) VALUES(?1,?2)", "parameters": [0, 0], "errorPolicy": policy, "includeInput": True}},
                {"id": "accept", "component": "file.ndjson.destination", "config": {"connection": "good"}},
                {"id": "reject", "component": "file.ndjson.destination", "config": {"connection": "bad"}}],
                "paths": [{"from": "source", "output": 0, "to": "command", "input": 0},
                    {"from": "command", "output": 0, "to": "accept", "input": 0},
                    {"from": "command", "output": 1, "to": "reject", "input": 0}]}]}
        (folder / "package.json").write_text(json.dumps(package))
        return folder, package, workers

    def run(item, *, resume=False, mode=0, result=0, exit_code=0, durable=True):
        folder, _, workers = item
        completed = subprocess.run([runner, str(folder / "package.json"), str(folder / "business.sqlite3"),
            str(folder / "state") if durable else "", str(int(resume)), str(mode), str(result), str(workers)], capture_output=True, text=True, timeout=45)
        assert completed.returncode == exit_code, (folder.name, completed.returncode, completed.stdout, completed.stderr)
        return json.loads(completed.stdout) if completed.stdout else None

    def business(item):
        with sqlite3.connect(item[0] / "business.sqlite3") as connection:
            return connection.execute("SELECT id,mirrored FROM commands ORDER BY id").fetchall()

    def verify(item, policy="redirect"):
        folder = item[0]
        assert business(item) == [(1, 1), (2, 2), (3, 3)]
        accepted = [json.loads(line) for line in (folder / "good.jsonl").read_text().splitlines()]
        assert accepted == [{"id": 1}, {"id": 2}, {"id": 3}], accepted
        errors = [json.loads(line) for line in (folder / "bad.jsonl").read_text().splitlines()]
        if policy == "redirect":
            assert len(errors) == 1, errors
            error = errors[0]
            assert error["componentId"] == "command" and error["sourceRowId"] and error["errorCode"] == 14, error
            assert error["providerCode"].startswith("SQLITE_") and error["input"], error
        else:
            assert not errors
        return errors

    for workers in (1, 4):
        for policy in ("redirect", "ignore"):
            item = fixture(f"{policy}-{workers}", policy, workers)
            run(item)
            errors = verify(item, policy)
            before = business(item)
            run(item, resume=True)
            assert business(item) == before and verify(item, policy) == errors
    item = fixture("no-journal")
    run(item, durable=False)
    verify(item)
    item = fixture("fail", "fail")
    run(item, result=14)
    assert business(item) == [(1, 1), (2, 2)]
    item = fixture("known-rollback-explicit-resume", "fail")
    folder = item[0]
    (folder / "source.jsonl").write_text(''.join(json.dumps({"id": value}) + '\n' for value in [1, 2, 3]))
    with sqlite3.connect(folder / "business.sqlite3") as connection:
        connection.execute("CREATE TABLE permit(id INTEGER PRIMARY KEY)")
        connection.executemany("INSERT INTO permit VALUES(?)", [(1,), (3,)])
        connection.execute("CREATE TRIGGER check_permit BEFORE INSERT ON commands WHEN NOT EXISTS(SELECT 1 FROM permit WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'record rejected'); END")
    run(item, result=14)
    assert business(item) == [(1, 1)]
    with sqlite3.connect(folder / "business.sqlite3") as connection:
        connection.execute("INSERT INTO permit VALUES(2)")
    run(item, resume=True)
    verify(item, "fail")
    for mode in (2, 3, 4):
        item = fixture(f"known-result-kill-{mode}", workers=4)
        run(item, mode=mode, exit_code=91)
        run(item, resume=True)
        errors = verify(item)
        run(item, resume=True)
        assert verify(item) == errors
    item = fixture("commit-before-journal-kill")
    run(item, mode=1, exit_code=91)
    assert business(item) == [(1, 1)]
    run(item, resume=True, result=16)
    assert business(item) == [(1, 1)]
    run(item, resume=True, result=16)
    assert business(item) == [(1, 1)]
print("SQL row commands: mixed rows, repeated parameters, private transactions, error identities, actual process kills and unknown-commit fencing passed")
