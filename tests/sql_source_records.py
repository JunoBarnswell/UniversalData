"""SQL result conversion errors share Native source identities and error ports."""
import base64
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="ud-sql-records-") as directory:
    root = pathlib.Path(directory)
    database = root / "records.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE source(value TEXT, label TEXT)")
        connection.executemany("INSERT INTO source VALUES(?,?)", [("18446744073709551615", "max"), ("-1", "negative"), ("3", "normal"), ("18446744073709551616", "overflow")])
    good, errors = root / "good.jsonl", root / "errors.jsonl"
    schema = {"version": 12, "fields": [{"name": "value", "type": "uint64", "nullable": False}, {"name": "label", "type": "utf8", "nullable": False}]}
    package = {"version": 12, "id": "sql-records", "connections": [
        {"id": "input", "driver": str(pathlib.Path(sys.argv[2]).resolve()), "resource": str(database), "credentialReference": "fixture"},
        {"id": "good", "driver": "file", "resource": str(good)}, {"id": "errors", "driver": "file", "resource": str(errors)}],
        "controlFlow": {"tasks": [{"id": "copy", "kind": "dataFlow", "config": {"pipeline": "rows"}}], "constraints": []},
        "dataFlows": [{"id": "rows", "components": [
            {"id": "read", "component": "sql.source", "config": {"connection": "input", "sql": "SELECT value,label FROM source ORDER BY rowid", "schema": schema, "batchSize": 1, "errorPolicy": "redirect", "includeInput": True}},
            {"id": "write", "component": "file.ndjson.destination", "config": {"connection": "good"}},
            {"id": "reject", "component": "file.ndjson.destination", "config": {"connection": "errors"}}],
            "paths": [{"from": "read", "output": 0, "to": "write", "input": 0}, {"from": "read", "output": 1, "to": "reject", "input": 0}]}]}
    definition = root / "package.json"
    definition.write_text(json.dumps(package))
    credentials = root / "credentials.json"
    credentials.write_text(json.dumps({"fixture": str(database)}))
    result = subprocess.run([sys.argv[1], str(definition), "0", "", "0", str(credentials)], capture_output=True, text=True, timeout=40)
    assert result.returncode == 0, (result.stdout, result.stderr)
    accepted = [json.loads(line) for line in good.read_text().splitlines()]
    rejected = [json.loads(line) for line in errors.read_text().splitlines()]
    assert [int(row["value"]) for row in accepted] == [18446744073709551615, 3]
    assert len(rejected) == 2 and len({row["sourceRowId"] for row in rejected}) == 2
    for row in rejected:
        assert row["componentId"] == "read" and row["columnId"] and row["inputFormat"] == "SQLRawCells/Wire12"
        assert base64.b64decode(row["input"])
