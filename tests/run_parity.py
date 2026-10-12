"""Future C/.NET8/Java17 same-DAG file execution fixture.

Arguments: run_runner, dotnet, RunConsumer.dll, java, classes, jar, native_dir.
This covers file execution; connector/recovery/scheduler/distributed equivalence
and independently installed package consumption are separate mandatory gates.
"""
import json
import os
import pathlib
import subprocess
import sys
import tempfile

runner, dotnet, consumer, java, classes, jar, native_dir = [str(pathlib.Path(p).resolve()) for p in sys.argv[1:]]
env = dict(os.environ)
if os.name == "nt":
    env["PATH"] = native_dir + os.pathsep + env.get("PATH", "")
else:
    env["LD_LIBRARY_PATH"] = native_dir + os.pathsep + env.get("LD_LIBRARY_PATH", "")
schema = {"version": 2, "fields": [
    {"name": "id", "type": "int64", "nullable": False},
    {"name": "amount", "type": "decimal128", "precision": 38, "scale": 2, "nullable": True},
    {"name": "name", "type": "utf8", "nullable": True},
    {"name": "list", "type": "list", "nullable": True, "children": [
        {"name": "item", "type": "int64", "nullable": True}]}]}
renamed = json.loads(json.dumps(schema))
renamed["fields"][2]["name"] = "label"
rows = [{"id": i, "amount": "123456789012345678901234567890123456.78" if i % 3 else None,
         "name": "中文\u0000" + str(i), "list": [i, None] if i % 7 else None} for i in range(10003)]
with tempfile.TemporaryDirectory(prefix="ud-execution-parity-") as folder:
    root = pathlib.Path(folder)
    source = root / "input.ndjson"
    source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
    nodes = [
        {"id": "source", "kind": "source", "operation": "ndjson", "config": {"path": str(source), "schema": schema, "batch_size": 19}},
        {"id": "pass", "kind": "transform", "operation": "pass", "config": {}},
        {"id": "cast", "kind": "transform", "operation": "cast", "config": {"schema": renamed}},
        {"id": "pass2", "kind": "transform", "operation": "pass", "config": {}},
        {"id": "sink0", "kind": "sink", "operation": "ndjson", "config": {"path": {"$parameter": "output0"}}},
        {"id": "sink1", "kind": "sink", "operation": "ndjson", "config": {"path": {"$parameter": "output1"}}}]
    edges = [{"from": a, "to": b, "port": 0} for a, b in [
        ("source", "pass"), ("pass", "sink0"), ("source", "cast"), ("cast", "pass2"), ("pass2", "sink1")]]
    document = {"version": 2, "nodes": nodes, "edges": edges,
                "parameters": {name: {"kind": "string", "required": True} for name in ("output0", "output1")}}
    graph = root / "dag.json"
    graph.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    for index in range(3):
        parameters = {f"output{port}": str(root / f"{index}-{port}.ndjson") for port in range(2)}
        values = root / f"{index}.parameters.json"
        values.write_text(json.dumps(parameters), encoding="utf-8")
        metrics = root / f"{index}.metrics.json"
        if index == 0:
            command = [runner, str(graph), "async", "0", str(len(rows)), json.dumps(parameters)]
        elif index == 1:
            command = [dotnet, consumer, str(graph), str(values), str(metrics), "0"]
        else:
            command = [java, "-Djava.library.path=" + native_dir, "-cp", classes + os.pathsep + jar,
                       "RunConsumer", str(graph), str(values), str(metrics), "0"]
        subprocess.run(command, env=env, check=True, timeout=120)
        if index:
            measured = json.loads(metrics.read_text(encoding="utf-8"))
            assert measured["state"] == 2 and measured["result"] == 0
            assert measured["inputRows"] == len(rows) and measured["outputRows"] == len(rows) * 2
            assert measured["queueHighWater"] <= 10
        for port in range(2):
            actual = (root / f"{index}-{port}.ndjson").read_bytes()
            if index:
                assert actual == (root / f"0-{port}.ndjson").read_bytes()
            expected = [dict(row) for row in rows]
            if port:
                for row in expected:
                    row["label"] = row.pop("name")
            assert [json.loads(line) for line in actual.decode("utf-8").splitlines()] == expected
print("C/.NET8/Java17 same-DAG file execution and metrics fixture passed; full capability parity remains a separate gate")
