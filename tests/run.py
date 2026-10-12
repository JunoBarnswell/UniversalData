"""Future runtime gate: real DAG file effects, independent runs and failure cleanup."""
import json
import pathlib
import subprocess
import sys
import tempfile

runner = pathlib.Path(sys.argv[1]).resolve()
schema = {"version": 2, "fields": [
    {"name": "id", "type": "int64", "nullable": False},
    {"name": "amount", "type": "decimal128", "precision": 38, "scale": 4, "nullable": True},
    {"name": "name", "type": "utf8", "nullable": True},
    {"name": "children", "type": "list", "nullable": True, "children": [
        {"name": "item", "type": "int64", "nullable": True}]}]}
renamed = json.loads(json.dumps(schema))
renamed["fields"][2]["name"] = "label"
rows = [{"id": i, "amount": "123456789012345678901234567890.1234" if i % 7 else None,
         "name": "中文\u0000" + str(i), "children": [i, None, -i] if i % 3 else None}
        for i in range(10003)]

with tempfile.TemporaryDirectory(prefix="ud-dag-run-") as directory:
    root = pathlib.Path(directory)
    source = root / "source.ndjson"
    source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")

    def graph(prefix):
        targets = [root / (prefix + str(i) + ".ndjson") for i in range(2)]
        nodes = [
            {"id": "source", "kind": "source", "operation": "ndjson", "config": {
                "schema": schema, "path": str(source), "batch_size": 19, "max_record_bytes": 4096}},
            {"id": "pass", "kind": "transform", "operation": "pass", "config": {}},
            {"id": "cast", "kind": "transform", "operation": "cast", "config": {"schema": renamed}},
            {"id": "pass2", "kind": "transform", "operation": "pass", "config": {}},
            {"id": "sink0", "kind": "sink", "operation": "ndjson", "config": {"path": str(targets[0]), "max_record_bytes": 4096}},
            {"id": "sink1", "kind": "sink", "operation": "ndjson", "config": {"path": str(targets[1]), "max_record_bytes": 4096}}]
        edges = [{"from": a, "to": b, "port": 0} for a, b in [
            ("source", "pass"), ("pass", "sink0"), ("source", "cast"), ("cast", "pass2"), ("pass2", "sink1")]]
        document = {"version": 2, "parameters": {}, "nodes": nodes, "edges": edges}
        path = root / (prefix + ".json")
        path.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
        return path, targets

    for mode, expected in [("sync", 0), ("async", 0), ("cancel", 8), ("cancel-async", 8),
                           ("deny", 13), ("revoke", 13), ("quota", 4)]:
        path, targets = graph(mode)
        subprocess.run([str(runner), str(path), mode, str(expected), str(len(rows))], check=True, timeout=120)
        if expected == 0:
            actual = [json.loads(line) for line in targets[0].read_text(encoding="utf-8").splitlines()]
            assert actual == rows
            changed = [dict(row, label=row["name"]) for row in rows]
            for row in changed:
                del row["name"]
            assert [json.loads(line) for line in targets[1].read_text(encoding="utf-8").splitlines()] == changed
            digest = targets[0].read_bytes()
            subprocess.run([str(runner), str(path), mode, "11", str(len(rows))], check=True, timeout=120)
            assert targets[0].read_bytes() == digest
        else:
            assert all(not path.exists() for path in targets)
        assert not list(root.glob("*.ud-run.part"))

    # Two independent execution processes overlap; neither shares a mutable context.
    graphs = [graph("parallel-" + str(i)) for i in range(2)]
    processes = [subprocess.Popen([str(runner), str(path), "async", "0", str(len(rows))]) for path, _ in graphs]
    for process in processes:
        assert process.wait(timeout=120) == 0
    for _, targets in graphs:
        assert [json.loads(line) for line in targets[0].read_text(encoding="utf-8").splitlines()] == rows

print("DAG fan-out, bounded queues, async isolation, cancellation, authorization and publication fixtures passed")
