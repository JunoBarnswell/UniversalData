"""Observe batch ownership, record isolation and column resources through Native."""
import json
import pathlib
import subprocess
import sys
import tempfile


def node(identity, kind, **config):
    return dict(id=identity, component=kind, config=config)


def edge(source, target, port=0):
    return {"from": source, "output": port, "to": target, "input": 0}


with tempfile.TemporaryDirectory(prefix="ud-component-product-") as folder:
    root = pathlib.Path(folder)

    def run(name, fields, rows, components, paths, extra=()):
        source = root / (name + "-source.ndjson")
        source.write_text("".join(json.dumps(row) + "\n" for row in rows))
        connections = [dict(id="source", driver="file", resource=str(source)), *extra]
        for output in ("left", "right", "errors"):
            connections.append(dict(id=output, driver="file", resource=str(root / (name + "-" + output))))
        document = dict(version=11, id=name, connections=connections,
            controlFlow=dict(tasks=[dict(id="flow", kind="dataFlow", config=dict(pipeline="flow"))], constraints=[]),
            dataFlows=[dict(id="flow", components=[node("source", "file.ndjson.source", connection="source",
                schema=dict(version=11, fields=fields), batchSize=7), *components], paths=paths)])
        package = root / (name + ".json")
        package.write_text(json.dumps(document))
        result = subprocess.run([sys.argv[1], str(package), "0"], capture_output=True, text=True, timeout=40)
        assert result.returncode == 0, (name, result.stdout, result.stderr)
        return {output: [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
                for output in ("left", "right", "errors")
                for path in [root / (name + "-" + output)]}

    integer = dict(name="id", type="int64", nullable=False)
    rows = [dict(id=at) for at in range(67)]
    for kind in ("balanced", "multicast"):
        actual = run(kind, [integer], rows,
            [node("dispatch", kind, outputs=["left", "right"]),
             node("left", "file.ndjson.destination", connection="left"),
             node("right", "file.ndjson.destination", connection="right")],
            [edge("source", "dispatch"), edge("dispatch", "left"), edge("dispatch", "right", 1)])
        if kind == "balanced":
            # The whole seven-row source batch has one owner, including the tail.
            assert actual["left"] == [row for at, row in enumerate(rows) if (at // 7) % 2 == 0]
            assert actual["right"] == [row for at, row in enumerate(rows) if (at // 7) % 2 == 1]
        else:
            assert actual["left"] == rows and actual["right"] == rows

    literal = lambda value: dict(literal=value, type="int64")
    expression = dict(op="mul", args=[dict(column="id"), literal(2)])
    # Valid rows surround an actual overflow in the same source batch.
    for kind in ("derive", "split"):
        config = dict(errorPolicy="redirect", includeInput=True)
        if kind == "derive":
            config["columns"] = [dict(name="doubled", expression=expression)]
            outputs = [edge("transform", "left"), edge("transform", "errors", 1)]
        else:
            config["condition"] = dict(op="gt", args=[expression, literal(0)])
            outputs = [edge("transform", "left"), edge("transform", "right", 1), edge("transform", "errors", 2)]
        actual = run("errors-" + kind, [integer], [dict(id=1), dict(id=9223372036854775807), dict(id=-1)],
            [node("transform", kind, **config),
             node("left", "file.ndjson.destination", connection="left"),
             *([node("right", "file.ndjson.destination", connection="right")] if kind == "split" else []),
             node("errors", "file.ndjson.destination", connection="errors")],
            [edge("source", "transform"), *outputs])
        assert sorted(row["id"] for row in actual["left"] + actual["right"]) == [-1, 1]
        assert len(actual["errors"]) == 1
        error = actual["errors"][0]
        assert error["componentId"] == "transform" and error["sourceRowId"] and error["input"]
        assert error["errorKind"] == "conversion" and error["errorCode"] != 0
        if kind == "derive":
            assert error["columnId"]

    files = root / "columns"
    files.mkdir()
    payload = "column 中文\nsecond line"
    (files / "input.txt").write_text(payload)
    actual = run("column-files", [dict(name="sourcePath", type="utf8", nullable=False),
        dict(name="targetPath", type="utf8", nullable=False)],
        [dict(sourcePath="input.txt", targetPath="output.txt")],
        [node("import", "importColumn", connection="files", pathColumn="sourcePath", valueColumn="body", type="utf8"),
         node("export", "exportColumn", connection="files", pathColumn="targetPath", valueColumn="body"),
         node("left", "file.ndjson.destination", connection="left")],
        [edge("source", "import"), edge("import", "export"), edge("export", "left")],
        [dict(id="files", driver="file", resource=str(files))])
    assert (files / "output.txt").read_text() == payload and actual["left"][0]["body"] == payload

print("Whole-batch balanced dispatch, multicast copies, mixed expression errors and Import/Export Column passed")
