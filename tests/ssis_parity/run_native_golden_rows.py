"""Capture row values and types from a Native Derived Column data flow."""
import argparse
import csv
import json
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as parquet


NUMBER_EXPRESSION = "([id] % 7 == 0) ? NULL(DT_I4) : (DT_I4)([amount] + 7)"
TEXT_EXPRESSION = "(DT_WSTR,24)UPPER(TRIM([label]))"
VERSION = json.loads((Path(__file__).resolve().parents[2] / "metadata/product.json")
                     .read_text(encoding="utf-8"))["product"]["protocolVersion"]
SCALAR_TYPES = {4: "DT_I4", 24: "DT_WSTR"}


def field(name, kind, scalar_type, **attributes):
    return dict(name=name, type=kind, nullable=False,
                scalar=dict(type=scalar_type, **attributes))


def node(node_id, component, **config):
    return dict(id=node_id, component=component, config=config)


def document(source, target):
    fields = [
        field("id", "int64", "DT_I4"),
        field("amount", "int64", "DT_I4"),
        field("label", "utf8", "DT_WSTR", length=24),
    ]
    nodes = [
        node("source", "file.ndjson.source", connection="input",
             schema=dict(version=VERSION, fields=fields), batchSize=1024),
        node("derive", "derive", includeInput=True, columns=[
            dict(name="calculated", expression=NUMBER_EXPRESSION),
            dict(name="normalized", expression=TEXT_EXPRESSION),
        ]),
        node("output", "file.parquet.destination", connection="output", fileMode="overwrite"),
    ]
    paths = [
        {"from": "source", "to": "derive", "output": 0, "input": 0},
        {"from": "derive", "to": "output", "output": 0, "input": 0},
    ]
    connections = [dict(id=name, driver="file", resource=str(path))
                   for name, path in (("input", source), ("output", target))]
    return dict(version=VERSION, id="derived-rows", connections=connections,
                controlFlow=dict(tasks=[dict(id="flow", kind="dataFlow",
                                             config=dict(pipeline="rows"))], constraints=[]),
                dataFlows=[dict(id="rows", components=nodes, paths=paths)])


def capture(runner, input_csv, output):
    with tempfile.TemporaryDirectory(prefix="ud-native-ssis-") as scratch:
        root = Path(scratch)
        source, target, package = (root / name for name in ("input.ndjson", "out.parquet", "package.json"))
        with input_csv.open(encoding="cp1252", newline="") as stream, source.open("w", encoding="utf-8") as rows:
            for identity, amount, label in csv.reader(stream):
                rows.write(json.dumps(dict(id=int(identity), amount=int(amount), label=label)) + "\n")
        package.write_text(json.dumps(document(source, target)), encoding="utf-8")
        (root / "state").mkdir()
        run = subprocess.run([str(runner), str(package), str(root / "state"), "0", "2"],
                             check=True, capture_output=True, text=True, timeout=300)
        stats = json.loads(run.stdout)
        if stats["result"]:
            raise ValueError(f"Native Derived Column execution failed: {stats}\n{run.stderr}")

        data = parquet.ParquetFile(target)
        columns = {}
        for name in ("calculated", "normalized"):
            column = data.schema_arrow.field(name)
            kind, length, code_page, *_ = map(int, column.metadata[b"ud.scalar"].split(b","))
            numeric = pa.types.is_decimal(column.type)
            columns[name] = dict(ssisType=SCALAR_TYPES[kind], length=length,
                                 precision=column.type.precision if numeric else 0,
                                 scale=column.type.scale if numeric else 0, codePage=code_page)
        with output.open("w", encoding="utf-8", newline="\n") as stream:
            for batch in data.iter_batches(columns=["id", *columns]):
                for row in batch.to_pylist():
                    for column, metadata in columns.items():
                        value = row[column]
                        observation = dict(caseId="derived-rows", rowId=row["id"], componentId="derive",
                                           outputColumnId=column, value=value, isNull=value is None,
                                           errorCode=0, errorColumn=0, outputPort=0, **metadata)
                        stream.write(json.dumps(observation, ensure_ascii=False, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture(args.runner.resolve(), args.input, args.output)
