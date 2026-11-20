"""Serialize row values produced by DTExec and metadata read from its pipeline."""
import csv
import hashlib
import json
import os
from pathlib import Path
import sys


def capture(root, row_count):
    with (root / "metadata.csv").open(encoding="utf-8", newline="") as stream:
        columns = {
            name: dict(ssisType=kind, length=int(length), precision=int(precision),
                       scale=int(scale), codePage=int(code_page))
            for name, kind, length, precision, scale, code_page in csv.reader(stream)
        }
    assert set(columns) == {"calculated", "normalized"}
    seen = set()
    with (root / "output.csv").open(encoding="cp1252", newline="") as stream, (
        root / "observations.ndjson"
    ).open("w", encoding="utf-8", newline="\n") as output:
        rows = csv.reader(stream)
        header = next(rows)
        if header != ["id", "calculated", "normalized"]:
            raise ValueError(f"Unexpected Microsoft SSIS output columns: {header!r}")
        for identity, numeric, label in rows:
            row = int(identity)
            assert 1 <= row <= row_count and row not in seen
            seen.add(row)
            for column, value in (("calculated", int(numeric) if numeric.strip() else None),
                                  ("normalized", label)):
                observation = dict(caseId="derived-rows", rowId=row, componentId="derive",
                                   outputColumnId=column, value=value, isNull=value is None,
                                   errorCode=0, errorColumn=0, outputPort=0, **columns[column])
                output.write(json.dumps(observation, ensure_ascii=False, separators=(",", ":")) + "\n")
    assert len(seen) == row_count, (len(seen), row_count)

    def record(role, file):
        return dict(role=role, path=file, sha256=hashlib.sha256((root / file).read_bytes()).hexdigest())

    artifacts = [
        record("package", "baseline.dtsx"),
        record("observations", "observations.ndjson"),
        record("executionLog", "dtexec.log"),
        record("input", "input.csv"),
    ]
    environment = dict(
        ssisVersion=os.environ["UD_SSIS_CAPTURE_RUNTIME"],
        sqlServerVersion=os.environ["UD_SSIS_CAPTURE_RUNTIME"],
        ssdtVersion=os.environ["UD_SSIS_CAPTURE_SSDT"],
        windowsVersion=os.environ["UD_SSIS_CAPTURE_WINDOWS"],
        localeId=1033,
        codePage=1252,
        timeZone=os.environ["UD_SSIS_CAPTURE_TZ"],
        command=os.environ["UD_SSIS_CAPTURE_COMMAND"],
        packageSha256=artifacts[0]["sha256"],
    )
    manifest = dict(producer="Microsoft SSIS", platform="Windows",
                    environment=environment, artifacts=artifacts)
    (root / "capture.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    capture(Path(sys.argv[1]), int(sys.argv[2]))
