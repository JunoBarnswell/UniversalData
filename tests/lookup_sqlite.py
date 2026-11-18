"""SQLite acceptance for native Lookup ports, query counts, and crash replay."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from product_version import PROTOCOL_VERSION


def field(name, type_="utf8", nullable=False, **metadata):
    return dict(name=name, type=type_, nullable=nullable, **metadata)


def schema(*fields):
    return dict(version=PROTOCOL_VERSION, fields=fields)


def component(id_, kind, **config):
    return dict(id=id_, component=kind, config=config)


def edge(source, target, port=0):
    return {"from": source, "to": target, "input": 0, "output": port}


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def fixture(root, plugin, mode, probes, no_match="output", batch=3, limit=4, scope="execution"):
    root.mkdir()
    (root / "state").mkdir()
    (root / "spill").mkdir()
    (root / "input.ndjson").write_text(
        "".join(json.dumps({"seq": i, "probe": value}) + "\n" for i, value in enumerate(probes))
    )
    db = root / "lookup.db"
    with closing(sqlite3.connect(db)) as connection, connection:
        connection.executescript(
            "CREATE TABLE reference(rkey TEXT COLLATE NOCASE NOT NULL PRIMARY KEY, "
            "payload TEXT NOT NULL); INSERT INTO reference VALUES ('Alice', 'original');"
        )
    reference = schema(field("rkey", keyOrder=1), field("payload"))
    config = dict(cache="reference", keys=[1], noMatch=no_match, errorPolicy="redirect",
                  includeInput=True)
    nodes = [
        component("probes", "file.ndjson.source", connection="input", batchSize=batch,
                  schema=schema(field("seq", "int64"), field("probe", nullable=True))),
        component("lookup", "cacheLookup" if mode == "full" else "dbLookup", **config),
    ]
    connections = [
        dict(id="db", driver=plugin, resource=str(db), credentialReference="fixture"),
        dict(id="input", driver="file", resource=str(root / "input.ndjson")),
        dict(id="spill", driver="file", resource=str(root / "spill")),
    ]
    paths = [edge("probes", "lookup")]
    for port, output in enumerate(("matched", "rejected", "unmatched")):
        nodes.append(component(output, "file.ndjson.destination", connection=output))
        paths.append(edge("lookup", output, port))
        connections.append(dict(id=output, driver="file", resource=str(root / f"{output}.ndjson")))
    tasks = [dict(id="lookup", kind="dataFlow", config=dict(pipeline="lookup"))]
    flows = [dict(id="lookup", components=nodes, paths=paths)]
    constraints = []
    if mode == "full":
        tasks.insert(0, dict(id="build", kind="dataFlow", config=dict(pipeline="build")))
        constraints.append({"from": "build", "to": "lookup", "on": "success"})
        flows.insert(0, dict(id="build", components=[
            component("reference", "sql.source", connection="db", table="reference",
                      schema=reference, schemaPolicy=dict(policy="strict")),
            component("build", "cacheBuild", cache="reference", mapping=[0, 1]),
        ], paths=[edge("reference", "build")]))
    document = dict(
        version=PROTOCOL_VERSION, id="sqlite-lookup", connections=connections, dataFlows=flows,
        controlFlow=dict(tasks=tasks, constraints=constraints),
        policy=dict(buffers=dict(connection="spill", residentBytes=8192,
                                 workingBytes=4194304, spillBytes=16777216, maxBlocks=64)),
        caches=[dict(id="reference", sourceConnection="db", sourceTable="reference",
                     schema=reference, keys=[0], mode=mode, scope=scope,
                     maxEntries=limit, ttlMs=600000)],
    )
    (root / "package.json").write_text(json.dumps(document))
    return db


def native(root, runner, resume=False, kill=False, epoch=False):
    command = [runner, str(root / "package.json"), str(root / "state"), str(int(resume)), "1"]
    if kill:
        command.extend(["3", "1", "lookup"])
    if epoch:
        command.extend(["0", "1", "", "67108864", "", "lookup-epoch"])
    execution = subprocess.run(command, capture_output=True, text=True, timeout=60)
    if kill:
        assert execution.returncode == 91, (execution.stdout, execution.stderr)
        return None
    assert execution.returncode == 0, (execution.stdout, execution.stderr)
    result = json.loads(execution.stdout)
    if result["result"]:
        timelines = [
            (flow["id"], flow["confirmedEpoch"], [
                (node["id"], node["result"], node["state"], node["steps"],
                 node["processedRows"], node["producedRows"], node.get("facts"))
                for tree in flow["trees"] for node in tree["nodes"]
            ])
            for flow in result.get("diagnostics", {}).get("pipelines", [])
        ]
        raise AssertionError((root.name, result["result"], execution.stderr, timelines))
    return result


def lookup_facts(value):
    if isinstance(value, dict):
        if "sqlRequests" in value:
            return value
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return None
    return next((found for child in children if (found := lookup_facts(child))), None)


def assert_ports(root, mode, probes, no_match):
    with closing(sqlite3.connect(root / "lookup.db")) as connection:
        collation = " COLLATE BINARY" if mode == "full" else ""
        expected = [
            connection.execute(
                f"SELECT rkey,payload FROM reference WHERE rkey{collation}=?", (probe,)
            ).fetchone()
            for probe in probes
        ]
    actual = records(root / "matched.ndjson")
    assert [(r["seq"], r["rkey"], r["payload"]) for r in actual] == [
        (i, *match) for i, match in enumerate(expected) if match
    ], actual
    unmatched = records(root / "unmatched.ndjson")
    rejected = records(root / "rejected.ndjson")
    missed = [(i, probes[i]) for i, match in enumerate(expected) if not match]
    if no_match == "output":
        assert [(r["seq"], r["probe"]) for r in unmatched] == missed, unmatched
        assert not rejected, rejected
    else:
        assert not unmatched, unmatched
        assert len(rejected) == len(missed), rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runner")
    parser.add_argument("sqlite")
    args = parser.parse_args()
    probes = ["alice", "alice", "unknown", "Alice", "unknown", None, "alice", "Alice", None]
    with tempfile.TemporaryDirectory(prefix="ud-lookup-") as folder:
        root = Path(folder)
        for mode in ("full", "partial", "none"):
            case = root / mode
            fixture(case, args.sqlite, mode, probes)
            result = native(case, args.runner)
            assert_ports(case, mode, probes, "output")
            facts = lookup_facts(result)
            if mode == "none":
                assert facts["sqlRequests"] == facts["sqlProbes"] == len(probes), facts
            if mode == "partial":
                assert facts["sqlRequests"] == 2 and facts["sqlProbes"] == 3, facts
        case = root / "bounded-partial"
        fixture(case, args.sqlite, "partial", probes, limit=1)
        native(case, args.runner)
        assert_ports(case, "partial", probes, "output")
        case = root / "record-error"
        fixture(case, args.sqlite, "none", probes, no_match="ignore")
        native(case, args.runner)
        assert_ports(case, "none", probes, "ignore")

        for mode in ("partial", "none"):
            case = root / f"recover-{mode}"
            db = fixture(case, args.sqlite, mode, ["alice", "alice", "alice"], batch=1,
                         scope="epoch")
            native(case, args.runner, kill=True)
            with closing(sqlite3.connect(db)) as connection, connection:
                connection.execute("UPDATE reference SET payload='changed'")
            result = native(case, args.runner, resume=True)
            assert [(r["seq"], r["payload"]) for r in records(case / "matched.ndjson")] == [
                (0, "original"), (1, "changed"), (2, "changed")
            ]
            facts = lookup_facts(result)
            assert facts["replayedBatches"] == 1, facts
            assert facts["sqlRequests"] == (2 if mode == "none" else 1), facts

        for mode in ("partial", "none"):
            case = root / f"stream-{mode}"
            db = fixture(case, args.sqlite, mode, [], scope="epoch")
            with closing(sqlite3.connect(db)) as writer:
                assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
            path = case / "package.json"
            document = json.loads(path.read_text())
            flow = document["dataFlows"][0]
            flow["components"] = [
                component("probes", "fixture.lookup.source"),
                component("lookup", "dbLookup", cache="reference", keys=[1]),
                component("oracle", "fixture.lookup.sink"),
            ]
            flow["paths"] = [edge("probes", "lookup"), edge("lookup", "oracle")]
            document["connections"][0]["maxLeases"] = 3
            path.write_text(json.dumps(document))
            result = native(case, args.runner, epoch=True)
            facts = lookup_facts(result)
            assert facts["sqlRequests"] == facts["sqlProbes"] == 3, facts
    print("Native SQLite Lookup modes, ports, bounded cache and crash replay passed")


if __name__ == "__main__":
    main()
