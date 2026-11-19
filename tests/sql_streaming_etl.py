"""SQLite SQL Source streaming contract.

This is an end-to-end product test.  It uses the real CLI, Native SQLite
connector and a separate source/target database for each mode.  The Native
diagnostic flag is the authoritative proof that stream mode did not build the
snapshot before handing the first batch to the destination.
"""
import argparse
from contextlib import closing
import json
import os
import pathlib
import signal
import sqlite3
import subprocess
import tempfile
import time

from product_version import PROTOCOL_VERSION


parser = argparse.ArgumentParser()
parser.add_argument("--cli", required=True, type=pathlib.Path)
parser.add_argument("--sqlite", required=True, type=pathlib.Path)
parser.add_argument("--performance-json", type=pathlib.Path)
parser.add_argument("--performance-rows", nargs="+", type=int, default=[100000, 1000000])
parser.add_argument("--performance-batch", type=int, default=512)
parser.add_argument("--performance-timeout", type=int, default=240)
args = parser.parse_args()


def schema():
    return {
        "version": PROTOCOL_VERSION,
        "fields": [
            {"name": "id", "type": "int64", "nullable": False},
            {"name": "label", "type": "utf8", "nullable": False},
        ],
    }


def error_schema():
    return schema_fields(
        [
            ("executionId", "utf8", False),
            ("taskId", "utf8", False),
            ("pipelineId", "utf8", False),
            ("componentId", "utf8", False),
            ("ruleId", "utf8", False),
            ("ruleVersion", "int64", False),
            ("columnId", "utf8", False),
            ("sourceRowId", "utf8", False),
            ("errorCode", "int64", False),
            ("policy", "utf8", False),
            ("errorKind", "utf8", False),
            ("providerCode", "utf8", False),
            ("inputFormat", "utf8", False),
            ("sourcePosition", "int64", True),
            ("packageId", "utf8", False),
        ]
    )


def schema_fields(fields):
    return {
        "version": PROTOCOL_VERSION,
        "fields": [
            {"name": name, "type": field_type, "nullable": nullable}
            for name, field_type, nullable in fields
        ],
    }


def package(plugin, source, target, stream, sql=None, batch_size=17):
    config = {
        "connection": "source",
        "schema": schema(),
        "sql": sql or "SELECT id,label FROM source_rows ORDER BY id",
        "batchSize": batch_size,
    }
    if stream:
        config["readMode"] = "stream"
    target_config = {"connection": "target", "table": "target_rows", "mode": "append"}
    if not stream:
        target_config["delivery"] = "response"
    return {
        "version": PROTOCOL_VERSION,
        "id": "sqlite-stream-etl-" + ("stream" if stream else "snapshot"),
        "connections": [
            {"id": "source", "driver": str(plugin), "resource": str(source), "credentialReference": "source-config"},
            {"id": "target", "driver": str(plugin), "resource": str(target), "credentialReference": "target-config"},
        ],
        "controlFlow": {
            "tasks": [{"id": "copy", "kind": "dataFlow", "config": {"pipeline": "copy"}}],
            "constraints": [],
        },
        "dataFlows": [
            {
                "id": "copy",
                "components": [
                    {"id": "read", "component": "sql.source", "config": config},
                    {"id": "write", "component": "sql.destination", "config": target_config},
                ],
                "paths": [{"from": "read", "output": 0, "to": "write", "input": 0}],
            }
        ],
    }


def policy(root, resources, plugin, source, target):
    source_config = root / (source.name + ".config")
    target_config = root / (target.name + ".config")
    source_config.write_text(str(source), encoding="utf-8")
    target_config.write_text(str(target), encoding="utf-8")
    return {
        "dataDirectory": str(root / "state"),
        "fileRoots": [str(resources)],
        "readOnlyRoots": [str(resources)],
        "networkOrigins": [],
        "pluginRoots": [str(plugin.parent)],
        "credentialFiles": {"source-config": str(source_config), "target-config": str(target_config)},
    }


def create_database(path, rows):
    with closing(sqlite3.connect(path)) as database:
        with database:
            database.execute("CREATE TABLE source_rows(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            database.executemany("INSERT INTO source_rows VALUES(?,?)", rows)
            database.execute("CREATE TABLE target_rows(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")


def query_database(path, query, parameters=()):
    with closing(sqlite3.connect(path)) as database:
        return database.execute(query, parameters).fetchall()


def source_node(facts):
    diagnostics = facts.get("diagnostics", {})
    for flow in diagnostics.get("pipelines", []):
        for tree in flow.get("trees", []):
            for node in tree.get("nodes", []):
                if node.get("id") == "read":
                    return node
    raise AssertionError("Native diagnostics did not contain SQL Source node")


def run(cli, policy_file, package_file, execution_id, resume=False):
    command = [str(cli), "execute", str(policy_file), str(package_file), "{}", execution_id]
    if resume:
        command.append("resume")
    return subprocess.run(command, capture_output=True, text=True, timeout=90)


def measure_delivery(root, plugin, stream, row_count, batch_size):
    resources = root / "resources"
    resources.mkdir(parents=True)
    source, target = resources / "source.db", resources / "target.db"
    create_database(source, ((index, f"row-{index}") for index in range(1, row_count + 1)))
    create_database(target, [])
    package_file, policy_file = root / "package.json", root / "policy.json"
    package_file.write_text(json.dumps(package(plugin, source, target, stream, batch_size=batch_size)),
                            encoding="utf-8")
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")
    command = [str(args.cli), "execute", str(policy_file), str(package_file), "{}", "measure-delivery"]
    first = None
    with tempfile.TemporaryFile(mode="w+b") as output:
        start = time.perf_counter()
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.DEVNULL)
        with closing(sqlite3.connect(target, timeout=0.05)) as probe:
            while child.poll() is None and time.perf_counter() - start < args.performance_timeout:
                if first is None:
                    try:
                        if probe.execute("SELECT EXISTS(SELECT 1 FROM target_rows)").fetchone()[0]:
                            first = time.perf_counter() - start
                    except sqlite3.OperationalError as error:
                        if "locked" not in str(error):
                            child.kill()
                            child.wait()
                            raise
                time.sleep(0.005)
        elapsed = time.perf_counter() - start
        if child.poll() is None:
            child.kill()
            child.wait()
            return {"status": "Timeout"}
        output.seek(0)
        try:
            facts = json.load(output)
        except json.JSONDecodeError:
            return {"status": "Fail", "processExitCode": child.returncode}
    if child.returncode != 0:
        return {"status": "Fail", "nativeStatus": facts["result"]}
    node = source_node(facts)
    checksum = 0
    with closing(sqlite3.connect(target)) as database:
        count = 0
        for count, (key, label) in enumerate(database.execute(
                "SELECT id,label FROM target_rows ORDER BY id"), 1):
            assert key == count and label == f"row-{count}"
            checksum = (checksum * 1099511628211 + key) & ((1 << 64) - 1)
        assert count == row_count
    source_facts = node["facts"]
    expected_snapshot_rows = 0 if stream else row_count
    passed = (
        first is not None
        and facts["queueHighWater"] <= 1
        and source_facts["materialized"] == (not stream)
        and source_facts["snapshotRows"] == expected_snapshot_rows
    )
    status = "Pass" if passed else "Fail"
    return {"status": status,
            "firstCommitObservedMs": None if first is None else first * 1000,
            "elapsedMs": elapsed * 1000, "rowsPerSecond": row_count / elapsed,
            "sourcePrepareMs": node["prepareMs"], "sourceFacts": source_facts,
            "queueHighWater": facts["queueHighWater"], "readChecksum": f"{checksum:016x}",
            "buffers": facts["diagnostics"]["buffers"]}


def run_mode(root, plugin, stream):
    resources = root / ("stream-resources" if stream else "snapshot-resources")
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    rows = [(index, f"row-{index}") for index in range(1, 2001)]
    create_database(source, rows)
    create_database(target, [])
    package_file = root / ("stream.json" if stream else "snapshot.json")
    package_file.write_text(json.dumps(package(plugin, source, target, stream)), encoding="utf-8")
    policy_file = root / ("stream-policy.json" if stream else "snapshot-policy.json")
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")
    execution_id = "stream-etl" if stream else "snapshot-etl"
    child = run(args.cli, policy_file, package_file, execution_id)
    assert child.returncode == 0, (child.returncode, child.stdout, child.stderr)
    facts = json.loads(child.stdout)
    assert facts["result"] == 0, facts
    actual = query_database(target, "SELECT id,label FROM target_rows ORDER BY id")
    assert actual == rows, (len(actual), len(rows))
    node = source_node(facts)
    assert bool(node["facts"]["materialized"]) is (not stream), node
    if stream:
        # No Journal is created for an ordinary stream execution.  The CLI
        # still records history in its catalog, so a second resume attempt is
        # rejected by the same Native RequiresJournal fact.
        retry = run(args.cli, policy_file, package_file, execution_id, resume=True)
        assert retry.returncode != 0, (retry.stdout, retry.stderr)
        return facts
    return facts


def invalid_stream_is_rejected(root, plugin):
    resources = root / "invalid-resources"
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    create_database(source, [(1, "one")])
    create_database(target, [])
    document = package(plugin, source, target, True)
    document["dataFlows"][0]["components"][0]["config"]["incremental"] = {"mode": "key", "key": 0}
    package_file = root / "invalid.json"
    package_file.write_text(json.dumps(document), encoding="utf-8")
    policy_file = root / "invalid-policy.json"
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")
    result = run(args.cli, policy_file, package_file, "invalid-stream")
    assert result.returncode != 0, (result.stdout, result.stderr)
    transaction = package(plugin, source, target, True)
    transaction["controlFlow"]["tasks"][0]["transaction"] = {
        "mode": "required",
        "participants": ["source", "target"],
    }
    transaction_file = root / "transaction.json"
    transaction_file.write_text(json.dumps(transaction), encoding="utf-8")
    result = run(args.cli, policy_file, transaction_file, "transaction-stream")
    assert result.returncode != 0, (result.stdout, result.stderr)
    receipt = package(plugin, source, target, True)
    receipt["dataFlows"][0]["components"][1]["config"]["delivery"] = "receipt"
    receipt_file = root / "receipt.json"
    receipt_file.write_text(json.dumps(receipt), encoding="utf-8")
    result = run(args.cli, policy_file, receipt_file, "receipted-stream")
    assert result.returncode != 0, (result.stdout, result.stderr)
    assert query_database(target, "SELECT count(*) FROM target_rows") == [(0,)]


def mixed_conversion_rows(root, plugin):
    resources = root / "mixed-resources"
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    with closing(sqlite3.connect(source)) as database:
        with database:
            database.execute("CREATE TABLE source_rows(id, label TEXT NOT NULL)")
            database.executemany("INSERT INTO source_rows VALUES(?,?)", [(1, "one"), ("bad", "bad"), (3, "three")])
    with closing(sqlite3.connect(target)) as database:
        with database:
            database.execute("CREATE TABLE target_rows(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            database.execute("CREATE TABLE errors(executionId TEXT NOT NULL, taskId TEXT NOT NULL, pipelineId TEXT NOT NULL, componentId TEXT NOT NULL, ruleId TEXT NOT NULL, ruleVersion INTEGER NOT NULL, columnId TEXT NOT NULL, sourceRowId TEXT NOT NULL, errorCode INTEGER NOT NULL, policy TEXT NOT NULL, errorKind TEXT NOT NULL, providerCode TEXT NOT NULL, inputFormat TEXT NOT NULL, sourcePosition INTEGER, packageId TEXT NOT NULL)")
    document = package(plugin, source, target, True)
    components = document["dataFlows"][0]["components"]
    components[0]["config"]["errorPolicy"] = "redirect"
    components.extend(
        [
            {"id": "error-cast", "component": "cast", "config": {"schema": error_schema()}},
            {"id": "errors", "component": "sql.destination", "config": {"connection": "target", "table": "errors", "mode": "append", "delivery": "response"}},
        ]
    )
    document["dataFlows"][0]["paths"].extend(
        [
            {"from": "read", "output": 1, "to": "error-cast", "input": 0},
            {"from": "error-cast", "output": 0, "to": "errors", "input": 0},
        ]
    )
    package_file = root / "mixed.json"
    package_file.write_text(json.dumps(document), encoding="utf-8")
    policy_file = root / "mixed-policy.json"
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")
    result = run(args.cli, policy_file, package_file, "mixed-stream")
    assert result.returncode == 0, (result.stdout, result.stderr)
    actual = query_database(target, "SELECT id,label FROM target_rows ORDER BY id")
    assert actual == [(1, "one"), (3, "three")], actual
    error_rows = query_database(target, "SELECT componentId FROM errors")
    assert error_rows == [("read",)]


def stream_sql_command(root, plugin):
    resources = root / "command-resources"
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    expected = [(index, f"row-{index}") for index in range(1, 36)]
    create_database(source, expected)
    create_database(target, [])
    with closing(sqlite3.connect(target)) as database, database:
        database.execute("CREATE TABLE command_rows(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
    document = package(plugin, source, target, True)
    flow = document["dataFlows"][0]
    flow["components"].insert(1, {"id": "command", "component": "sql.command", "config": {
        "connection": "target",
        "statement": "INSERT INTO command_rows(id,label) VALUES(?1,?2)",
        "parameters": [0, 1],
    }})
    flow["paths"] = [
        {"from": "read", "output": 0, "to": "command", "input": 0},
        {"from": "command", "output": 0, "to": "write", "input": 0},
    ]
    package_file, policy_file = root / "command.json", root / "command-policy.json"
    package_file.write_text(json.dumps(document), encoding="utf-8")
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")
    result = run(args.cli, policy_file, package_file, "command-stream")
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert query_database(target, "SELECT id,label FROM command_rows ORDER BY id") == expected
    assert query_database(target, "SELECT id,label FROM target_rows ORDER BY id") == expected
    assert query_database(target, "SELECT count(*) FROM sqlite_schema WHERE name='ud_sdk_receipts_v11'") == [(0,)]


def first_batch_commits_before_stream_failure(root, plugin):
    """A conversion error in batch two must not erase stream batch one."""
    resources = root / "partial-resources"
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    with closing(sqlite3.connect(source)) as database:
        with database:
            database.execute("CREATE TABLE source_rows(seq INTEGER PRIMARY KEY, id, label TEXT NOT NULL)")
            database.executemany(
                "INSERT INTO source_rows VALUES(?,?,?)",
                [(index, index, f"row-{index}") for index in range(1, 18)]
                + [(18, "bad", "bad"), (19, 19, "row-19")],
            )
    create_database(target, [])
    sql = "SELECT id,label FROM source_rows ORDER BY seq"
    for stream, expected_rows in ((False, 0), (True, 17)):
        database_path = target if not stream else resources / "stream-target.db"
        if stream:
            create_database(database_path, [])
        document = package(plugin, source, database_path, stream, sql=sql)
        package_file = root / ("partial-stream.json" if stream else "partial-snapshot.json")
        package_file.write_text(json.dumps(document), encoding="utf-8")
        policy_file = root / ("partial-stream-policy.json" if stream else "partial-snapshot-policy.json")
        policy_file.write_text(json.dumps(policy(root, resources, plugin, source, database_path)), encoding="utf-8")
        result = run(args.cli, policy_file, package_file, "partial-" + ("stream" if stream else "snapshot"))
        assert result.returncode != 0, (stream, result.stdout, result.stderr)
        actual = query_database(database_path, "SELECT id,label FROM target_rows ORDER BY id")
        assert len(actual) == expected_rows, (stream, actual)
        if stream:
            assert actual == [(index, f"row-{index}") for index in range(1, 18)]


def cancellation_closes_stream(root, plugin):
    """Hold the target transaction so the live source is cancelled mid-batch."""
    resources = root / "cancel-resources"
    resources.mkdir()
    source, target = resources / "source.db", resources / "target.db"
    rows = [(index, f"row-{index}") for index in range(1, 2001)]
    create_database(source, rows)
    create_database(target, [])
    package_file = root / "cancel.json"
    package_file.write_text(json.dumps(package(plugin, source, target, True)), encoding="utf-8")
    policy_file = root / "cancel-policy.json"
    policy_file.write_text(json.dumps(policy(root, resources, plugin, source, target)), encoding="utf-8")

    lock = sqlite3.connect(target, timeout=0)
    # RESERVED keeps metadata reads available, but blocks the destination's
    # first write after SQL Source has opened its live cursor.
    lock.execute("BEGIN IMMEDIATE")
    command = [str(args.cli), "execute", str(policy_file), str(package_file), "{}", "cancel-stream"]
    process_options = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if os.name == "nt":
        process_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        process_options["start_new_session"] = True
    child = subprocess.Popen(command, **process_options)
    try:
        deadline = time.monotonic() + 10
        while child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert child.poll() is None, "stream execution finished before cancellation"
        child.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
        stdout, stderr = child.communicate(timeout=20)
        assert child.returncode != 0, (stdout, stderr)
        if stdout:
            result = json.loads(stdout)
            assert result["result"] != 0, result
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        lock.rollback()
        lock.close()
    assert query_database(target, "SELECT count(*) FROM target_rows")[0][0] == 0


with tempfile.TemporaryDirectory(prefix="ud-sql-streaming-etl-") as temporary:
    root = pathlib.Path(temporary)
    root.joinpath("state").mkdir()
    plugin = args.sqlite.resolve()
    if not plugin.is_file():
        raise AssertionError(f"SQLite connector is missing: {plugin}")
    if args.performance_json:
        if min(args.performance_rows) < 1 or not 1 <= args.performance_batch <= 4096 or args.performance_timeout < 1:
            parser.error("Use positive row counts and timeout, and a batch size between 1 and 4096")
        args.performance_json.parent.mkdir(parents=True, exist_ok=True)
        results = []
        for rows in args.performance_rows:
            snapshot = measure_delivery(root / f"{rows}-snapshot", plugin, False, rows, args.performance_batch)
            stream = measure_delivery(root / f"{rows}-stream", plugin, True, rows, args.performance_batch)
            results.append({"rows": rows, "batchRows": args.performance_batch,
                            "snapshot": snapshot, "stream": stream})
            args.performance_json.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        raise SystemExit(0 if all(case[mode]["status"] == "Pass" for case in results
                                  for mode in ("snapshot", "stream")) else 1)
    snapshot = run_mode(root, plugin, False)
    stream = run_mode(root, plugin, True)
    mixed_conversion_rows(root, plugin)
    stream_sql_command(root, plugin)
    first_batch_commits_before_stream_failure(root, plugin)
    cancellation_closes_stream(root, plugin)
    invalid_stream_is_rejected(root, plugin)
    print(
        "SQLite SQL ETL: snapshot materialization, stream first-batch path, "
        "bounded 17-row transfer, mixed conversion error output, SQL Command delivery, first-batch commit before "
        "stream failure, cancellation cleanup, Native history and illegal stream "
        "recovery/incremental/transaction combinations passed"
    )
