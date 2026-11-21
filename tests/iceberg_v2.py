"""Real Iceberg v2 REST/Trino acceptance for Native source and destination.

The fixture deliberately has no in-process catalog or file-service double.  The
Native package talks to the configured REST catalog and local Iceberg files by
default. The ``UD_TEST_ICEBERG_S3_*`` variables run real S3 writes, reads and
same-execution recovery. Trino reads the same table through its
``/v1/statement`` endpoint.  The optional observer executable is the existing
``test_next_phase_execution`` runner, used only to terminate a real Native
execution at a journal boundary.
"""

import argparse
from contextlib import closing
import base64
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import sqlite3
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

from product_version import PROTOCOL_VERSION


FIELD_IDS = {
    "id": "00000000000000000000000000000001",
    "value": "00000000000000000000000000000002",
    "amount": "00000000000000000000000000000003",
}
BEFORE_CHECKPOINT = 2
AFTER_CHECKPOINT = 3
TARGET_COMMITTED = 4
AUTH_DENIED = 13


def field(name, kind, nullable=False, key_order=0, identity=None):
    result = {"name": name, "type": kind, "nullable": nullable}
    if key_order:
        result["keyOrder"] = key_order
    if identity:
        result["identity"] = identity
    return result


def schema(value_name="value"):
    return {
        "version": PROTOCOL_VERSION,
        "fields": [
            field("id", "int64", key_order=1, identity=FIELD_IDS["id"]),
            field(value_name, "utf8", identity=FIELD_IDS["value"]),
            field("amount", "int64", identity=FIELD_IDS["amount"]),
        ],
    }


def source_schema(value_name="value"):
    # The source contract takes a schema object rather than a separate schema
    # connection.  Keep the Iceberg field IDs stable across the SQL alias
    # rename so the destination is tested by ID, not by column position/name.
    return schema(value_name)


def no_proxy_opener():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _headers(extra=None, user=None, password=None):
    headers = {"Accept": "application/json"}
    if extra:
        headers.update(extra)
    if user:
        token = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
        headers["Authorization"] = "Basic " + token
    return headers


def http_json(url, method="GET", body=None, headers=None, user=None, password=None,
              timeout=45):
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url, data=payload, method=method,
        headers=_headers({"Content-Type": "application/json"} if payload else {},
                         user, password),
    )
    try:
        with no_proxy_opener().open(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            detail = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            detail = raw.decode("utf-8", errors="replace")
        raise AssertionError(f"HTTP {method} {url} failed ({error.code}): {detail}") from error


def read_json_location(location, storage_root, user=None, password=None):
    parsed = urllib.parse.urlsplit(location)
    if parsed.scheme in ("http", "https"):
        status, payload = http_json(location, user=user, password=password)
        assert 200 <= status < 300, (status, location)
        return payload
    if parsed.scheme == "file":
        path = Path(urllib.parse.unquote(parsed.path))
    else:
        path = Path(location)
        if not path.is_absolute():
            path = storage_root / path
    assert path.is_file(), f"Catalog metadata location is not a local file: {path}"
    return json.loads(path.read_text(encoding="utf-8"))


def catalog_endpoint(catalog, namespace, table):
    base = catalog.rstrip("/")
    if not base.endswith("/v1"):
        base += "/v1"
    return (base + "/namespaces/" + urllib.parse.quote(namespace, safe="") +
            "/tables/" + urllib.parse.quote(table, safe=""))


def catalog_state(catalog, namespace, table, storage_root, user=None, password=None):
    endpoint = catalog_endpoint(catalog, namespace, table)
    status, payload = http_json(endpoint, user=user, password=password)
    assert 200 <= status < 300, (status, payload)
    location = payload.get("metadata-location") or payload.get("metadataLocation")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        assert location, payload
        metadata = read_json_location(location, storage_root, user, password)
    assert metadata.get("format-version") == 2, metadata
    snapshots = metadata.get("snapshots") or []
    snapshot_ids = [int(item["snapshot-id"]) for item in snapshots]
    assert len(snapshot_ids) == len(set(snapshot_ids)), metadata
    current = metadata.get("current-snapshot-id", -1)
    assert current == -1 or int(current) in snapshot_ids, metadata
    history = metadata.get("snapshot-log") or []
    assert len(history) >= len(snapshots), metadata
    return {
        "endpoint": endpoint,
        "payload": payload,
        "metadataLocation": location,
        "metadata": metadata,
        "snapshotIds": snapshot_ids,
        "currentSnapshotId": int(current),
    }


def trino_endpoint(url):
    value = url.rstrip("/")
    return value if value.endswith("/v1/statement") else value + "/v1/statement"


def trino_query(url, sql, catalog_name, namespace, user, password=None):
    headers = _headers({
        "Content-Type": "text/plain; charset=utf-8",
        "X-Trino-User": user,
        "X-Trino-Catalog": catalog_name,
        "X-Trino-Schema": namespace,
    }, None, None)
    opener = no_proxy_opener()
    request = urllib.request.Request(trino_endpoint(url), data=sql.encode("utf-8"),
                                     method="POST", headers=headers)
    if user and password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        request.add_header("Authorization", "Basic " + token)
    rows = []
    try:
        with opener.open(request, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        while True:
            if payload.get("error"):
                raise AssertionError(f"Trino query failed: {payload['error']}")
            rows.extend(payload.get("data") or [])
            next_uri = payload.get("nextUri")
            if not next_uri:
                return rows
            follow = urllib.request.Request(next_uri, method="GET", headers=headers)
            if user and password:
                follow.add_header("Authorization", "Basic " + token)
            with opener.open(follow, timeout=45) as response:
                payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise AssertionError(f"Trino REST query failed ({error.code}): {detail}") from error


def trino_rows(args, namespace, table):
    qualified = ".".join(quote_identifier(part) for part in
                           (args.trino_catalog, namespace, table))
    return [tuple(row) for row in trino_query(
        args.trino, f"SELECT id, value, amount FROM {qualified} ORDER BY id",
        args.trino_catalog, namespace, args.trino_user, args.trino_password,
    )]


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def partitioned_table_name(args, namespace, table):
    return ".".join(quote_identifier(part) for part in
                      (args.trino_catalog, namespace, table))


def create_partitioned_table(args, namespace, table, location):
    # The table is deliberately created by Trino.  Native create currently
    # uses Iceberg's unpartitioned policy; this fixture tests the real
    # consumer path for a table whose partition spec is catalog-owned.
    qualified = partitioned_table_name(args, namespace, table)
    trino_query(
        args.trino,
        f"CREATE TABLE {qualified} ("
        "id BIGINT NOT NULL, value VARCHAR NOT NULL, amount BIGINT NOT NULL) "
        "WITH (format = 'PARQUET', "
        f"location = {sql_literal(location.as_uri())}, "
        "partitioning = ARRAY['bucket(id, 2)'])",
        args.trino_catalog, namespace, args.trino_user, args.trino_password,
    )


def evolve_partitioning(args, namespace, table, transform):
    qualified = partitioned_table_name(args, namespace, table)
    trino_query(
        args.trino,
        f"ALTER TABLE {qualified} SET PROPERTIES "
        f"partitioning = ARRAY[{sql_literal(transform)}]",
        args.trino_catalog, namespace, args.trino_user, args.trino_password,
    )


def partition_specs(metadata):
    specs = metadata.get("partition-specs") or []
    assert isinstance(specs, list) and specs, metadata
    return specs


def partition_spec(specs, spec_id):
    for spec in specs:
        if int(spec.get("spec-id", -1)) == int(spec_id):
            return spec
    raise AssertionError((spec_id, specs))


def physical_partition_files(location):
    files = sorted(location.rglob("*.parquet"))
    assert files, f"No Parquet data files under the shared warehouse: {location}"
    directories = {
        str(path.parent.relative_to(location))
        for path in files
        if any("=" in part for part in path.relative_to(location).parts[:-1])
    }
    assert directories, (location, [str(path) for path in files])
    return files, directories


def partitioned_regression(args, root, plugin, catalog, storage, namespace):
    """Exercise Native writes against real Trino-created/evolved partitions."""
    case = root / "partitioned"
    case.mkdir()
    source_files = case / "files"
    source_files.mkdir()
    source_db = source_files / "source.db"
    table = "partitioned_orders"
    table_location = storage / namespace / table
    table_location.mkdir(parents=True, exist_ok=True)
    create_partitioned_table(args, namespace, table, table_location)

    initial = [(i, f"value-{i}", i * 10) for i in range(1, 17)]
    create_source(source_db, initial)
    policy = write_cli_policy(case / "policy", plugin, catalog, storage,
                              source_db=source_db)
    append = package_document(plugin, catalog, storage, source_db, namespace, table)
    run_cli(args, append, policy, case / "append-state", "iceberg-partition-append")
    assert trino_rows(args, namespace, table) == initial
    first = catalog_state(catalog, namespace, table, storage,
                          args.catalog_user, args.catalog_password)
    first_specs = partition_specs(first["metadata"])
    first_spec_id = int(first["metadata"]["default-spec-id"])
    first_spec = partition_spec(first_specs, first_spec_id)
    assert any("bucket[2]" in str(field.get("transform"))
               for field in first_spec.get("fields", [])), first_specs
    files, directories = physical_partition_files(table_location)
    assert len(directories) >= 2, directories
    assert any(f"-s{first_spec_id}-" in path.name for path in files), files

    # Keep most rows in the original spec, while moving two existing rows and
    # one new row into the evolved spec.  The renamed value column proves the
    # write still maps by Iceberg field ID.
    evolve_partitioning(args, namespace, table, "bucket(id, 4)")
    evolved = catalog_state(catalog, namespace, table, storage,
                            args.catalog_user, args.catalog_password)
    evolved_specs = partition_specs(evolved["metadata"])
    evolved_spec_id = int(evolved["metadata"]["default-spec-id"])
    assert evolved_spec_id != first_spec_id and len(evolved_specs) >= 2, evolved_specs
    assert any("bucket[4]" in str(field.get("transform"))
               for field in partition_spec(evolved_specs, evolved_spec_id).get("fields", [])), evolved_specs

    updates = [(1, "value-1-updated", 1001), (9, "value-9-updated", 1009),
               (17, "value-17", 170)]
    update_db = source_files / "update.db"
    create_source(update_db, updates)
    update_policy = write_cli_policy(case / "update-policy", plugin, catalog, storage,
                                     source_db=update_db)
    upsert = package_document(plugin, catalog, storage, update_db, namespace, table,
                              mode="upsert", renamed=True)
    run_cli(args, upsert, update_policy, case / "update-state", "iceberg-partition-upsert")
    expected = {row[0]: row for row in initial}
    expected.update({row[0]: row for row in updates})
    expected_rows = [expected[key] for key in sorted(expected)]
    assert trino_rows(args, namespace, table) == expected_rows
    after_upsert = catalog_state(catalog, namespace, table, storage,
                                 args.catalog_user, args.catalog_password)
    assert int(after_upsert["metadata"]["default-spec-id"]) == evolved_spec_id
    files, _ = physical_partition_files(table_location)
    assert any(f"-s{evolved_spec_id}-" in path.name for path in files), files

    # A second catalog-owned evolution leaves the table with multiple physical
    # specs.  The Iceberg component currently exposes append/upsert/changeset;
    # a SQL source has insert row metadata only, so standalone ``delete`` is an
    # explicit schema rejection rather than a silently misclassified append.
    evolve_partitioning(args, namespace, table, "bucket(id, 8)")
    latest = catalog_state(catalog, namespace, table, storage,
                           args.catalog_user, args.catalog_password)
    latest_specs = partition_specs(latest["metadata"])
    latest_spec_id = int(latest["metadata"]["default-spec-id"])
    assert latest_spec_id != evolved_spec_id and len(latest_specs) >= 3, latest_specs
    assert any("bucket[8]" in str(field.get("transform"))
               for field in partition_spec(latest_specs, latest_spec_id).get("fields", [])), latest_specs

    delete_db = source_files / "delete.db"
    create_source(delete_db, [(2, "value-2", 20), (9, "value-9-updated", 1009)])
    delete_policy = write_cli_policy(case / "delete-policy", plugin, catalog, storage,
                                     source_db=delete_db)
    delete = package_document(plugin, catalog, storage, delete_db, namespace, table,
                              mode="delete")
    files_before_rejected_delete, _ = physical_partition_files(table_location)
    run_cli(args, delete, delete_policy, case / "delete-state", "iceberg-partition-delete",
            expected_status=5)
    assert trino_rows(args, namespace, table) == expected_rows
    files_after_rejected_delete, _ = physical_partition_files(table_location)
    assert files_after_rejected_delete == files_before_rejected_delete
    print(
        "Iceberg partition slice passed: Trino REST-created bucket[2] table, "
        "Native physical partition append, catalog-owned bucket evolution, "
        "field-ID upsert across old/new specs. Standalone SQL delete is explicitly "
        "rejected (status 5); equality deletes remain available through changeset/CDC "
        "row metadata. Still unclaimed: Native partitioned-table creation, object-store "
        "credentials, compaction, and snapshot expiration/maintenance.",
        flush=True,
    )


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def sqlite_rows(path, table):
    with closing(sqlite3.connect(path)) as database:
        return database.execute(f"SELECT id,value,amount FROM {table} ORDER BY id").fetchall()


def create_source(path, rows):
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("CREATE TABLE source_rows(id INTEGER NOT NULL, value TEXT NOT NULL, amount INTEGER NOT NULL)")
        database.executemany("INSERT INTO source_rows VALUES(?,?,?)", rows)


def create_sink(path):
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("CREATE TABLE sink_rows(id INTEGER PRIMARY KEY, value TEXT NOT NULL, amount INTEGER NOT NULL)")


def sql_connection(identifier, plugin, database, reference):
    return {
        "id": identifier,
        "driver": str(plugin),
        "resource": str(database),
        "credentialReference": reference,
    }


def package_document(plugin, catalog, storage, source_db, namespace, table,
                     mode="append", renamed=False, source=False, snapshot_id=None,
                     sink_db=None, runner=False, execution_id=None,
                     storage_credential=None):
    source_reference = "fixture" if runner else "source-config"
    sink_reference = "fixture" if runner else "sink-config"
    storage_value = str(storage)
    storage_url = urllib.parse.urlsplit(storage_value)
    storage_connection = {
        "id": "storage",
        "driver": "s3" if storage_url.scheme in ("http", "https") else "file",
        "resource": storage_value,
    }
    if storage_credential:
        storage_connection["credentialReference"] = storage_credential
    connections = [
        {"id": "catalog", "driver": "http", "resource": catalog},
        storage_connection,
    ]
    if source:
        connections.append(sql_connection("sink", plugin, sink_db, sink_reference))
    else:
        connections.append(sql_connection("source", plugin, source_db, source_reference))

    if source:
        source_config = {
            "connection": "catalog",
            "storageConnection": "storage",
            "namespace": namespace,
            "table": table,
            "schema": source_schema(),
            "batchSize": 2,
        }
        if snapshot_id is not None:
            source_config["snapshotId"] = int(snapshot_id)
        components = [
            {"id": "source", "component": "iceberg.source", "config": source_config},
            {"id": "sink", "component": "sql.destination", "config": {
                "connection": "sink", "table": "sink_rows", "mode": "append", "provisionReceipts": True,
            }},
        ]
        paths = [{"from": "source", "output": 0, "to": "sink", "input": 0}]
    else:
        value_name = "label" if renamed else "value"
        query = ("SELECT id, value AS label, amount FROM source_rows ORDER BY id,rowid"
                 if renamed else "SELECT id, value, amount FROM source_rows ORDER BY id,rowid")
        destination_config = {
            "connection": "catalog", "storageConnection": "storage",
            "namespace": namespace, "table": table, "mode": mode,
        }
        if mode != "append":
            destination_config["keys"] = [0]
        components = [
            {"id": "source", "component": "sql.source", "config": {
                "connection": "source", "sql": query,
                "schema": schema(value_name), "batchSize": 2,
            }},
            {"id": "destination", "component": "iceberg.destination", "config": destination_config},
        ]
        paths = [{"from": "source", "output": 0, "to": "destination", "input": 0}]
    return {
        "version": PROTOCOL_VERSION,
        "id": execution_id or "iceberg-v2-" + uuid.uuid4().hex[:12],
        "connections": connections,
        "controlFlow": {
            "tasks": [{"id": "flow", "kind": "dataFlow", "config": {"pipeline": "flow"}}],
            "constraints": [],
        },
        "dataFlows": [{"id": "flow", "components": components, "paths": paths}],
    }


def write_cli_policy(root, plugin, catalog, storage, source_db=None, sink_db=None,
                     storage_credentials=None):
    root.mkdir(parents=True, exist_ok=True)
    source_config = root / "source-credential.txt"
    sink_config = root / "sink-credential.txt"
    if source_db is not None:
        source_config.write_text(str(source_db), encoding="utf-8")
    if sink_db is not None:
        sink_config.write_text(str(sink_db), encoding="utf-8")
    origin = urllib.parse.urlsplit(catalog)
    network_origin = f"{origin.scheme}://{origin.netloc}"
    credentials = {}
    if source_db is not None:
        credentials["source-config"] = str(source_config)
    if sink_db is not None:
        credentials["sink-config"] = str(sink_config)
    storage_value = str(storage)
    storage_url = urllib.parse.urlsplit(storage_value)
    if storage_credentials is not None:
        storage_config = root / "storage-credential.json"
        storage_config.write_text(json.dumps(storage_credentials), encoding="utf-8")
        credentials["storage-config"] = str(storage_config)
    file_roots = {
        *(str(db.parent) for db in (source_db, sink_db) if db is not None),
    }
    network_origins = [network_origin]
    if storage_url.scheme in ("http", "https"):
        network_origins.append(f"{storage_url.scheme}://{storage_url.netloc}")
    else:
        file_roots.add(storage_value)
    return {
        "dataDirectory": str(root / "state"),
        "fileRoots": sorted(file_roots),
        "pluginRoots": [str(Path(plugin).resolve().parent)],
        "networkOrigins": sorted(set(network_origins)),
        "credentialFiles": credentials,
    }


def run_cli(args, package, policy, state, execution_id, resume=False, expected_status=0):
    state.mkdir(parents=True, exist_ok=True)
    package_file = state.parent / (state.name + "-package.json")
    policy_file = state.parent / (state.name + "-policy.json")
    policy = dict(policy)
    policy["dataDirectory"] = str(state)
    package_file.write_text(json.dumps(package), encoding="utf-8")
    policy_file.write_text(json.dumps(policy), encoding="utf-8")
    command = [str(args.cli), "execute", str(policy_file), str(package_file), "{}", execution_id]
    if resume:
        command.append("resume")
    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    if expected_status and not result.stdout:
        diagnostic = json.loads(result.stderr)
        assert result.returncode == expected_status and diagnostic.get("code") == expected_status, (
            result.returncode, result.stdout, result.stderr)
        return {"result": expected_status}
    assert result.returncode == int(expected_status != 0), (result.returncode, result.stdout, result.stderr)
    facts = json.loads(result.stdout)
    assert facts.get("result") == expected_status, (facts, result.stderr)
    return facts


def run_observer(args, package, state, event_kind=0, event_count=1,
                 component="destination", resume=False, expect_crash=False, memory_limit=67108864):
    state.mkdir(parents=True, exist_ok=True)
    package_file = state.parent / "observer-package.json"
    package_file.write_text(json.dumps(package), encoding="utf-8")
    state.mkdir(parents=True, exist_ok=True)
    result = subprocess.run([
        str(args.runner), str(package_file), str(state), str(int(resume)), "2",
        str(-1 if resume else event_kind), str(event_count), component, str(memory_limit),
    ], capture_output=True, text=True, timeout=180)
    if expect_crash:
        assert result.returncode == 91, (result.returncode, result.stdout, result.stderr)
        return None
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    facts = json.loads(result.stdout)
    assert facts.get("result") == 0, (facts, result.stderr)
    return facts


def crash_boundary(args, root, plugin, catalog, storage, namespace, table, rows, after_commit):
    case = root / ("crash-after" if after_commit else "crash-before")
    case.mkdir()
    source_db = case / "source.db"
    create_source(source_db, rows)
    package = package_document(plugin, catalog, storage, source_db, namespace, table,
                               runner=True, execution_id="observer-" + table)
    state = case / "state"
    # The sink records intent, then fences the POST after writing data files,
    # then records the outcome after the Catalog responds.
    run_observer(args, package, state, event_kind=BEFORE_CHECKPOINT,
                 event_count=3 if after_commit else 2, expect_crash=True)
    before = catalog_state(catalog, namespace, table, storage,
                           args.catalog_user, args.catalog_password)
    if after_commit:
        assert before["snapshotIds"], before
    else:
        assert not before["snapshotIds"], before
        assert any((storage / namespace / table).rglob("*.parquet")), "Data file was not uploaded"
    run_observer(args, package, state, resume=True)
    after = catalog_state(catalog, namespace, table, storage,
                          args.catalog_user, args.catalog_password)
    assert after["snapshotIds"], after
    assert trino_rows(args, namespace, table) == rows, (table, trino_rows(args, namespace, table))
    if after_commit:
        assert len(after["snapshotIds"]) == len(before["snapshotIds"]), (before, after)


def resume_pinned_source(args, root, files, storage, namespace, table, expected):
    sink = files / "pinned-sink.db"
    create_sink(sink)
    package = package_document(args.sqlite, args.catalog, storage, None, namespace,
                               table, source=True, sink_db=sink, runner=True)
    state = root / "pinned-source-state"
    run_observer(args, package, state, event_kind=TARGET_COMMITTED, component="sink",
                 expect_crash=True)

    extra = files / "extra.db"
    appended = [(4, "delta", 40)]
    create_source(extra, appended)
    policy = write_cli_policy(root / "pinned-writer-policy", args.sqlite, args.catalog,
                              storage, source_db=extra)
    append = package_document(args.sqlite, args.catalog, storage, extra, namespace, table)
    run_cli(args, append, policy, root / "pinned-writer-state", "iceberg-append-during-recovery")
    assert trino_rows(args, namespace, table) == expected + appended

    run_observer(args, package, state, resume=True)
    actual = sqlite_rows(sink, "sink_rows")
    assert actual == expected, (actual, expected)


def reject_linked_table(args, root, policy, source_db, storage, namespace):
    outside = root / "outside-warehouse"
    outside.mkdir()
    table = "linked_table"
    link = storage / namespace / table
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        if os.name == "nt" and error.winerror == 1314:
            print("Windows fixture lacks permission to create a symlink for the linked-table case")
            return
        raise
    package = package_document(args.sqlite, args.catalog, storage, source_db, namespace, table)
    run_cli(args, package, policy, root / "linked-table-state", "iceberg-linked-table",
            expected_status=AUTH_DENIED)
    assert not list(outside.iterdir()), "Catalog wrote through an unauthorized warehouse link"


def load_external_catalog(raw):
    if not raw:
        return None, {}
    candidate = Path(raw)
    if candidate.is_file():
        text = candidate.read_text(encoding="utf-8").strip()
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return text, {}
        if isinstance(value, dict):
            return (value.get("url") or value.get("resource") or value.get("endpoint"), value)
    return raw, {}


def s3_case_resource(resource, namespace):
    parsed = urllib.parse.urlsplit(resource.rstrip("/"))
    if parsed.scheme != "https" or not parsed.netloc:
        raise AssertionError(
            "UD_TEST_ICEBERG_S3_RESOURCE must be an absolute HTTPS path-style URL"
        )
    path = parsed.path.strip("/")
    if "/" not in path:
        raise AssertionError(
            "UD_TEST_ICEBERG_S3_RESOURCE must contain bucket and optional prefix"
        )
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc,
                                    "/" + path + "/" + namespace,
                                    "", ""))


def s3_smoke(args, root, files, source_db, namespace, storage):
    """Run a focused real object-store write/read through Native and Trino."""
    credentials = dict(getattr(args, "s3_credentials", {}))
    credentials.update({
        "username": args.s3_access_key,
        "password": args.s3_secret_key,
        "awsSigV4": "aws:amz:" + args.s3_region + ":s3",
    })
    ca = None
    if args.s3_ca:
        ca = files / "storage-ca.pem"
        shutil.copyfile(args.s3_ca, ca)
        credentials["caConnection"] = "storage-ca"
    table = "orders_s3"
    for name in (namespace,):
        qualified = ".".join(quote_identifier(value) for value in
                               (args.trino_catalog, name))
        trino_query(args.trino, "CREATE SCHEMA IF NOT EXISTS " + qualified,
                    args.trino_catalog, "default", args.trino_user,
                    args.trino_password)
    policy = write_cli_policy(root / "s3-policy", args.sqlite, args.catalog,
                              storage, source_db=source_db,
                              storage_credentials=credentials)
    package = package_document(args.sqlite, args.catalog, storage, source_db,
                               namespace, table, storage_credential="storage-config")
    if ca:
        package["connections"].append(dict(id="storage-ca", driver="file", resource=str(ca)))
    run_cli(args, package, policy, root / "s3-state", "iceberg-s3-smoke")
    assert trino_rows(args, namespace, table) == [(1, "alpha", 10), (2, "beta", 20)]
    status, payload = http_json(catalog_endpoint(args.catalog, namespace, table),
                                user=args.catalog_user,
                                password=args.catalog_password)
    assert 200 <= status < 300, (status, payload)
    metadata_location = payload.get("metadata-location") or payload.get("metadataLocation")
    assert metadata_location and metadata_location.startswith("s3://"), payload

    sink = files / "s3-sink.db"
    create_sink(sink)
    source_package = package_document(
        args.sqlite, args.catalog, storage, None, namespace, table, source=True,
        sink_db=sink, storage_credential="storage-config")
    if ca:
        source_package["connections"].append(dict(id="storage-ca", driver="file", resource=str(ca)))
    source_policy = write_cli_policy(
        root / "s3-source-policy", args.sqlite, args.catalog, storage,
        sink_db=sink, storage_credentials=credentials)
    run_cli(args, source_package, source_policy, root / "s3-source-state",
            "iceberg-s3-source")
    assert sqlite_rows(sink, "sink_rows") == [(1, "alpha", 10), (2, "beta", 20)]
    print("Iceberg v2 Native real S3 smoke: SigV4 destination upload, REST snapshot "
          "visibility, Trino readback, and bounded Native S3 source read passed",
          flush=True)
    recovery_credentials = root / "s3-recovery-credentials.json"
    recovery_credentials.touch(mode=0o600)
    recovery_credentials.write_text(json.dumps(credentials), encoding="utf-8")
    for phase, event, count in (("before_catalog", BEFORE_CHECKPOINT, 2),
                                ("before_receipt", BEFORE_CHECKPOINT, 3),
                                ("multipart", AFTER_CHECKPOINT, 2)):
        s3_crash_boundary(args, root, namespace, storage, ca, recovery_credentials, phase, event, count)


def active_multipart_uploads(args, root, storage, table_location):
    endpoint = urllib.parse.urlsplit(storage)
    table = urllib.parse.urlsplit(table_location)
    query = "uploads=&" + urllib.parse.urlencode({"prefix": table.path.lstrip("/") + "/"})
    url = urllib.parse.urlunsplit((endpoint.scheme, endpoint.netloc, "/" + table.netloc, query, ""))
    curl_config = root / "multipart-inspection.curl"
    curl_config.touch(mode=0o600)
    curl_config.write_text("user = " + json.dumps(f"{args.s3_access_key}:{args.s3_secret_key}") + "\n",
                           encoding="utf-8")
    cmd = ["curl", "--fail", "--silent", "--show-error", "--max-time", "30", "--config", str(curl_config),
           "--aws-sigv4", "aws:amz:" + args.s3_region + ":s3"]
    if args.s3_ca:
        cmd += ["--cacert", str(args.s3_ca)]
    result = subprocess.run([*cmd, url], capture_output=True, check=True, timeout=35)
    return ET.fromstring(result.stdout).findall("{*}Upload")


def s3_crash_boundary(args, root, namespace, storage, ca, credentials, phase, event, count):
    case = root / phase
    case.mkdir()
    source = case / "source.db"
    if phase == "multipart":
        random_bytes = random.Random(11)
        rows = [(index, base64.b64encode(random_bytes.randbytes(96 * 1024)).decode("ascii"), 70)
                for index in range(128)]
    else:
        rows = [(7, "recovered", 70)]
    create_source(source, rows)
    table = "recovery_" + phase
    package = package_document(args.sqlite, args.catalog, storage, source, namespace, table,
                               runner=True, execution_id="iceberg-s3-" + phase,
                               storage_credential="file:" + str(credentials))
    if ca:
        package["connections"].append(dict(id="storage-ca", driver="file", resource=str(ca)))
    package["dataFlows"][0]["components"][0]["config"]["batchSize"] = len(rows)
    state = case / "state"
    memory_limit = 1024 * 1024 * 1024
    run_observer(args, package, state, event_kind=event, event_count=count, expect_crash=True,
                 memory_limit=memory_limit)
    before = catalog_state(args.catalog, namespace, table, storage, args.catalog_user, args.catalog_password)
    assert len(before["snapshotIds"]) == int(phase == "before_receipt"), (phase, before)
    location = before["metadata"]["location"]
    if phase == "multipart":
        assert len(active_multipart_uploads(args, root, storage, location)) == 1, location
    facts = run_observer(args, package, state, resume=True, memory_limit=memory_limit)
    assert facts["committed"] == facts["acknowledged"] == len(rows), (phase, facts)
    after = catalog_state(args.catalog, namespace, table, storage, args.catalog_user, args.catalog_password)
    assert len(after["snapshotIds"]) == 1, (phase, after)
    if before["snapshotIds"]:
        assert after["snapshotIds"] == before["snapshotIds"], (phase, before, after)
    summary = after["metadata"]["snapshots"][0]["summary"]
    assert len(summary["universaldata.commit.id"]) == len(summary["universaldata.commit.digest"]) == 64, summary
    if phase == "multipart":
        assert not active_multipart_uploads(args, root, storage, location), location
    name = ".".join(quote_identifier(part) for part in (args.trino_catalog, namespace, table))
    observed = trino_query(args.trino, f'SELECT id, lower(to_hex(sha256(to_utf8("value")))), amount FROM {name} ORDER BY id',
                           args.trino_catalog, namespace, args.trino_user, args.trino_password)
    expected = [[index, hashlib.sha256(value.encode()).hexdigest(), amount] for index, value, amount in rows]
    assert observed == expected, (phase, observed)
    print(f"Iceberg S3 recovery at {phase}: one snapshot, {len(rows)} acknowledged rows, exact Trino readback", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--sqlite", type=Path, required=True)
    parser.add_argument("--runner", type=Path,
                        default=Path(os.environ["UD_TEST_ICEBERG_RUNNER"])
                        if os.environ.get("UD_TEST_ICEBERG_RUNNER") else None)
    parser.add_argument("--catalog", default=os.environ.get("UD_TEST_ICEBERG_CATALOG"))
    parser.add_argument("--trino", default=os.environ.get("UD_TEST_TRINO_URL"))
    parser.add_argument("--storage", type=Path,
                        default=Path(os.environ["UD_TEST_ICEBERG_STORAGE"])
                        if os.environ.get("UD_TEST_ICEBERG_STORAGE") else None)
    parser.add_argument("--namespace", default=os.environ.get("UD_TEST_ICEBERG_NAMESPACE"))
    parser.add_argument("--trino-catalog", default=os.environ.get("UD_TEST_TRINO_CATALOG", "iceberg"))
    parser.add_argument("--trino-user", default=os.environ.get("UD_TEST_TRINO_USER", "ud-iceberg-v2"))
    parser.add_argument("--trino-password", default=os.environ.get("UD_TEST_TRINO_PASSWORD"))
    parser.add_argument("--catalog-user", default=os.environ.get("UD_TEST_ICEBERG_USER"))
    parser.add_argument("--catalog-password", default=os.environ.get("UD_TEST_ICEBERG_PASSWORD"))
    parser.add_argument("--s3-resource", default=os.environ.get("UD_TEST_ICEBERG_S3_RESOURCE"))
    parser.add_argument("--s3-ca", type=Path, default=os.environ.get("UD_TEST_ICEBERG_S3_CA"))
    storage_mode = parser.add_mutually_exclusive_group()
    storage_mode.add_argument("--filesystem-only", action="store_true")
    storage_mode.add_argument("--s3-only", action="store_true")
    parser.add_argument("--s3-region", default=os.environ.get("UD_TEST_ICEBERG_S3_REGION"))
    parser.add_argument("--s3-access-key",
                        default=os.environ.get("UD_TEST_ICEBERG_S3_ACCESS_KEY"))
    parser.add_argument("--s3-secret-key",
                        default=os.environ.get("UD_TEST_ICEBERG_S3_SECRET_KEY"))
    args = parser.parse_args()
    if args.filesystem_only:
        args.s3_resource = None
        args.s3_ca = None
    if args.s3_only and not args.s3_resource:
        print("Blocked: UD_TEST_ICEBERG_S3_RESOURCE is unavailable")
        return 77
    if args.s3_resource:
        args.catalog = os.environ.get("UD_TEST_ICEBERG_S3_CATALOG") or args.catalog
        args.trino = os.environ.get("UD_TEST_ICEBERG_S3_TRINO_URL") or args.trino

    credential_file = os.environ.get("UD_TEST_ICEBERG_S3_CREDENTIALS") if args.s3_resource else None
    args.s3_credentials = {}
    if credential_file:
        credential_path = Path(credential_file)
        if not credential_path.is_file():
            print("Blocked: UD_TEST_ICEBERG_S3_CREDENTIALS is unavailable")
            return 77
        credentials = json.loads(credential_path.read_text(encoding="utf-8"))
        if not isinstance(credentials, dict):
            print("Blocked: UD_TEST_ICEBERG_S3_CREDENTIALS must contain an object")
            return 77
        args.s3_credentials = {
            key: credentials[key]
            for key in ("username", "password", "awsSigV4", "headers", "caConnection")
            if key in credentials
        }
        args.s3_access_key = args.s3_access_key or credentials.get("username")
        args.s3_secret_key = args.s3_secret_key or credentials.get("password")
        sigv4 = credentials.get("awsSigV4", "")
        if not args.s3_region and isinstance(sigv4, str) and sigv4.startswith("aws:amz:"):
            args.s3_region = sigv4[8:].split(":", 1)[0]

    catalog, catalog_config = load_external_catalog(args.catalog)
    if not catalog:
        print("Blocked: UD_TEST_ICEBERG_CATALOG is unavailable")
        return 77
    if not args.trino:
        print("Blocked: UD_TEST_TRINO_URL is unavailable")
        return 77
    if not args.storage and not args.s3_resource:
        print("Blocked: UD_TEST_ICEBERG_STORAGE is unavailable")
        return 77
    if args.s3_resource and not all((args.s3_region, args.s3_access_key, args.s3_secret_key)):
        print("Blocked: S3 resource requires region, access key, and secret key")
        return 77
    if args.s3_resource and not shutil.which("curl"):
        print("Blocked: curl is required to independently inspect S3 multipart uploads")
        return 77
    if args.s3_ca and not args.s3_ca.is_file():
        print("Blocked: the configured S3 CA file is unavailable")
        return 77
    if not args.cli.is_file() or not args.sqlite.is_file():
        print("Blocked: CLI or SQLite Native connector is unavailable")
        return 77
    if not args.runner or not args.runner.is_file():
        print("Blocked: Native observer runner is unavailable")
        return 77
    args.catalog_user = args.catalog_user or catalog_config.get("user")
    args.catalog_password = args.catalog_password or catalog_config.get("password")
    args.namespace = args.namespace or ("ud_iceberg_v2_" + uuid.uuid4().hex[:12])
    args.catalog = catalog
    args.sqlite = args.sqlite.resolve()
    args.cli = args.cli.resolve()
    args.runner = args.runner.resolve()
    if args.storage:
        args.storage = args.storage.resolve()
        args.storage.mkdir(parents=True, exist_ok=True)
    storage_case = None
    if not args.s3_resource:
        storage_case = args.storage / ("ud-iceberg-v2-" + uuid.uuid4().hex)
        storage_case.mkdir()

    try:
        with tempfile.TemporaryDirectory(prefix="ud-iceberg-v2-") as temporary:
            root = Path(temporary)
            files = root / "files"
            files.mkdir()
            source_db = files / "source.db"
            create_source(source_db, [(1, "alpha", 10), (2, "beta", 20)])
            if args.s3_resource:
                storage = s3_case_resource(args.s3_resource, args.namespace)
                s3_smoke(args, root, files, source_db, args.namespace, storage)
                return 0
            policy = write_cli_policy(root, args.sqlite, args.catalog, storage_case,
                                      source_db=source_db)
            table = "orders"
            namespace = args.namespace
            crash_namespace = namespace + "_crash"
            for name in (namespace, crash_namespace + "_before", crash_namespace + "_after"):
                qualified = ".".join(quote_identifier(value) for value in (args.trino_catalog, name))
                trino_query(args.trino, "CREATE SCHEMA IF NOT EXISTS " + qualified, args.trino_catalog,
                            "default", args.trino_user, args.trino_password)
            append = package_document(args.sqlite, args.catalog, storage_case, source_db,
                                      namespace, table, mode="append")
            run_cli(args, append, policy, root / "state-append", "iceberg-append")
            first = catalog_state(args.catalog, namespace, table, storage_case,
                                  args.catalog_user, args.catalog_password)
            assert first["snapshotIds"], first
            assert trino_rows(args, namespace, table) == [(1, "alpha", 10), (2, "beta", 20)]
            first_snapshot = first["currentSnapshotId"]
            reject_linked_table(args, root, policy, source_db, storage_case, namespace)

            with closing(sqlite3.connect(source_db)) as database, database:
                database.execute("DELETE FROM source_rows")
                database.executemany("INSERT INTO source_rows VALUES(?,?,?)", [
                    (1, "alpha-superseded", 99), (1, "alpha-updated", 100),
                    (2, "beta", 20), (3, "gamma", 30),
                ])
            upsert = package_document(args.sqlite, args.catalog, storage_case, source_db,
                                      namespace, table, mode="upsert", renamed=True)
            run_cli(args, upsert, policy, root / "state-upsert", "iceberg-upsert")
            second = catalog_state(args.catalog, namespace, table, storage_case,
                                   args.catalog_user, args.catalog_password)
            assert set(first["snapshotIds"]).issubset(second["snapshotIds"]), (first, second)
            assert len(second["snapshotIds"]) > len(first["snapshotIds"]), (first, second)
            expected = [(1, "alpha-updated", 100), (2, "beta", 20), (3, "gamma", 30)]
            observed = trino_rows(args, namespace, table)
            assert observed == expected, (observed, expected)

            sink_current = files / "sink-current.db"
            create_sink(sink_current)
            current_source = package_document(args.sqlite, args.catalog, storage_case, None,
                                              namespace, table, source=True, sink_db=sink_current)
            current_policy = write_cli_policy(root / "current-policy", args.sqlite, args.catalog,
                                              storage_case, sink_db=sink_current)
            (root / "current-policy" / "state").mkdir(parents=True)
            run_cli(args, current_source, current_policy, root / "current-state", "iceberg-source-current")
            assert sqlite_rows(sink_current, "sink_rows") == expected

            sink_history = files / "sink-history.db"
            create_sink(sink_history)
            historical_source = package_document(args.sqlite, args.catalog, storage_case, None,
                                                 namespace, table, source=True,
                                                 snapshot_id=first_snapshot, sink_db=sink_history)
            history_policy = write_cli_policy(root / "history-policy", args.sqlite, args.catalog,
                                              storage_case, sink_db=sink_history)
            (root / "history-policy" / "state").mkdir(parents=True)
            run_cli(args, historical_source, history_policy, root / "history-state",
                    "iceberg-source-history")
            assert sqlite_rows(sink_history, "sink_rows") == [(1, "alpha", 10), (2, "beta", 20)]
            resume_pinned_source(args, root, files, storage_case, namespace, table, expected)
            partitioned_regression(args, root, args.sqlite, args.catalog, storage_case, namespace)

            crash_boundary(args, root, args.sqlite, args.catalog, storage_case,
                           crash_namespace + "_before", "append_before", [(1, "one", 1), (2, "two", 2)],
                           after_commit=False)
            crash_boundary(args, root, args.sqlite, args.catalog, storage_case,
                           crash_namespace + "_after", "append_after", [(1, "one", 1), (2, "two", 2)],
                           after_commit=True)
            print("Iceberg v2 Native REST: SQLite SQL source append, repeated-key upsert with field-ID rename, "
                  "Trino current rows, Catalog history, current/historical/pinned Native source reads, "
                  "and recovery after the upload and before the local commit receipt passed")
            return 0
    finally:
        if storage_case is not None:
            shutil.rmtree(storage_case, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
