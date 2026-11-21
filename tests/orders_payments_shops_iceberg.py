#!/usr/bin/env python3
"""Bounded real three-source acceptance gate.

This is intentionally an external-resource test, not a SQLite regression.  A
run is enabled only when it has a disposable PostgreSQL connection (logical
CDC over verified TLS), a disposable TLS MySQL connection, a real Kafka topic
and an Iceberg REST/S3/Trino fixture. The only SQLite file is the
independent SQL destination used to inspect Native receipts.

The source fixture is deliberately small and deterministic:

* PostgreSQL CDC inserts order ``(1, 7, 1000000)``;
* Kafka delivers payment ``+100``, an earlier-event-time refund ``-30``, and
  a later duplicate of the payment;
* TLS MySQL supplies shop ``(7, shop-seven)``.

The expected final window is exactly ``(1000000, 1001000, 7, 70)``.  This
gate does not claim CDC update/delete retractions or historical/as-of MySQL
semantics: the current event-window/cache contracts are insert-only and the
shop lookup is a current cache snapshot.
"""

import argparse
import atexit
import copy
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid

from product_version import PROTOCOL_VERSION

from iceberg_v2 import load_external_catalog, s3_case_resource
from orders_fixture_support import command, mysql_identifier, quote_identifier
from orders_kafka_fixture import KafkaFixture
from orders_mysql_fixture import MySqlFixture
from orders_postgres_fixture import PostgresFixture


SKIP = 77
ORDER_TIME = 1_000_000
WINDOW_SIZE = 1_000
EXPECTED = (ORDER_TIME, ORDER_TIME + WINDOW_SIZE, 7, 70)
PAYMENTS = (
    {"event_id": "payment-1", "order_id": 1, "event_time": 1_000_100,
     "net_amount": 100, "kind": "payment"},
    {"event_id": "refund-1", "order_id": 1, "event_time": 1_000_050,
     "net_amount": -30, "kind": "refund"},
    # Same event_id is intentionally delivered after the refund and must be
    # removed by eventDeduplicate rather than counted twice.
    {"event_id": "payment-1", "order_id": 1, "event_time": 1_000_120,
     "net_amount": 100, "kind": "payment"},
)


def field(name, kind, nullable=False, key_order=0):
    value = {"name": name, "type": kind, "nullable": nullable}
    if key_order:
        value["keyOrder"] = key_order
    return value


def schema(fields):
    return {"version": PROTOCOL_VERSION, "fields": fields}


ORDER_SCHEMA = schema([
    field("order_id", "int64", key_order=1), field("shop_id", "int64"),
    field("order_time", "int64"),
])
PAYMENT_SCHEMA = schema([
    field("event_id", "utf8"), field("order_id", "int64"),
    field("event_time", "int64"), field("net_amount", "int64"),
    field("kind", "utf8"),
])
SHOP_SCHEMA = schema([field("shop_id", "int64"), field("shop_name", "utf8")])


def edge(source, target, output=0, input=0):
    return {"from": source, "output": output, "to": target, "input": input}


def node(identifier, component, **config):
    return {"id": identifier, "component": component, "config": config}


def qname(*parts):
    return ".".join(quote_identifier(part) for part in parts)


def package_document(args, pg, mysql, kafka, target, spill, namespace, table):
    """Build the actual mixed-source Native package.

    No Kafka transaction delivery is requested here: the documented mixed
    PG/Kafka/SQL topology uses the local Journal/at-least-once boundary.  A
    Kafka stage transaction would be a different topology and would conceal
    whether PG CDC and SQL/Iceberg shared the same commit.
    """
    connections = [
        {"id": "orders", "driver": str(args.postgres_plugin),
         # Keep the identity namespace identical to the discovery package;
         # the conninfo is still a temporary TLS fixture credential and is
         # written only inside this run's private package directory.
         "resource": pg.conninfo,
         "credentialReference": "file:" + str(pg.config_path)},
        {"id": "shops", "driver": str(args.mysql_plugin),
         "resource": "orders-shops-mysql", "credentialReference": "file:" + str(mysql.config)},
        {"id": "payments", "driver": str(args.kafka_plugin),
         "resource": "orders-payments-kafka", "credentialReference": "file:" + str(kafka.credential_path)},
        {"id": "target", "driver": str(args.sqlite), "resource": str(target),
         "credentialReference": "fixture"},
        {"id": "spill", "driver": "file", "resource": str(spill)},
        {"id": "catalog", "driver": "http", "resource": args.catalog},
        {"id": "warehouse", "driver": "s3", "resource": s3_case_resource(args.s3_resource, namespace),
         "credentialReference": "file:" + str(args.s3_credentials)},
    ]
    if args.catalog_credentials is not None:
        connections[-2]["credentialReference"] = "file:" + str(args.catalog_credentials)
    if args.s3_ca:
        connections.append(dict(id="storage-ca", driver="file", resource=str(args.s3_ca)))

    build_components = [
        node("shopSource", "sql.source", connection="shops", table=mysql.table,
             sql="SELECT shop_id, shop_name FROM %s" % mysql_identifier(mysql.table),
             schema=SHOP_SCHEMA, batchSize=16,
             schemaPolicy={"policy": "explicitMapping", "mapping": [
                 {"expected": "shop_id", "actual": "shop_id"},
                 {"expected": "shop_name", "actual": "shop_name"},
             ]}),
        node("shopCache", "cacheBuild", cache="shops", mapping=[0, 1]),
    ]
    metric_components = [
        node("orders", "postgresql.cdc.source", connection="orders", table=pg.table,
             publication=pg.publication, slot=pg.slot, maxTransactions=1,
             mode="snapshotAndStream", appendOnly=True, schema=ORDER_SCHEMA),
        node("orderTime", "eventTime", field="order_time", lagMicros=86_400_000_000),
        node("payments", "mq.source", connection="payments", targetType="topic",
             address=kafka.topic, format="json", group=kafka.group,
             initialPosition="earliest", partition=0,
             endOffsets=[{"partition": 0, "nextOffset": kafka.end_offset}],
             prefetch=16, maxMessageBytes=65536, maxInFlightBytes=262144,
             schema=PAYMENT_SCHEMA),
        node("paymentTime", "eventTime", field="event_time", lagMicros=86_400_000_000),
        node("deduplicate", "eventDeduplicate", keys=["event_id"], timeField="event_time",
             ttlMicros=86_400_000_000, stateBytes=65536),
        node("deduplicatedTime", "eventTime", field="event_time", lagMicros=86_400_000_000),
        node("join", "eventJoin", leftKeys=["order_id"], rightKeys=["order_id"],
             leftTimeField="order_time", rightTimeField="event_time", withinMicros=1000,
             stateBytes=65536),
        node("shops", "cacheLookup", cache="shops", keys=[1], errorPolicy="fail"),
        node("lookupTime", "eventTime", field="event_time", lagMicros=86_400_000_000),
        node("window", "eventWindow", mode="tumbling", timeField="event_time",
             sizeMicros=1000, allowedLatenessMicros=0, latePolicy="fail", emit="final",
             plan={"version": PROTOCOL_VERSION, "group": ["shop_id"],
                   "aggregates": [{"function": "sum", "column": "net_amount",
                                   "name": "net_total"}]}),
        node("fanout", "multicast"),
        node("sqlMetric", "sql.destination", connection="target", table="metric",
             mode="upsert", keys=[0, 1, 2], provisionReceipts=True),
        node("icebergMetric", "iceberg.destination", connection="catalog",
             storageConnection="warehouse", namespace=namespace, table=table,
             mode="upsert", keys=[0, 1, 2]),
    ]
    metric_paths = [
        edge("orders", "orderTime"), edge("payments", "paymentTime"),
        edge("paymentTime", "deduplicate"), edge("deduplicate", "deduplicatedTime"),
        edge("orderTime", "join", input=0), edge("deduplicatedTime", "join", input=1),
        edge("join", "shops"), edge("shops", "lookupTime"), edge("lookupTime", "window"),
        edge("window", "fanout"), edge("fanout", "sqlMetric", output=0),
        edge("fanout", "icebergMetric", output=1),
    ]
    return {
        "version": PROTOCOL_VERSION,
        "id": "orders-payments-shops-" + uuid.uuid4().hex[:12],
        "connections": connections,
        "caches": [{"id": "shops", "sourceConnection": "shops", "schema": SHOP_SCHEMA,
                    "keys": [0], "scope": "execution"}],
        "policy": {"buffers": {"connection": "spill", "residentBytes": 4096,
                                  "workingBytes": 16 * 1024 * 1024,
                                  "spillBytes": 64 * 1024 * 1024, "maxBlocks": 1024}},
        "controlFlow": {"tasks": [
            {"id": "buildShops", "kind": "dataFlow", "config": {"pipeline": "buildShops"}},
            {"id": "metric", "kind": "dataFlow", "config": {"pipeline": "metric"}},
        ], "constraints": [{"from": "buildShops", "to": "metric", "on": "success"}]},
        "dataFlows": [
            {"id": "buildShops", "components": build_components,
             "paths": [edge("shopSource", "shopCache")]},
            {"id": "metric", "components": metric_components, "paths": metric_paths},
        ],
    }


def create_target(path):
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("CREATE TABLE metric (_window_start INTEGER NOT NULL, "
                         "_window_end INTEGER NOT NULL, shop_id INTEGER NOT NULL, "
                         "net_total INTEGER, PRIMARY KEY (_window_start, _window_end, shop_id))")


def sql_rows(path):
    with closing(sqlite3.connect(path)) as database:
        return database.execute("SELECT _window_start, _window_end, shop_id, net_total "
                                "FROM metric ORDER BY 1, 2, 3").fetchall()


def receipt_count(path):
    with closing(sqlite3.connect(path)) as database:
        names = database.execute("SELECT name FROM sqlite_master WHERE type='table' AND "
                                 "name='ud_sdk_receipts_v11'").fetchall()
        if not names:
            raise AssertionError("SQL destination did not provision receipts")
        return database.execute("SELECT count(*) FROM ud_sdk_receipts_v11").fetchone()[0]


def metric_node(facts, identifier):
    for pipeline in facts.get("diagnostics", {}).get("pipelines", []):
        if pipeline.get("id") != "metric":
            continue
        for tree in pipeline.get("trees", []):
            for item in tree.get("nodes", []):
                if item.get("id") == identifier:
                    return item
    raise AssertionError("missing Native diagnostic node " + identifier)


def run_native(runner, package, state, resume=False):
    state.mkdir(parents=True, exist_ok=True)
    result = command([runner, package, state, int(resume), 2], timeout=180)
    if result.returncode:
        raise AssertionError("Native runner failed (%s):\n%s\n%s" %
                             (result.returncode, result.stdout, result.stderr))
    facts = json.loads(result.stdout)
    if facts.get("result") != 0:
        raise AssertionError((facts, result.stderr))
    return facts


def discover_postgres_schema(discoverer, package, pg, root):
    """Resolve Native's provider/physical identities before opening CDC."""
    discovery = copy.deepcopy(package)
    for connection in discovery["connections"]:
        connection["credentialReference"] = "fixture"
    source = next(connection for connection in discovery["connections"]
                  if connection["id"] == "orders")
    source["resource"] = pg.conninfo
    discovery_file = root / "postgres-discovery-package.json"
    schema_file = root / "postgres-discovered-schema.json"
    discovery_file.write_text(json.dumps(discovery), encoding="utf-8")
    command([discoverer, "--discover", discovery_file, "orders", pg.table,
             schema_file], timeout=120)
    try:
        discovered = json.loads(schema_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AssertionError("PostgreSQL schema discovery produced no JSON: %s" % error) from error
    fields = discovered.get("fields")
    if discovered.get("version") != PROTOCOL_VERSION or not isinstance(fields, list) or \
            len(fields) != len(ORDER_SCHEMA["fields"]):
        raise AssertionError("unexpected PostgreSQL discovery schema: %s" % discovered)
    return discovered


def http_json(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"Accept": "application/json",
                                              "Content-Type": "application/json"})
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=45) as response:
            raw = response.read()
            return response.status, json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as error:
        raise AssertionError("HTTP %s %s failed (%s): %s" %
                             (method, url, error.code,
                              error.read().decode("utf-8", errors="replace"))) from error


def trino_query(url, catalog, namespace, statement, user):
    endpoint = url.rstrip("/")
    if not endpoint.endswith("/v1/statement"):
        endpoint += "/v1/statement"
    headers = {"Content-Type": "text/plain; charset=utf-8", "X-Trino-User": user,
               "X-Trino-Catalog": catalog, "X-Trino-Schema": namespace}
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(endpoint, data=statement.encode("utf-8"),
                                     method="POST", headers=headers)
    with opener.open(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rows = list(payload.get("data") or [])
    while payload.get("nextUri"):
        with opener.open(urllib.request.Request(payload["nextUri"], method="GET",
                                                headers=headers), timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("error"):
            raise AssertionError(payload["error"])
        rows.extend(payload.get("data") or [])
    if payload.get("error"):
        raise AssertionError(payload["error"])
    return [tuple(row) for row in rows]


def create_trino_schema(url, catalog, namespace, user):
    trino_query(url, catalog, namespace,
                "CREATE SCHEMA %s" % quote_identifier(namespace), user)


def iceberg_rows(url, catalog, namespace, table, user):
    qualified = qname(catalog, namespace, table)
    return trino_query(url, catalog, namespace,
                       "SELECT _window_start, _window_end, shop_id, net_total FROM %s "
                       "ORDER BY 1, 2, 3" % qualified, user)


def iceberg_metadata(catalog_url, namespace, table):
    endpoint = catalog_url.rstrip("/")
    if not endpoint.endswith("/v1"):
        endpoint += "/v1"
    endpoint += "/namespaces/%s/tables/%s" % (
        urllib.parse.quote(namespace, safe=""), urllib.parse.quote(table, safe=""))
    _, payload = http_json(endpoint)
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        location = payload.get("metadata-location") or payload.get("metadataLocation")
        if not location:
            raise AssertionError("Iceberg REST table has no metadata location: %s" % payload)
        parsed = urllib.parse.urlsplit(location)
        if parsed.scheme in ("http", "https"):
            _, metadata = http_json(location)
        else:
            metadata = json.loads(Path(urllib.parse.unquote(parsed.path or location)).read_text())
    if metadata.get("format-version") != 2 or not metadata.get("snapshots"):
        raise AssertionError("Iceberg table did not commit a v2 snapshot: %s" % metadata)
    return metadata


def cleanup_iceberg_table(catalog_url, namespace, table):
    """Remove only this run's table/namespace if the REST fixture is alive."""
    base = catalog_url.rstrip("/")
    if not base.endswith("/v1"):
        base += "/v1"
    table_url = base + "/namespaces/%s/tables/%s" % (
        urllib.parse.quote(namespace, safe=""), urllib.parse.quote(table, safe=""))
    namespace_url = base + "/namespaces/%s" % urllib.parse.quote(namespace, safe="")
    for endpoint in (table_url, namespace_url):
        try:
            http_json(endpoint, method="DELETE")
        except Exception:
            # A coordinator may already be down; cleanup must never mask the
            # actual gate result and the identifiers are unique to this run.
            pass


def required_args(args):
    missing = []
    for label, path in (("runner", args.runner), ("discoverer", args.discoverer),
                        ("sqlite", args.sqlite),
                        ("postgres plugin", args.postgres_plugin),
                        ("mysql helper", args.mysql_helper),
                        ("mysql plugin", args.mysql_plugin),
                        ("kafka plugin", args.kafka_plugin)):
        if not path.is_file():
            missing.append(label + "=" + str(path))
    if not args.mysql_config or not Path(args.mysql_config).is_file():
        missing.append("UD_TEST_ORDERS_MYSQL_CONFIG")
    if not args.pg_config or not Path(args.pg_config).is_file():
        missing.append("UD_TEST_ORDERS_PG_CONFIG")
    if not args.kafka_config or not Path(args.kafka_config).is_file():
        missing.append("UD_TEST_ORDERS_KAFKA_CONFIG")
    if not args.catalog:
        missing.append("UD_TEST_ICEBERG_CATALOG")
    if not args.trino:
        missing.append("UD_TEST_TRINO_URL")
    if not args.s3_resource:
        missing.append("UD_TEST_ICEBERG_S3_RESOURCE")
    if not args.s3_credentials or not args.s3_credentials.is_file():
        missing.append("UD_TEST_ICEBERG_S3_CREDENTIALS")
    if args.s3_ca and not args.s3_ca.is_file():
        missing.append("UD_TEST_ICEBERG_S3_CA")
    return missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--discoverer", type=Path)
    parser.add_argument("--postgres-plugin", type=Path, required=True)
    parser.add_argument("--mysql-helper", type=Path, required=True)
    parser.add_argument("--mysql-plugin", type=Path, required=True)
    parser.add_argument("--kafka-plugin", type=Path, required=True)
    parser.add_argument("--sqlite", type=Path, required=True)
    parser.add_argument("--pg-config", type=Path,
                        default=Path(os.environ["UD_TEST_ORDERS_PG_CONFIG"])
                        if os.environ.get("UD_TEST_ORDERS_PG_CONFIG") else None)
    parser.add_argument("--mysql-config", type=Path,
                        default=Path(os.environ["UD_TEST_ORDERS_MYSQL_CONFIG"])
                        if os.environ.get("UD_TEST_ORDERS_MYSQL_CONFIG") else None)
    parser.add_argument("--kafka-config", type=Path,
                        default=Path(os.environ["UD_TEST_ORDERS_KAFKA_CONFIG"])
                        if os.environ.get("UD_TEST_ORDERS_KAFKA_CONFIG") else None)
    parser.add_argument("--catalog", default=os.environ.get("UD_TEST_ICEBERG_S3_CATALOG") or
                        os.environ.get("UD_TEST_ICEBERG_CATALOG"))
    parser.add_argument("--trino", default=os.environ.get("UD_TEST_ICEBERG_S3_TRINO_URL") or
                        os.environ.get("UD_TEST_TRINO_URL"))
    parser.add_argument("--trino-user", default=os.environ.get("UD_TEST_TRINO_USER",
                                                                 "ud-orders-payments"))
    parser.add_argument("--s3-resource", default=os.environ.get("UD_TEST_ICEBERG_S3_RESOURCE"))
    parser.add_argument("--s3-credentials", type=Path, default=os.environ.get("UD_TEST_ICEBERG_S3_CREDENTIALS"))
    parser.add_argument("--s3-ca", type=Path, default=os.environ.get("UD_TEST_ICEBERG_S3_CA"))
    args = parser.parse_args()
    if args.discoverer is None:
        args.discoverer = args.runner.parent / "test_next_phase_queries"
    for name in ("runner", "discoverer", "postgres_plugin", "mysql_helper", "mysql_plugin",
                 "kafka_plugin", "sqlite", "pg_config", "mysql_config",
                 "kafka_config", "s3_credentials", "s3_ca"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.resolve())
    args.catalog, catalog_configuration = load_external_catalog(args.catalog)
    missing = required_args(args)
    if missing:
        print("Blocked real three-source gate; missing: " + ", ".join(missing), flush=True)
        print("Supported contracts remain separate: insert-only PG CDC, Kafka JSON offsets, "
              "TLS MySQL current cache, SQL receipts, and Iceberg REST/Trino are not claimed "
              "without all resources.", flush=True)
        return SKIP

    namespace = "ud_orders_payments_" + uuid.uuid4().hex[:10]
    table = "net_metric"
    print(json.dumps({"postgres": "TLS logical CDC", "mysql": "TLS current snapshot",
                      "kafka": "real JSON topic with bounded end offset",
                      "iceberg": args.catalog, "trino": args.trino,
                      "namespace": namespace, "table": table}, sort_keys=True), flush=True)

    with tempfile.TemporaryDirectory(prefix="ud-orders-payments-shops-") as folder:
        root = Path(folder)
        target = root / "sql-target.db"
        spill = root / "spill"
        state = root / "state"
        if args.s3_ca:
            s3_settings = json.loads(args.s3_credentials.read_text(encoding="utf-8"))
            s3_settings["caConnection"] = "storage-ca"
            args.s3_credentials = root / "s3-credentials.json"
            args.s3_credentials.write_text(json.dumps(s3_settings), encoding="utf-8")
            args.s3_credentials.chmod(0o600)
        args.catalog_credentials = None
        if catalog_configuration.get("user") and catalog_configuration.get("password"):
            args.catalog_credentials = root / "iceberg-catalog-credentials"
            args.catalog_credentials.write_text(json.dumps({"username": catalog_configuration["user"],
                "password": catalog_configuration["password"]}), encoding="utf-8")
            args.catalog_credentials.chmod(0o600)
        spill.mkdir()
        create_target(target)
        if sum(item["net_amount"] for item in PAYMENTS[:2]) != EXPECTED[-1]:
            raise AssertionError("fixture oracle no longer describes net 70")
        # Every fixture creates only its tagged objects and removes them in its
        # context manager.  Provisioning is intentionally after all static
        # resource checks above.
        with PostgresFixture(root, args.pg_config, ORDER_TIME) as pg, \
                MySqlFixture(root, args.mysql_config, args.mysql_helper, args.mysql_plugin) as mysql, \
                KafkaFixture(root, args.kafka_config, PAYMENTS) as kafka:
            create_trino_schema(args.trino, os.environ.get("UD_TEST_TRINO_CATALOG", "iceberg"),
                                namespace, args.trino_user)
            atexit.register(cleanup_iceberg_table, args.catalog, namespace, table)
            package = package_document(args, pg, mysql, kafka, target, spill, namespace, table)
            discovery_package = copy.deepcopy(package)
            for connection in discovery_package["connections"]:
                connection["credentialReference"] = "fixture"
            source = next(connection for connection in discovery_package["connections"]
                          if connection["id"] == "orders")
            source["resource"] = pg.conninfo
            package["dataFlows"][1]["components"][0]["config"]["schema"] = \
                discover_postgres_schema(args.discoverer, discovery_package, pg, root)
            package_file = root / "package.json"
            package_file.write_text(json.dumps(package), encoding="utf-8")
            facts = run_native(args.runner, package_file, state)
            actual = sql_rows(target)
            if actual != [EXPECTED]:
                raise AssertionError("SQL exact net gate mismatch: %s" % (actual,))
            receipts = receipt_count(target)
            if receipts <= 0:
                raise AssertionError("SQL receipt table is empty")
            if metric_node(facts, "deduplicate").get("producedRows") != 2:
                raise AssertionError("duplicate payment was not removed")
            if metric_node(facts, "join").get("producedRows") != 2:
                raise AssertionError("PG CDC order did not join both payment/refund rows")
            if metric_node(facts, "window").get("producedRows") != 1:
                raise AssertionError("final window did not emit one result")
            # The same package/execution state must not publish a second SQL
            # row or duplicate Iceberg snapshot when resumed at the committed
            # Native epoch.
            run_native(args.runner, package_file, state, resume=True)
            if sql_rows(target) != [EXPECTED]:
                raise AssertionError("resume changed the exact SQL result")
            if receipt_count(target) != receipts:
                raise AssertionError("resume changed the SQL receipts")
            iceberg = iceberg_rows(args.trino,
                                   os.environ.get("UD_TEST_TRINO_CATALOG", "iceberg"),
                                   namespace, table, args.trino_user)
            if iceberg != [EXPECTED]:
                raise AssertionError("Trino Iceberg readback mismatch: %s" % (iceberg,))
            metadata = iceberg_metadata(args.catalog, namespace, table)
            if not metadata.get("location", "").startswith("s3://"):
                raise AssertionError("Expected table location in S3")
            if len(metadata["snapshots"]) != 1:
                raise AssertionError("resume did not preserve a single Iceberg snapshot")
            print("REAL PG CDC + Kafka + TLS MySQL -> SQL/Iceberg gate passed: "
                  "dedup=2, joined=2, final window=1, exact net=70, SQL receipts>0, "
                  "Trino rows=1, Iceberg snapshots=%d" % len(metadata["snapshots"]), flush=True)
            print("Unsupported contracts intentionally unclaimed: CDC update/delete "
                  "retractions and historical/as-of shop lookup.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
