"""Exercise host-registered read-only SQL sources through Management and Native.

The test deliberately uses the public Analysis HTTP flow.  SQLite is always
covered; PostgreSQL is covered when ``--pg-config`` points at the disposable
fixture's libpq conninfo file.  The fixture credentials are only copied into a
private policy/credential file and are never printed.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from analysis_workspace import Session, draft, publish


def sql_identifier(value):
    if not value or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
                         for character in value):
        raise ValueError("invalid fixture identifier")
    return '"' + value + '"'


def postgres_environment(config):
    """Map the fixture conninfo file to libpq environment variables."""
    import shlex

    settings = dict(item.split("=", 1) for item in shlex.split(config.read_text(encoding="utf-8")))
    names = {
        "host": "PGHOST", "hostaddr": "PGHOSTADDR", "port": "PGPORT", "user": "PGUSER",
        "dbname": "PGDATABASE", "password": "PGPASSWORD", "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT", "connect_timeout": "PGCONNECT_TIMEOUT",
    }
    environment = dict(os.environ)
    for key, variable in names.items():
        if key in settings:
            environment[variable] = settings[key]
    environment["PGCLIENTENCODING"] = "UTF8"
    return environment


def psql(config, statement, psql_path):
    result = subprocess.run(
        [str(psql_path), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1"],
        input=statement, text=True, encoding="utf-8", capture_output=True,
        env=postgres_environment(config), timeout=60)
    if result.returncode:
        raise AssertionError("PostgreSQL fixture query failed: " + result.stderr)
    return result.stdout.strip()


def model_for(preview):
    model = preview["suggestion"]
    return {**model, "confirmLineAmount": True, "confirmSingleCurrency": True, "currency": "CNY"}


def denied(action, code=403):
    try:
        action()
    except AssertionError as error:
        assert f": {code} " in str(error), error
    else:
        raise AssertionError(f"expected HTTP {code}")


def exercise_dataset(session, source_id, expected_amount, dataset_root, state_root, label):
    dataset = session.call(dataset_root, {"sourceId": source_id})
    path = dataset_root + "/" + dataset["id"]
    preview = session.call(path + "/preview")
    ids = {field["semanticRole"]: field["id"] for field in preview["fields"]}
    assert {"identifier", "date", "category", "amount", "currency"} <= ids.keys(), preview
    published = publish(session, draft(session, dataset, preview))
    assert published["state"] == "published", published
    release = published["release"]["id"]
    query = {"releaseId": release, "dimensions": [{"fieldId": ids["category"]}]}
    result = session.call(path + "/query", query)
    total = result["total"]
    assert Decimal(total["amount"]) == Decimal(expected_amount), (label, total)
    assert int(total["orders"]) == 2 and int(total["records"]) == 3, (label, total)
    monthly = {**query, "filters": [
        {"fieldId": ids["date"], "operator": "from", "value": "2026-11-01"},
        {"fieldId": ids["date"], "operator": "before", "value": "2026-12-01"},
    ]}
    month_result = session.call(path + "/query", monthly)
    month_total = month_result["total"]
    assert Decimal(month_total["amount"]) == Decimal("150.00"), (label, month_total)
    assert int(month_total["orders"]) == 1 and int(month_total["records"]) == 2, (label, month_total)
    assert session.call(path + "/query", query)["releaseId"] == release

    # Publication is a real Parquet asset, not a renamed NDJSON file.
    files = sorted((state_root / dataset["id"]).glob("release-*.parquet"))
    assert len(files) == 8, f"{label}: expected eight SQL parquet shards, found {len(files)}"
    for asset in files:
        content = asset.read_bytes()
        assert content[:4] == b"PAR1" and content[-4:] == b"PAR1", asset
    # Damage two distinct shard files in turn, so at least one is not the
    # primary asset. A warmed report must still detect changed contents.
    for asset in files[:2]:
        original = asset.read_bytes()
        info = asset.stat()
        asset.chmod(0o600)
        try:
            asset.write_bytes(b"X" + original[1:])
            os.utime(asset, ns=(info.st_atime_ns, info.st_mtime_ns))
            try:
                session.call(path + "/query", query)
            except AssertionError as error:
                _, status, body = str(error).partition(": 422 ")
                assert status and json.loads(body)["code"] == 9, error
            else:
                raise AssertionError(f"{label}: modified shard was accepted")
        finally:
            asset.write_bytes(original)
            os.utime(asset, ns=(info.st_atime_ns, info.st_mtime_ns))
            asset.chmod(info.st_mode)
    assert session.call(path + "/query", query)["total"] == total
    return dataset, path, release, query, ids


def make_identity(path):
    password = secrets.token_urlsafe(32)
    salt = os.urandom(32)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600000)
    encoded = "pbkdf2-sha256$600000$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()
    path.write_text(json.dumps({
        "version": 1,
        "roles": {"operator": [{"permission": "*", "resource": "*"}]},
        "users": [{"id": "analyst", "tenant": "bi", "passwordHash": encoded,
                    "stamp": secrets.token_hex(32), "roles": ["operator"]}],
    }), encoding="utf-8")
    path.chmod(0o600)
    return password


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dotnet", required=True)
    parser.add_argument("--management", required=True, type=Path)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--plugin", required=True, type=Path,
                        help="directory containing Native connector plugins")
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--pg-config", type=Path,
                        help="disposable PostgreSQL conninfo file; omit to run SQLite only")
    parser.add_argument("--pg-table", default=None)
    parser.add_argument("--psql", type=Path, default=None)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    psql_path = args.psql or Path(os.environ.get("UD_TEST_PSQL", "psql"))
    if args.pg_config and not args.pg_config.is_file():
        raise SystemExit("--pg-config is not a file")

    with tempfile.TemporaryDirectory(prefix="analysis-sql-", dir=args.work) as directory:
        private = Path(directory).resolve()
        sources = private / "sources"
        sources.mkdir()
        plugin_input = args.plugin.resolve()
        plugin_root = plugin_input.parent if plugin_input.is_file() else plugin_input
        suffixes = (".dll",) if os.name == "nt" else (".so", ".dylib")
        for driver in (("sqlite", "postgresql") if args.pg_config else ("sqlite",)):
            if not any((plugin_root / f"ud_connector_{driver}{suffix}").is_file() for suffix in suffixes):
                raise AssertionError(f"Missing Native {driver} connector under {plugin_root}")
        if os.name == "nt":
            # Only this unrelated directory is authorized. A Worker that guesses
            # a plugin location from its executable will fail the real preview.
            configured_root = private / "registered-drivers"
            configured_root.mkdir()
            for library in plugin_root.glob("*.dll"):
                target = configured_root / library.name
                try:
                    os.link(library, target)
                except OSError:
                    shutil.copyfile(library, target)
            plugin_root = configured_root
        sqlite_path = sources / "orders.sqlite"
        with sqlite3.connect(sqlite_path) as connection:
            # Keep the SQLite fixture's amount as text, matching the existing
            # public dashboard fixture.  SQLite exposes NUMERIC affinity as a
            # decimal discovery type without precision metadata, which the
            # analysis schema contract deliberately rejects; the analysis
            # model still performs the checked decimal conversion at bind
            # time.
            connection.execute("CREATE TABLE orders(orderid TEXT, orderdate TEXT, region TEXT, amount TEXT, currency TEXT)")
            connection.executemany("INSERT INTO orders VALUES(?,?,?,?,?)", [
                ("001", "2026-11-02", "华东", "100.00", "CNY"),
                ("001", "2026-11-03", "华南", "50.00", "CNY"),
                ("002", "2026-10-02", "华东", "100.00", "CNY"),
            ])
        # sqlite3.Connection's context manager commits/rolls back but does
        # not close the handle; close it before the host opens the same file
        # and before TemporaryDirectory attempts Windows cleanup.
        connection.close()

        # SQLite is a local file source; the Native manager uses the resource
        # itself as its read-only connection configuration when no credential
        # reference is present.  Network SQL sources below use a credential
        # file containing their provider-specific connection configuration.
        credential_files = {}
        resource_grants = {str(sqlite_path): ["Open", "Discover", "Read"]}
        sql_sources = [{
            "id": "orders-sqlite", "name": "SQLite 订单", "allowSharing": True,
            "connection": {"driver": "ud:sqlite", "resource": str(sqlite_path)},
            "table": "orders",
        }, {
            # Invalid identifiers never become visible or executable.
            "id": "unsafe-sql", "name": "Invalid", "allowSharing": True,
            "connection": {"driver": "ud:sqlite", "resource": str(sqlite_path)},
            "table": "orders; DROP TABLE orders",
        }]

        pg_table = args.pg_table or ("ud_analysis_orders_" + uuid.uuid4().hex[:12])
        pg_credential = private / "postgres-credential"
        if args.pg_config:
            pg_credential.write_text(args.pg_config.read_text(encoding="utf-8"), encoding="utf-8")
            pg_credential.chmod(0o600)
            credential_files["postgres"] = str(pg_credential)
            pg_resource = "analysis-postgres"
            resource_grants[pg_resource] = ["Open", "Discover", "Read", "Network"]
            psql(args.pg_config, f"CREATE TABLE {sql_identifier(pg_table)} "
                 "(orderid text, orderdate date, region text, amount numeric(12,2), currency text); "
                 f"INSERT INTO {sql_identifier(pg_table)} VALUES "
                 "('001','2026-11-02','华东',100.00,'CNY'),"
                 "('001','2026-11-03','华南',50.00,'CNY'),"
                 "('002','2026-10-02','华东',100.00,'CNY')", psql_path)
            sql_sources.append({
                "id": "orders-postgres", "name": "PostgreSQL 订单", "allowSharing": True,
                "connection": {"driver": "ud:postgresql", "resource": pg_resource,
                               "credentialReference": "postgres"},
                "table": pg_table,
            })

        password = make_identity(private / "identity.json")
        policy = private / "policy.json"
        policy.write_text(json.dumps({
            "dataDirectory": str(private / "state"), "identityFile": str(private / "identity.json"),
            "fileRoots": [str(sources)], "networkOrigins": [],
            "pluginRoots": [str(plugin_root)], "credentialFiles": credential_files,
            "resources": resource_grants,
            "tenants": {"bi": {
                "fileRoots": [], "readOnlyRoots": [str(sources)], "networkOrigins": [],
                "pluginRoots": [str(plugin_root)],
                "credentialReferences": list(credential_files), "resources": resource_grants,
                "analysisSqlSources": sql_sources,
            }},
        }), encoding="utf-8")

        with socket.socket() as free:
            free.bind(("127.0.0.1", 0))
            address = "http://127.0.0.1:" + str(free.getsockname()[1])
        log = args.work / "analysis-sql-host.log"
        with log.open("w", encoding="utf-8") as output:
            host = subprocess.Popen([
                args.dotnet, str(args.management), "--worker", str(args.worker), "--policy",
                str(policy), "--urls", address,
            ], stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if host.poll() is not None:
                        raise RuntimeError(f"Management stopped; see {log}")
                    try:
                        build_opener(ProxyHandler({})).open(address + "/health/live", timeout=1).close()
                        break
                    except (HTTPError, URLError, TimeoutError):
                        time.sleep(.2)
                else:
                    raise TimeoutError(f"Management unavailable; see {log}")

                session = Session(address, "analyst", password)
                sources_response = session.call("/api/analysis/sources")
                source_ids = {item["id"] for item in sources_response}
                assert "orders-sqlite" in source_ids, sources_response
                assert "unsafe-sql" not in source_ids, sources_response
                dataset_root = "/api/analysis/datasets"

                state_root = private / "state" / "tenants" / "bi" / "analysis" / "analyst"
                exercise_dataset(session, "orders-sqlite", "250.00", dataset_root, state_root, "SQLite")
                if args.pg_config:
                    exercise_dataset(session, "orders-postgres", "250.00", dataset_root, state_root, "PostgreSQL")
                denied(lambda: session.call(dataset_root, {"sourceId": "unsafe-sql"}))
                print("Analysis SQL sources passed SQLite read-only authorization, Native schema discovery, "
                      "Parquet publication and PostgreSQL live table coverage" if args.pg_config else
                      "Analysis SQL sources passed SQLite read-only authorization, Native schema discovery and "
                      "Parquet publication (PostgreSQL fixture not supplied)", flush=True)
            finally:
                host.terminate()
                try:
                    host.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    host.kill()
                    host.wait()
                # The management process owns a short-lived Worker child.
                # Give that child time to close its SQLite handle before the
                # Windows TemporaryDirectory cleanup runs.
                time.sleep(1)
        if args.pg_config:
            try:
                psql(args.pg_config, f"DROP TABLE IF EXISTS {sql_identifier(pg_table)}", psql_path)
            except Exception as error:
                print("WARNING: PostgreSQL test cleanup failed: " + str(error), file=sys.stderr)


if __name__ == "__main__":
    main()
