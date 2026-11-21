"""Launch actual SDK Workers with independent SQLite, HTTP and XA database fixtures.

UD_TEST_FLUENT_XA_CONFIG names a JSON file whose outer array contains groups of
two or three database objects. Each needs name, kind, connectionString and
nativeConnectionString. Keep the file private and use disposable databases.
UD_TEST_FLUENT_LOAD=1 enables the dedicated runner's slow-session P95 gate.
"""
import hashlib
import http.cookiejar
import http.server
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import closing


def atomic(dotnet, program, worker, *plugin_roots, decision_probe=None, action="--atomic"):
    supplied = os.environ.get("UD_TEST_FLUENT_XA_CONFIG")
    if not supplied or not Path(supplied).is_file():
        print("Blocked: configure UD_TEST_FLUENT_XA_CONFIG with actual XA database participants", flush=True)
        return 77
    plugin_roots = list(dict.fromkeys(plugin_roots))
    if decision_probe is None:
        return subprocess.run([dotnet, program, action, supplied, *plugin_roots],
                              env={**os.environ, "UD_WORKER_PATH": worker}, timeout=240).returncode
    with tempfile.TemporaryDirectory(prefix="ud-xa-decision-") as scratch:
        wrapper = Path(scratch) / "worker"
        arms = Path(scratch) / "invocations"
        arms.mkdir()
        wrapper.write_text("#!/bin/sh\n"
                           f"export UD_TEST_XA_DECISION_ARMS={shlex.quote(str(arms))}\n"
                           f"export LD_PRELOAD={shlex.quote(decision_probe)}\n"
                           f'exec {shlex.quote(worker)} "$@"\n', encoding="utf-8")
        wrapper.chmod(0o700)
        return subprocess.run([dotnet, program, "--atomic-decision", supplied, str(arms), *plugin_roots],
                              env={**os.environ, "UD_WORKER_PATH": str(wrapper)}, timeout=240).returncode


def sessions(dotnet, program, worker, sqlite_plugin, mode=None):
    joined = set()
    entered = {}
    released = {}
    rendezvous = threading.Condition()
    locked = None
    latency = mode == "--latency"

    class Oracle(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal locked
            verb, *parts = self.path.strip("/").split("/")
            with rendezvous:
                if verb == "meet" and len(parts) == 3:
                    joined.add(tuple(parts))
                    rendezvous.notify_all()
                    answer = rendezvous.wait_for(lambda: len(joined) == 4, timeout=12)
                elif verb == "hold" and len(parts) == 1:
                    entered[parts[0]] = True
                    rendezvous.notify_all()
                    answer = rendezvous.wait_for(lambda: released.get(parts[0], False), timeout=22)
                elif verb == "entered" and len(parts) == 1:
                    answer = entered.get(parts[0], False)
                elif verb == "release" and len(parts) == 1:
                    released[parts[0]] = True
                    rendezvous.notify_all()
                    answer = True
                elif verb == "signal" and len(parts) == 1:
                    entered[parts[0]] = True
                    answer = True
                elif latency and verb == "lock" and not parts and locked is None:
                    locked = sqlite3.connect(databases[0], check_same_thread=False)
                    locked.execute("BEGIN EXCLUSIVE")
                    answer = True
                elif latency and verb == "unlock" and not parts and locked is not None:
                    locked.rollback()
                    locked.close()
                    locked = None
                    answer = True
                else:
                    self.send_error(404)
                    return
            body = json.dumps(answer).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # The cancelled Worker is expected to close its blocked socket.
                pass

        def log_message(self, format, *args):
            pass

    with tempfile.TemporaryDirectory(prefix="ud-fluent-") as directory, \
            http.server.ThreadingHTTPServer(("127.0.0.1", 0), Oracle) as server:
        root = Path(directory)
        files = root / "files"
        files.mkdir()
        databases = [files / (name + ".db") for name in ("orders-original", "ledger", "orders-replacement")]
        for path in databases:
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("CREATE TABLE sdk_orders (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)")
                db.execute("INSERT INTO sdk_orders VALUES (?, ?)", (1, path.stem))

        origin = f"http://127.0.0.1:{server.server_port}/"
        private = root / "private"
        private.mkdir()
        policy = private / "policy.json"
        module = Path(program).resolve(strict=True)
        references = [str(module.parent), str(Path(sqlite_plugin).resolve(strict=True).parent)]
        tenant = dict(fileRoots=[str(files)], pluginRoots=references, networkOrigins=[origin], credentialReferences=[])
        policy.write_text(json.dumps(dict(dataDirectory=str(root / "state"), fileRoots=[str(files)], pluginRoots=references,
            networkOrigins=[origin], credentialFiles={}, maxConcurrency=2 if mode == "--node-capacity" else 12,
            memoryLimit=2147483648,
            executionTimeoutSeconds=90, cancellationGraceSeconds=10 if latency else 5,
            trustedAssemblyHashes=[hashlib.sha256(module.read_bytes()).hexdigest()], tenants={"fluent": tenant})))
        serving = threading.Thread(target=server.serve_forever, daemon=True)
        serving.start()
        try:
            args = [mode, str(policy), *map(str, databases[:2]), origin] if mode else [
                str(policy), *map(str, databases), origin, str(files / "atomic-admission")]
            run = subprocess.run([dotnet, program, *args], env={**os.environ, "UD_WORKER_PATH": worker},
                check=True, capture_output=True, text=True, timeout=165 if latency else 115)
            print(run.stdout, end="")
            if latency or mode == "--node-capacity":
                return 0
            assert {result[0] for result in joined} == {"first", "second"}, joined
            assert {result[1] for result in joined} == {"orders", "ledger"}, joined
            assert len({result[2] for result in joined}) == (1 if mode == "--shared" else 2), joined
            for path in databases:
                with closing(sqlite3.connect(path)) as db:
                    assert db.execute("SELECT Id, Tag FROM sdk_orders").fetchall() == [(1, path.stem)], path.name
            print("Real Worker sessions, concurrent SQL/HTTP branches, source revision pinning, and fault isolation passed")
        except subprocess.CalledProcessError as error:
            print(error.stdout, end="")
            print(error.stderr, end="", file=sys.stderr)
            raise
        finally:
            server.shutdown()
            serving.join(timeout=3)
            if locked is not None:
                locked.close()
    return 0


def _postgres_environment(config):
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


def management_cdc(dotnet, program, worker, management, postgres_plugin, sqlite_plugin):
    supplied = os.environ.get("UD_TEST_POSTGRESQL_CONFIG")
    if not supplied or not Path(supplied).is_file():
        print("Blocked: configure UD_TEST_POSTGRESQL_CONFIG with an isolated PostgreSQL fixture", flush=True)
        return 77
    psql = os.environ.get("UD_TEST_PSQL") or shutil.which("psql")
    if not psql:
        print("Blocked: psql is required for the authenticated Management CDC fixture", flush=True)
        return 77
    config = Path(supplied).resolve()
    pg_environment = _postgres_environment(config)

    def query(sql, timeout=45):
        result = subprocess.run(
            [psql, "-X", "-w", "-qAt", "-F", "\t", "-v", "ON_ERROR_STOP=1"], input=sql,
            capture_output=True, text=True, encoding="utf-8", env=pg_environment, timeout=timeout)
        if result.returncode:
            raise AssertionError(f"PostgreSQL fixture query failed ({result.returncode}): {result.stderr}")
        return result.stdout.strip()

    fixture_id = uuid.uuid4().hex[:12]
    source_table = "ud_sdk_fluent_cdc_source_" + fixture_id
    slot = "ud_sdk_fluent_cdc_slot_" + fixture_id
    publication = "ud_sdk_fluent_cdc_pub_" + fixture_id
    request_id = "sdk-fluent-management-cdc-request"
    password = "management-cdc-password-2026"
    subject = "operator"
    with tempfile.TemporaryDirectory(prefix="ud-fluent-management-cdc-") as directory:
        root = Path(directory)
        resources = root / "resources"
        resources.mkdir()
        target = resources / "target.db"
        private = root / "credentials"
        private.mkdir()
        pg_credential = private / "postgresql.connection"
        pg_credential.write_text(config.read_text(encoding="utf-8"), encoding="utf-8")
        sqlite_credential = private / "sqlite.connection"
        sqlite_credential.write_text(str(target), encoding="utf-8")
        position_file = private / "position.json"
        with closing(sqlite3.connect(target)) as database:
            database.execute("CREATE TABLE output (id INTEGER PRIMARY KEY, label TEXT NOT NULL, value INTEGER NOT NULL)")
            database.commit()

        query(f"""
            CREATE TABLE {source_table}(id bigint PRIMARY KEY, label text NOT NULL, value bigint NOT NULL);
            ALTER TABLE {source_table} REPLICA IDENTITY FULL;
            CREATE PUBLICATION {publication} FOR TABLE {source_table};
        """)
        try:
            slot_lsn = query(f"SELECT lsn FROM pg_create_logical_replication_slot('{slot}', 'pgoutput');")
            system_id, timeline = query(
                "SELECT system_identifier, timeline_id FROM pg_control_system(), pg_control_checkpoint();"
            ).split("\t")
            query(f"""
                BEGIN;
                INSERT INTO {source_table} VALUES (1, 'before', 10), (2, 'remove', 20);
                UPDATE {source_table} SET label='after', value=11 WHERE id=1;
                DELETE FROM {source_table} WHERE id=2;
                COMMIT;
            """)
            position_file.write_text(json.dumps({
                "systemId": system_id, "timeline": int(timeline), "lsn": slot_lsn
            }, separators=(",", ":")), encoding="utf-8")

            cli = str(Path(worker).resolve().parent.parent / "cli" / "ud_cli.dll")
            identity = root / "identity.json"
            subprocess.run([dotnet, cli, "identity-init", str(identity), subject, "fluent"],
                           input=password + "\n", text=True, check=True, timeout=30)
            plugin_roots = sorted({str(Path(postgres_plugin).resolve().parent),
                                   str(Path(sqlite_plugin).resolve().parent),
                                   str(Path(program).resolve().parent)})
            grants = ["Open", "Discover", "Read", "Write", "Receipt", "Delete"]
            tenant = dict(
                fileRoots=[str(resources)], readOnlyRoots=[], networkOrigins=[],
                pluginRoots=plugin_roots, credentialReferences=["pg", "sqlite"],
                resources={"pg-resource": grants, "sqlite-resource": grants})
            policy = root / "policy.json"
            policy.write_text(json.dumps(dict(
                dataDirectory=str(root / "state"), fileRoots=[str(resources)], networkOrigins=[],
                pluginRoots=plugin_roots,
                credentialFiles={"pg": str(pg_credential), "sqlite": str(sqlite_credential)},
                identityFile=str(identity), maxConcurrency=2, memoryLimit=2147483648,
                executionTimeoutSeconds=90, cancellationGraceSeconds=10,
                tenants={"fluent": tenant})), encoding="utf-8")
            with (root / "management.log").open("w+", encoding="utf-8") as log:
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    address = f"http://127.0.0.1:{listener.getsockname()[1]}"
                service = subprocess.Popen(
                    [dotnet, management, "--policy", str(policy), "--worker", worker,
                     "--node", "fluent-cdc", "--urls", address],
                    stdout=log, stderr=subprocess.STDOUT)
                try:
                    _wait_for_management(address, service, log)
                    command = [dotnet, program, "--management-cdc", str(policy), address,
                               source_table, slot, publication, str(position_file), str(target), subject, password]
                    child = subprocess.run([*command], env={**os.environ, "UD_WORKER_PATH": worker},
                                           capture_output=True, text=True, timeout=150)
                    if child.returncode:
                        print(child.stdout, end="")
                        print(child.stderr, end="", file=sys.stderr)
                        return child.returncode
                    print(child.stdout, end="")

                    client, csrf = _management_login(address, subject, password)
                    package = _management_call(client, csrf, address,
                                               f"/api/packages/sdk-fluent-management-cdc/versions/1")
                    assert package["id"] == "sdk-fluent-management-cdc", package
                    jobs = _management_call(client, csrf, address, "/api/jobs")
                    job = next((entry for entry in jobs
                                if entry.get("id") == "sdk-fluent-management-cdc"), None)
                    assert job is not None and job["packageId"] == "sdk-fluent-management-cdc", jobs
                    first = _management_call(client, csrf, address,
                                             "/api/jobs/sdk-fluent-management-cdc/runs", "POST",
                                             {"requestId": request_id})
                    assert first["runId"], first
                    second = _management_call(client, csrf, address,
                                              "/api/jobs/sdk-fluent-management-cdc/runs", "POST",
                                              {"requestId": request_id})
                    assert second["id"] == first["id"] and second["runId"] == first["runId"], (first, second)
                    run_id = first["runId"]
                    deadline = time.monotonic() + 120
                    while True:
                        status = _management_call(client, csrf, address, "/api/executions/" + run_id)
                        if status["state"] not in ("Queued", "Claimed", "Running"):
                            break
                        if time.monotonic() >= deadline:
                            raise AssertionError(status)
                        time.sleep(.2)
                    assert status["state"] == "Succeeded", status
                    history = _management_call(client, csrf, address,
                                               "/api/jobs/sdk-fluent-management-cdc/occurrences")
                    assert len(history) == 1 and history[0]["runId"] == run_id, history
                    with closing(sqlite3.connect(target)) as database:
                        rows = database.execute("SELECT id, label, value FROM output ORDER BY id").fetchall()
                    assert rows == [(1, "after", 11)], rows
                    assert query(f"SELECT id || E'\\t' || label || E'\\t' || value FROM {source_table} ORDER BY id") == "1\tafter\t11"
                    print("Typed PostgreSQL TLS CDC -> Management job -> Worker process lifetime -> SQLite independent readback passed")
                finally:
                    if service.poll() is None:
                        service.terminate()
                        try:
                            service.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            service.kill()
                            service.wait(timeout=10)
        finally:
            query(f"""
                SELECT pg_terminate_backend(active_pid)
                  FROM pg_replication_slots
                 WHERE slot_name = '{slot}' AND active_pid IS NOT NULL;
                SELECT pg_drop_replication_slot(slot_name)
                  FROM pg_replication_slots WHERE slot_name = '{slot}';
                DROP PUBLICATION IF EXISTS {publication};
                DROP TABLE IF EXISTS {source_table};
            """)
    return 0


def _wait_for_management(address, service, log):
    deadline = time.monotonic() + 30
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.monotonic() < deadline:
        try:
            with opener.open(address + "/health/live", timeout=4) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            pass
        if service.poll() is not None:
            log.seek(0)
            raise AssertionError(log.read())
        time.sleep(.1)
    raise AssertionError("Management did not become live")


def _management_login(address, subject, password):
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    request = urllib.request.Request(address + "/api/auth/login", method="POST",
        data=json.dumps({"subject": subject, "password": password}).encode(),
        headers={"Content-Type": "application/json"})
    with client.open(request, timeout=10) as response:
        return client, json.loads(response.read())["csrf"]


def _management_call(client, csrf, address, path, method="GET", payload=None):
    headers = {"Content-Type": "application/json", "X-UD-CSRF": csrf}
    request = urllib.request.Request(address + path, method=method, headers=headers,
        data=None if payload is None else json.dumps(payload).encode())
    with client.open(request, timeout=20) as response:
        body = response.read()
        return json.loads(body) if body else None


if __name__ == "__main__":
    if sys.argv[-1] == "--atomic":
        raise SystemExit(atomic(*sys.argv[1:-1]))
    if sys.argv[-1] == "--decision":
        raise SystemExit(atomic(*sys.argv[1:-2], decision_probe=sys.argv[-2]))
    if sys.argv[-1] == "--native-orm":
        raise SystemExit(atomic(*sys.argv[1:-1], action="--native-orm"))
    if sys.argv[-1] == "--management-cdc":
        raise SystemExit(management_cdc(*sys.argv[1:-1]))
    if sys.argv[-1] in ("--shared", "--node-capacity"):
        raise SystemExit(sessions(*sys.argv[1:-1], mode=sys.argv[-1]))
    if sys.argv[-1] == "--latency":
        if os.environ.get("UD_TEST_FLUENT_LOAD") != "1":
            print("Blocked: set UD_TEST_FLUENT_LOAD=1 on a dedicated performance runner", flush=True)
            raise SystemExit(77)
        raise SystemExit(sessions(*sys.argv[1:-1], mode="--latency"))
    raise SystemExit(sessions(*sys.argv[1:]))
