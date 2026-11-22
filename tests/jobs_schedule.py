"""Real management and Native Catalog schedule admission."""
from contextlib import closing
import datetime as dt
import http.cookiejar
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

from product_version import PROTOCOL_VERSION

def wait_execution(call, job_id, expected):
    for _ in range(180):
        history = call(f"/api/jobs/{job_id}/occurrences")
        runs = [call(f"/api/executions/{entry['runId']}") for entry in history if entry.get('runId')]
        if len(runs) == expected and all(run["state"] not in ("Queued", "Claimed", "Running") for run in runs):
            assert all(run["state"] == "Succeeded" for run in runs), runs
            return
        time.sleep(0.2)
    raise AssertionError(f"Expected {expected} settled occurrences: {runs}")


def once(name):
    start = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=3)
    return dict(id=name, frequency="Once", startAt=start.isoformat(), enabled=True)


def incremental_job(call, source, target, connections, catalog):
    name = "incremental-orders"
    fields = [dict(name=key, type="int64", nullable=False) for key in ("seq", "id", "amount")]
    fields[1]["keyOrder"] = 1
    package = dict(version=PROTOCOL_VERSION, id=name, connections=connections,
        parameters={"marker": {"type": "utf8"}},
        controlFlow=dict(tasks=[dict(id="sync", kind="dataFlow", config={"pipeline": "orders"})], constraints=[]),
        dataFlows=[dict(id="orders", components=[
            dict(id="source", component="sql.source", config=dict(connection="source", table="orders",
                schema=dict(version=PROTOCOL_VERSION, fields=fields), batchSize=2,
                incremental=dict(mode="composite", keys=["seq", "id"], after=[0, 0], unique=True, monotonic=True))),
            dict(id="target", component="sql.destination", config=dict(connection="target", table="orders",
                mode="upsert", keys=[1], delivery="receipt", provisionReceipts=True))],
            paths=[{"from": "source", "output": 0, "to": "target", "input": 0}])])
    env_path = "/api/environments/test-orders/versions"
    environment = call(env_path, "PUT", dict(packageId=name, expectedRevision="0", packageParameters={"marker": "v1"}))
    publication = dict(requestId="initial-incremental", document=package, expectedPackageRevision="0",
                       schedules=[once("first")], environment=environment)
    result = call("/api/publications", "POST", publication)
    assert result["deployment"]["environment"] == environment, result
    assert call("/api/publications", "POST", publication) == result
    assert call("/api/publications/lookup", "POST", publication) == result
    call(env_path, "PUT", dict(packageId=name, expectedRevision="1", packageParameters={"marker": "v2"}))
    wait_execution(call, name, 1)
    moved = source.with_suffix(".unavailable")
    source.rename(moved)
    try:
        assert call("/api/publications/lookup", "POST", publication) == result
        assert call("/api/publications", "POST", publication) == result
    finally:
        moved.rename(source)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("insert into orders values (10, 3, 30), (11, 4, 40)")
        db.execute("update orders set seq=12, amount=13 where id=1")
    deployment = result["deployment"]
    call(f"/api/jobs/{name}", "PUT", dict(version=PROTOCOL_VERSION, id=name, packageId=name,
        packageRevision="1", enabled=True, schedules=[once("second")], deploymentId=deployment["id"],
        deploymentRevision=deployment["deploymentRevision"]))
    wait_execution(call, name, 2)
    with closing(sqlite3.connect(target)) as db:
        assert db.execute("select seq,id,amount from orders order by id").fetchall() == [
            (12, 1, 13), (10, 2, 20), (10, 3, 30), (11, 4, 40)]
        writes = db.execute("select seq,id from writes order by rowid").fetchall()
        assert writes == [(10, 1), (10, 2), (10, 3), (11, 4), (12, 1)], writes
    empty = call(f"/api/jobs/{name}/runs", "POST", {"requestId": "empty"})
    assert call(f"/api/jobs/{name}/runs", "POST", {"requestId": "empty"}) == empty
    wait_execution(call, name, 3)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("insert into orders values (12, 5, 50)")
    call(f"/api/jobs/{name}/runs", "POST", {"requestId": "after-empty"})
    wait_execution(call, name, 4)
    with closing(sqlite3.connect(target)) as db:
        assert db.execute("select seq,id from writes order by rowid").fetchall() == writes + [(12, 5)]
    print("Two schedule occurrences, empty run, immutable environment, composite continuation and SQL readback passed")

    with closing(sqlite3.connect(source)) as db, db:
        db.execute("insert into orders values (13, 6, 60)")
    with closing(sqlite3.connect(catalog)) as db, db:
        db.execute("create trigger deny_incremental_settlement before insert on documents "
                   "when new.id='incremental/incremental-orders' and new.tenant='local' "
                   "and json_extract(new.document,'$.ownerRun') is null "
                   "begin select raise(abort, 'lost Catalog settlement'); end")
    occurrence = call(f"/api/jobs/{name}/runs", "POST", {"requestId": "lost-settlement"})
    run_path = f"/api/executions/{occurrence['runId']}"
    try:
        for _ in range(180):
            run = call(run_path)
            if run["state"] not in ("Queued", "Claimed", "Running"):
                break
            time.sleep(0.2)
        assert run["state"] == "RecoveryRequired" and run["canResume"], run
        with closing(sqlite3.connect(target)) as db:
            assert db.execute("select seq,id from writes order by rowid").fetchall() == writes + [(12, 5), (13, 6)]
        with closing(sqlite3.connect(catalog)) as db:
            position = json.loads(db.execute("select document from documents where tenant='local' "
                "and id='incremental/incremental-orders' order by revision desc limit 1").fetchone()[0])
        assert position["ownerRun"] == "run/" + occurrence["runId"] and position["after"] == [12, 5], position
    finally:
        with closing(sqlite3.connect(catalog)) as db, db:
            db.execute("drop trigger deny_incremental_settlement")
    for _ in range(50):
        if not any(node["activeProcesses"] for node in call("/api/workers")):
            break
        time.sleep(0.2)
    resumed = call(run_path + "/resume", "POST")
    assert resumed["executionId"] == run["executionId"], (run, resumed)
    wait_execution(call, name, 5)
    with closing(sqlite3.connect(target)) as db:
        assert db.execute("select seq,id from writes order by rowid").fetchall() == writes + [(12, 5), (13, 6)]
    print("Lost Catalog settlement recovered the original Native execution and advanced its cursor exactly once")


dotnet, management, worker, *plugins = sys.argv[1:]
with tempfile.TemporaryDirectory(prefix="ud-job-schedules-") as temporary:
    root = Path(temporary)
    identities = root / "identities.json"
    cli = str(Path(worker).resolve().parent.parent / "cli" / "ud_cli.dll")
    subprocess.run([dotnet, cli, "identity-init", str(identities), "operator", "local"],
                   input="local-fixture-password-2026\n", text=True, check=True)
    policy = root / "policy.json"
    credentials, connections, plugin_roots, file_roots = {}, [], [], []
    if plugins:
        plugin = Path(plugins[0]).resolve(strict=True)
        plugin_roots = [str(plugin.parent)]
        resources = root / "resources"
        resources.mkdir()
        file_roots = [str(resources)]
        source, target = (resources / f"{name}.db" for name in ("source", "target"))
        for name, database in (("source", source), ("target", target)):
            credential = root / f"{name}.connection"
            credential.write_text(str(database), encoding="utf-8")
            credentials[name] = str(credential)
            connections.append(dict(id=name, driver=str(plugin), resource=str(database), credentialReference=name))
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("create table orders (seq integer not null, id integer not null primary key, amount integer not null)")
                db.execute("create index orders_cursor on orders(seq,id)")
                if name == "source":
                    db.execute("insert into orders values (10, 1, 10), (10, 2, 20)")
                else:
                    db.execute("create table writes(seq integer, id integer)")
                    for action in ("insert", "update"):
                        db.execute(f"create trigger record_{action} after {action} on orders "
                                   "begin insert into writes values (new.seq,new.id); end")
    policy.write_text(json.dumps(dict(
        dataDirectory=str(root / "state"), fileRoots=file_roots, networkOrigins=[],
        pluginRoots=plugin_roots, credentialFiles=credentials, identityFile=str(identities), maxConcurrency=2,
        tenants={"local": dict(fileRoots=file_roots, networkOrigins=[],
                              pluginRoots=plugin_roots, credentialReferences=list(credentials))}
    )), encoding="utf-8")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    csrf = ""
    def call(path, method="GET", payload=None):
        headers = {"Content-Type": "application/json"}
        if csrf:
            headers["X-UD-CSRF"] = csrf
        request = urllib.request.Request(origin + path, headers=headers, method=method,
            data=None if payload is None else json.dumps(payload).encode())
        try:
            with client.open(request, timeout=5) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise AssertionError(f"{method} {path} returned {error.code}: {error.read().decode()}") from error
    with (root / "management.log").open("w+") as log:
        process = subprocess.Popen([dotnet, management, "--policy", str(policy), "--worker", worker, "--node", "schedules", "--urls", origin],
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(100):
                try:
                    call("/health/live")
                    break
                except OSError:
                    if process.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    time.sleep(0.1)
            grant = call("/api/auth/login", "POST", {"subject": "operator", "password": "local-fixture-password-2026"})
            csrf = grant["csrf"]
            package = dict(version=PROTOCOL_VERSION, id="scheduled-package", connections=[],
                           controlFlow=dict(tasks=[], constraints=[]), dataFlows=[])
            call("/api/versions", "POST", {"document": package, "expectedPackageRevision": "0"})
            start = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=2)
            job = dict(version=PROTOCOL_VERSION, id="scheduled-job", packageId="scheduled-package",
                       packageRevision="1", enabled=True, schedules=[
                           dict(id="once", frequency="Once", startAt=start.isoformat(), enabled=True),
                           dict(id="interval", frequency="Interval", startAt=(start + dt.timedelta(days=1)).isoformat(),
                                enabled=True, intervalSeconds=31)])
            saved = call("/api/jobs/scheduled-job", "PUT", job)
            assert len(saved["schedules"]) == 2, saved
            future = call("/api/jobs/scheduled-job/schedule-preview", "POST", job["schedules"][1])
            assert len(future) == 5 and [b-a for a, b in zip(future, future[1:])] == [31000]*4
            first = call("/api/jobs/scheduled-job/runs", "POST", {"requestId": "same-request"})
            for _ in range(60):
                history = call("/api/jobs/scheduled-job/occurrences")
                if len(history) == 2:
                    break
                time.sleep(0.2)
            assert len(history) == 2 and len({entry["runId"] for entry in history}) == 2, history
            catalog = root / "state" / "tenants" / "local" / "catalog" / "catalog.sqlite3"
            lease = "tenant='local' AND kind=7 AND id='job/scheduled-job'"
            with closing(sqlite3.connect(catalog)) as holder:
                for _ in range(20):
                    now = int(time.time() * 1000)
                    with holder:
                        claimed = holder.execute("UPDATE leases SET owner='external-scheduler', "
                            f"generation=generation+1,until_ms=? WHERE {lease} AND until_ms<=?",
                            (now + 30000, now)).rowcount
                    if claimed:
                        break
                    time.sleep(0.1)
                assert claimed == 1
                try:
                    again = call("/api/jobs/scheduled-job/runs", "POST", {"requestId": "same-request"})
                finally:
                    with holder:
                        holder.execute(f"UPDATE leases SET owner='',until_ms=0 WHERE {lease} AND owner='external-scheduler'")
            assert first["runId"] == again["runId"] and first["id"] == again["id"]
            for _ in range(150):
                runs = [call(f"/api/executions/{entry['runId']}") for entry in history]
                nodes = call("/api/workers")
                if not any(node["activeProcesses"] for node in nodes) and all(
                    run["state"] not in ("Queued", "Claimed", "Running") for run in runs
                ):
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(f"Scheduled runs and Worker processes did not complete: {runs}, {nodes}")
            if any(run["state"] != "Succeeded" for run in runs):
                log.seek(0)
                raise AssertionError(f"Scheduled runs did not succeed: {runs}\n{log.read()}")
            print("One-shot admission, anchored interval preview and idempotent manual trigger passed")
            if plugins:
                incremental_job(call, source, target, connections, catalog)
        finally:
            process.terminate()
            try:
                process.wait(15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
