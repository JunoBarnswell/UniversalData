"""Real management and Native Catalog schedule admission."""
import datetime as dt
import http.cookiejar
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

from product_version import PROTOCOL_VERSION

dotnet, management, worker = sys.argv[1:]
with tempfile.TemporaryDirectory(prefix="ud-job-schedules-") as temporary:
    root = Path(temporary)
    identities = root / "identities.json"
    cli = str(Path(worker).resolve().parent.parent / "cli" / "ud_cli.dll")
    subprocess.run([dotnet, cli, "identity-init", str(identities), "operator", "local"],
                   input="local-fixture-password-2026\n", text=True, check=True)
    policy = root / "policy.json"
    policy.write_text(json.dumps(dict(
        dataDirectory=str(root / "state"), fileRoots=[], networkOrigins=[], pluginRoots=[],
        credentialFiles={}, identityFile=str(identities), maxConcurrency=2,
        tenants={"local": dict(fileRoots=[], networkOrigins=[], pluginRoots=[], credentialReferences=[])}
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
            call("/api/packages/scheduled-package/versions/1", "PUT", package)
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
            again = call("/api/jobs/scheduled-job/runs", "POST", {"requestId": "same-request"})
            assert first["runId"] == again["runId"] and first["id"] == again["id"]
            for _ in range(60):
                history = call("/api/jobs/scheduled-job/occurrences")
                if len(history) == 2:
                    break
                time.sleep(0.2)
            assert len(history) == 2 and len({entry["runId"] for entry in history}) == 2, history
            print("One-shot admission, anchored interval preview and idempotent manual trigger passed")
        finally:
            process.terminate()
            try:
                process.wait(15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
