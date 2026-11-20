"""Verify encrypted, idempotent migration against a dedicated PostgreSQL 17 catalog."""
import base64
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import uuid
import urllib.request
import socket
import time

from product_version import PROTOCOL_VERSION

dotnet, manager, worker = sys.argv[1:4]
connection = os.getenv("UD_TEST_CATALOG_POSTGRESQL_CONNECTION_FILE")
if not connection or not pathlib.Path(connection).is_file():
    print("Blocked: a dedicated PostgreSQL 17 Catalog is required")
    raise SystemExit(77)

with tempfile.TemporaryDirectory(prefix="ud-catalog-migration-") as directory:
    root = pathlib.Path(directory)
    cli = pathlib.Path(worker).parent.parent / "cli" / "ud_cli.dll"
    tenant = "migration-" + uuid.uuid4().hex[:16]
    key, identity, policy = [root / name for name in ("key","identity.json","policy.json")]
    key.write_bytes(base64.b64encode(os.urandom(32)))
    password = "catalog-migration-password-22771"
    command = [dotnet, cli]
    subprocess.run([*command,"identity-init",identity,"admin",tenant],
        input=password+"\n",text=True,check=True,timeout=20)
    policy.write_text(json.dumps(dict(dataDirectory=str(root/"source"),fileRoots=[],pluginRoots=[],
        networkOrigins=[],credentialFiles={},identityFile=str(identity),
        tenants={tenant:dict(fileRoots=[],pluginRoots=[],networkOrigins=[],credentialReferences=[])})))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0))
        base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    log = (root/"source.log").open("w+",encoding="utf-8")
    host = subprocess.Popen([dotnet,manager,"--policy",policy,"--worker",worker,"--urls",base],
                            stdout=log,stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                urllib.request.urlopen(base+"/health/ready",timeout=2).close()
                break
            except OSError:
                assert host.poll() is None and time.monotonic()<deadline
                time.sleep(.15)
        import http.cookiejar
        jar = http.cookiejar.CookieJar()
        client = urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(jar))
        csrf = ""

        def api(path,body,method="POST"):
            with client.open(urllib.request.Request(base+"/api"+path,method=method,data=json.dumps(body).encode(),
                headers={"Content-Type":"application/json","X-UD-CSRF":csrf}),timeout=5) as response:
                return json.load(response)

        csrf = api("/auth/login",dict(subject="admin",password=password))["csrf"]
        package = dict(version=PROTOCOL_VERSION,id="history",controlFlow=dict(tasks=[],constraints=[]),dataFlows=[])
        api("/packages/history/versions/1",package,"PUT")
        api("/jobs/daily",dict(version=PROTOCOL_VERSION,id="daily",packageId="history",packageRevision="1",
            enabled=True,schedules=[],parameters='{"private":"migration-secret"}'),"PUT")
    finally:
        host.terminate()
        host.wait(timeout=15)
        log.close()

    def migrate(mode,successful=True):
        result = subprocess.run([*command,"catalog","migrate",mode,policy,connection,key],
            capture_output=True,text=True,timeout=40,check=successful)
        return json.loads(result.stdout) if successful else result.returncode

    before = migrate("check")
    copied = migrate("import")
    assert migrate("import") == copied
    assert before["tenants"] == copied["tenants"] == migrate("verify")["tenants"]
    assert copied["tenants"][0]["protectedParameters"] == 1 and copied["cookieKeys"] > 0
    with (root/"target.log").open("w+",encoding="utf-8") as target_log:
        target = subprocess.Popen([dotnet,manager,"--policy",policy,"--worker",worker,"--urls",base,
            "--node","migrated","--Cluster:ConnectionFile",connection,"--Cluster:EncryptionKeyFile",key],
            stdout=target_log,stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while True:
                try:
                    with client.open(base+"/health/ready",timeout=2):
                        break
                except OSError:
                    assert target.poll() is None and time.monotonic() < deadline
                    time.sleep(.15)
            with client.open(base+"/api/auth/session",timeout=5) as response:
                assert json.load(response)["subject"] == "admin"
            with client.open(base+"/api/jobs",timeout=5) as response:
                [job] = json.load(response)
                assert job["id"] == "daily" and job["parametersReference"].startswith("parameters-")
        finally:
            target.terminate()
            target.wait(timeout=15)
    # Modification of an immutable source row must prevent a successful verification.
    database = root/"source"/"tenants"/tenant/"catalog"/"catalog.sqlite3"
    with sqlite3.connect(database) as source:
        source.execute("UPDATE documents SET digest=zeroblob(32) WHERE id='job/daily'")
    assert migrate("verify",False) != 0
    print("PASS: identical snapshot replay, historical job and session readback, encrypted references, corruption rejection")
