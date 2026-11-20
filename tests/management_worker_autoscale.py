"""Exercise two real Native file Tasks and the physical Worker lifecycle."""
import contextlib
import hashlib
import http.cookiejar
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

from product_version import PROTOCOL_VERSION


def eventually(check, seconds=25):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(.08)
    raise AssertionError(f"{check.__name__} did not converge in {seconds} seconds")


def file_contents(path):
    digest = hashlib.sha256()
    lines = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            block = block.replace(b"\r",b"")
            digest.update(block)
            lines += block.count(b"\n")
    return lines, digest.hexdigest()


dotnet, manager, worker = sys.argv[1:4]
with tempfile.TemporaryDirectory(prefix="ud-worker-supervision-") as directory:
    root = pathlib.Path(directory)
    files = root / "files"
    files.mkdir()
    input_file = files / "input.csv"
    rows = 5_000_000
    with input_file.open("wb") as source:
        source.write(b"id\n")
        for start in range(0,rows,25000):
            source.write("".join(f"{index}\n" for index in range(start,min(rows,start+25000))).encode())
    cli = pathlib.Path(worker).parent.parent / "cli" / "ud_cli.dll"
    identity = root / "identity.json"
    password = "supervision-password-7812"
    subprocess.run([dotnet, cli, "identity-init", identity, "admin", "studio"],
                   input=password + "\n", text=True, check=True, timeout=15)
    with socket.socket() as port:
        port.bind(("127.0.0.1", 0))
        base = f"http://127.0.0.1:{port.getsockname()[1]}"
    roots = [str(files)]
    tenant = dict(fileRoots=roots, pluginRoots=[], networkOrigins=[], credentialReferences=[])
    policy = root / "policy.json"
    policy.write_text(json.dumps(dict(dataDirectory=str(root / "state"), fileRoots=roots, pluginRoots=[],
        networkOrigins=[], credentialFiles={}, maxConcurrency=2, memoryLimit=67108864,
        identityFile=str(identity), tenants={"studio": tenant})), encoding="utf-8")
    settings = root / "supervisor.json"
    settings.write_text('{"prewarm":1,"idleSeconds":2}', encoding="utf-8")
    with (root / "manager.log").open("w+", encoding="utf-8") as log:
        env = {**os.environ, "DOTNET_PROCESSOR_COUNT": "2"}
        process = subprocess.Popen([dotnet, manager, "--worker", worker, "--policy", policy,
            "--worker-supervisor-config", settings, "--urls", base], stdout=log, stderr=subprocess.STDOUT, env=env)
        try:
            client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            csrf = ""

            def call(path, method="GET", body=None):
                with client.open(urllib.request.Request(base + path, method=method,
                        data=None if body is None else json.dumps(body).encode(),
                        headers={"Content-Type":"application/json","X-UD-CSRF":csrf}), timeout=4) as response:
                    body = response.read()
                    return json.loads(body) if body else None

            eventually(lambda: call("/health/ready"))
            csrf = call("/api/auth/login", "POST", dict(subject="admin", password=password))["csrf"]

            names = ("first","second")
            for name in names:
                package = dict(version=PROTOCOL_VERSION,id=name,connections=[
                    dict(id="source",driver="file",resource=str(input_file)),
                    dict(id="target",driver="file",resource=str(files/(name+".csv")))],
                    controlFlow=dict(tasks=[dict(id="copy",kind="dataFlow",config=dict(pipeline="data"))],constraints=[]),
                    dataFlows=[])
                discovery = call("/api/files/inspect","POST",dict(package=package,connection="source",
                    format="csv",options=dict(header=True)))
                package["dataFlows"] = [dict(id="data",components=[
                    dict(id="read",component="file.csv.source",config=dict(connection="source",
                        schema=discovery["schema"],fileFormat=discovery["metadata"]["options"])),
                    dict(id="write",component="file.csv.destination",config=dict(connection="target",fileMode="overwrite"))],
                    paths=[{"from":"read","to":"write","output":0,"input":0}])]
                call(f"/api/packages/{name}/versions/1","PUT",package)

            def capacity():
                return call("/api/workers")[0]["capacity"]

            def prewarmed(exclude=0):
                return next((pid for pid in capacity()["prewarmedPids"] if pid != exclude), None)

            first = eventually(prewarmed)
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(first), "/F"], check=True, capture_output=True)
            else:
                os.kill(first, signal.SIGKILL)
            replacement = eventually(lambda: prewarmed(first))
            tickets = [call("/api/executions","POST",dict(packageId=name,packageRevision="1"))["id"] for name in names]

            def simultaneous():
                runs = [call("/api/executions/"+ticket) for ticket in tickets]
                assert not any(run["state"] in ("Failed","RecoveryRequired") for run in runs), runs
                return runs if all(run["state"] == "Running" for run in runs) else None

            parallel = eventually(simultaneous)
            assert len({run["processId"] for run in parallel}) == 2
            assert replacement in {run["processId"] for run in parallel}
            assert capacity()["usedProcesses"] <= 2
            expected = file_contents(input_file)
            assert expected[0] == rows + 1
            for ticket,name in zip(tickets,names):
                eventually(lambda: call("/api/executions/"+ticket)["state"] == "Succeeded",55)
                assert file_contents(files/(name+".csv")) == expected
            cancelled = call("/api/executions","POST",dict(packageId=names[0],packageRevision="1",
                debug=dict(definitionBreakpoints=[["copy"]],previewRows=0)))["id"]
            eventually(lambda: call("/api/executions/"+cancelled+"/debug")["paused"])
            call("/api/executions/"+cancelled+"/cancel","POST")
            eventually(lambda: call("/api/executions/"+cancelled)["state"] == "Cancelled")
            assert file_contents(files/(names[0]+".csv")) == expected
            idle = eventually(prewarmed)
            change = settings.with_suffix(".next")
            change.write_text('{"prewarm":0,"idleSeconds":1}',encoding="utf-8")
            change.replace(settings)
            eventually(lambda: capacity()["usedProcesses"] == 0)
            change.write_text('{"prewarm":1,"idleSeconds":1}',encoding="utf-8")
            change.replace(settings)
            last = eventually(lambda: prewarmed(idle))
            process.kill()
            process.wait(timeout=10)

            def exited():
                if os.name == "nt":
                    result = subprocess.run(["tasklist", "/FI", f"PID eq {last}", "/FO", "CSV", "/NH"],
                        capture_output=True, check=True)
                    return f'"{last}"'.encode() not in result.stdout
                path = pathlib.Path(f"/proc/{last}/stat")
                return not path.exists() or path.read_text().split(") ", 1)[1][0] in "ZX"

            eventually(exited,10)
            print(f"PASS: prewarm, two Worker PIDs and {2*rows:,} exact rows, cancel, reload, quota and guardian exit")
        except BaseException:
            log.seek(0)
            print(log.read()[-5000:])
            raise
        finally:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            process.wait(timeout=10)
