"""Exercise a real mTLS coordinator and two independent Worker nodes."""
import hashlib
import http.cookiejar
import http.server
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

from product_version import PROTOCOL_VERSION


def write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def exercise(dotnet, management, worker, openssl, root, postgres=None):
    def run(*arguments, **kwargs):
        return subprocess.run(list(map(str, arguments)), cwd=root, check=True, timeout=30,
                              capture_output=True, **kwargs)

    run(openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=UD fixture CA",
        "-keyout", "ca.key", "-out", "ca.pem")
    pins = {}
    for name in ("server", "node-a", "node-b"):
        run(openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
            "-keyout", f"{name}.key", "-out", f"{name}.csr")
        extensions = root / f"{name}.cfg"
        extensions.write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=" +
                              ("serverAuth" if name == "server" else "clientAuth") + "\n", encoding="ascii")
        run(openssl, "x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
            "-CAcreateserial", "-out", f"{name}.pem", "-days", "1", "-extfile", extensions)
        run(openssl, "pkcs12", "-export", "-out", f"{name}.pfx", "-inkey", f"{name}.key", "-in", f"{name}.pem",
            "-certfile", "ca.pem", "-passout", "pass:")
        der = run(openssl, "x509", "-in", f"{name}.pem", "-outform", "der").stdout
        pins[name] = hashlib.sha256(der).hexdigest()
    identity = root / "identity.json"
    cli = Path(worker).resolve().parent.parent / "cli" / "ud_cli.dll"
    password = "remote-fixture-password-2026"
    run(dotnet, cli, "identity-init", identity, "operator", "cluster", input=(password + "\n").encode())
    effects = []
    lock = threading.Lock()

    class Endpoint(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"])).decode()
            with lock:
                effects.append(body)
            time.sleep(2)
            self.send_response(200)
            self.send_header("X-Trace", "remote-trace")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    endpoint = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Endpoint)
    thread = threading.Thread(target=endpoint.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{endpoint.server_port}"

    def policy(name):
        tenant = dict(fileRoots=[], networkOrigins=[origin], pluginRoots=[], credentialReferences=[])
        return write(root / f"{name}.json", dict(dataDirectory=str(root / f"{name}-state"), identityFile=str(identity),
            credentialFiles={}, maxConcurrency=3, memoryLimit=67108864, tenants={"cluster": tenant},
            fileRoots=[], networkOrigins=[origin], pluginRoots=[]))

    ports = []
    for _ in range(2 if postgres else 1):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            ports.append(listener.getsockname()[1])
    port = ports[0]
    env = {**os.environ, "SSL_CERT_FILE": str(root / "ca.pem"),
           "ASPNETCORE_Kestrel__Certificates__Default__Path": str(root / "server.pfx"),
           "ASPNETCORE_Kestrel__Certificates__Default__Password": ""}
    for name in ("node-a", "node-b"):
        env[f"WorkerNodes__{name}__CertificateSha256"] = pins[name]
        env[f"WorkerNodes__{name}__Tenants__0"] = "cluster"
    processes = []
    logs = []

    def launch(name, command):
        log = (root / (name + ".log")).open("w+", encoding="utf-8")
        logs.append(log)
        processes.append(subprocess.Popen(list(map(str, command)), env=env, stdout=log, stderr=subprocess.STDOUT))
        return processes[-1]

    base = f"https://localhost:{port}"
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(root / "ca.pem"))))
    csrf = ""

    def call(path, method="GET", data=None):
        request = urllib.request.Request(base + path, method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={"Content-Type": "application/json", "X-UD-CSRF": csrf})
        with client.open(request, timeout=4) as response:
            return json.load(response) if response.status != 204 else None

    def until(check, seconds=20):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                if result := check():
                    return result
            except OSError:
                pass
            if any(process.poll() is not None for process in processes):
                break
            time.sleep(.2)
        for log in logs:
            log.seek(0)
            print(log.read()[-3000:])
        raise RuntimeError("Cluster did not converge")

    try:
        manager_policy = policy("manager")
        cluster_args = []
        if postgres:
            import base64
            key = root / "cluster-key"
            key.write_bytes(base64.b64encode(os.urandom(32)))
            run(dotnet, cli, "catalog", "migrate", "import", manager_policy, postgres, key)
            cluster_args = ["--Cluster:ConnectionFile", postgres, "--Cluster:EncryptionKeyFile", key]
        initial_address = base
        primary_command = [dotnet, management, "--policy", manager_policy, "--worker", worker,
                           "--node", "manager", "--urls", base, *cluster_args]
        first = launch("manager", primary_command)
        until(lambda: call("/health/ready"))
        if postgres:
            standby = f"https://localhost:{ports[1]}"
            successor = launch("standby", [dotnet, management, "--policy", policy("standby"), "--worker", worker,
                               "--node", "standby", "--urls", standby, *cluster_args])
        csrf = call("/api/auth/login", "POST", dict(subject="operator", password=password))["csrf"]
        if postgres:
            active_cookie = next(iter(jar))
            active_csrf = csrf
            csrf = call("/api/auth/login","POST",dict(subject="operator",password=password))["csrf"]
            revoked_cookie = next(iter(jar))
            call("/api/auth/logout","POST")
            jar.set_cookie(active_cookie)
            csrf = active_csrf
        for name in ("node-a", "node-b"):
            configuration = write(root / f"{name}-agent.json", dict(managementEndpoints=[dict(url=f"wss://localhost:{candidate}/", certificateSha256=pins["server"]) for candidate in ports],
                nodeId=name, certificatePath=str(root / f"{name}.pfx"),
                policyPath=str(policy(name)), maxCpu=1, maxProcesses=1, maxMemoryBytes=805306368, tenants=["cluster"]))
            launch(name, [dotnet, worker, "agent", configuration])
        until(lambda: len([node for node in call("/api/workers") if node["online"] and node["remote"]]) == 2)
        document = dict(version=PROTOCOL_VERSION, id="remote-work", parameters={"payload": {"type": "utf8"}},
            connections=[dict(id="web", driver="http", resource=origin + "/effect")],
            controlFlow=dict(tasks=[dict(id="post", kind="http.request", config=dict(connection="web", method="POST",
                body={"$parameter": "payload"}, responseHeaders=["X-Trace"]))], constraints=[]), dataFlows=[])
        call("/api/versions", "POST", {"document": document, "expectedPackageRevision": "0"})
        if postgres:
            request = dict(packageId="remote-work",packageRevision="1",requestId="ha-request",
                           parameters=json.dumps(dict(payload="survivor")))
            run_state = call("/api/executions","POST",request)
            ticket = run_state["id"]
            assert call("/api/executions","POST",request)["id"] == ticket
            until(lambda: "survivor" in effects)
            running = call("/api/executions/"+ticket)
            assert running["state"] == "Running", running
            first.kill()
            first.wait(timeout=8)
            processes.remove(first)
            failed = time.monotonic()
            base = standby
            until(lambda: call("/health/ready"))
            elapsed = time.monotonic() - failed
            assert elapsed < 20, "Management recovery exceeded 20 seconds"
            assert call("/api/auth/session")["subject"] == "operator"
            jar.set_cookie(revoked_cookie)
            try:
                call("/api/auth/session")
                raise AssertionError("Revoked session was accepted after takeover")
            except urllib.error.HTTPError as denied:
                assert denied.code == 401, denied.code
            jar.set_cookie(active_cookie)
            assert call("/api/executions","POST",request)["id"] == ticket
            done = until(lambda: (state if (state := call("/api/executions/"+ticket))["state"] == "Succeeded" else None))
            assert (done["executionId"],done["processId"],done["workerNode"]) == (running["executionId"],running["processId"],running["workerNode"])
            assert effects.count("survivor") == 1, effects
            launch("candidate",primary_command)
            base = initial_address
            until(lambda: call("/health/live"))
            try:
                call("/health/ready")
                raise AssertionError("The former leader resumed dispatching while the new term was live")
            except urllib.error.HTTPError as fenced:
                assert fenced.code == 503, fenced.code
            successor.kill()
            successor.wait(timeout=8)
            processes.remove(successor)
            backwards = time.monotonic()
            until(lambda: call("/health/ready"))
            reverse_elapsed = time.monotonic() - backwards
            assert reverse_elapsed < 20
            assert call("/api/executions","POST",request)["id"] == ticket and effects.count("survivor") == 1
            print(f"PASS: PostgreSQL lease takeover in {elapsed:.2f}s and {reverse_elapsed:.2f}s, session continuity and revocation, same ticket, same Worker PID, one target effect")
            return
        tickets = [call("/api/executions", "POST", dict(packageId="remote-work", packageRevision="1",
            parameters=json.dumps(dict(payload=f"parameter-{index}"))))["id"] for index in range(3)]

        def finished():
            states = [call("/api/executions/" + ticket) for ticket in tickets]
            return states if all(state["state"] in ("Succeeded", "Failed", "RecoveryRequired") for state in states) else None

        states = until(finished)
        if any(state["state"] != "Succeeded" for state in states):
            for log in logs:
                log.seek(0)
                print(log.read()[-2500:])
        assert all(state["state"] == "Succeeded" for state in states), states
        assert {state["workerNode"] for state in states} == {"manager", "node-a", "node-b"}, states
        assert sorted(effects) == ["parameter-0", "parameter-1", "parameter-2"], effects
        for ticket in tickets:
            [task] = call(f"/api/executions/{ticket}/metrics")["diagnostics"]["tasks"]
            assert task["httpStatus"] == 200 and task["responseHeaders"] == "X-Trace: remote-trace\r\n", task
        print("Two authenticated Worker nodes, ordinary dispatch, capacity, parameter transfer and HTTP facts passed")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()
        endpoint.shutdown()
        endpoint.server_close()


if __name__ == "__main__":
    args = sys.argv[1:5]
    postgres = os.getenv("UD_TEST_SENTINEL_POSTGRESQL_CONNECTION_FILE") if "--sentinel" in sys.argv[5:] else None
    if "--sentinel" in sys.argv[5:] and (not postgres or not Path(postgres).is_file()):
        print("Blocked: a dedicated PostgreSQL 17 Catalog is required")
        raise SystemExit(77)
    with tempfile.TemporaryDirectory(prefix="ud-remote-workers-") as directory:
        exercise(*args, Path(directory), postgres)
