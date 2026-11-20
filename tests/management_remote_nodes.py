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


def exercise(dotnet, management, worker, openssl, root):
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

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
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

    base = f"https://localhost:{port}"
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
        urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(root / "ca.pem"))))
    csrf = ""

    def call(path, method="GET", data=None):
        request = urllib.request.Request(base + path, method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={"Content-Type": "application/json", "X-UD-CSRF": csrf})
        with client.open(request, timeout=4) as response:
            return json.load(response)

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
        launch("manager", [dotnet, management, "--policy", policy("manager"), "--worker", worker,
                           "--node", "manager", "--urls", base])
        until(lambda: call("/health/live"))
        csrf = call("/api/auth/login", "POST", dict(subject="operator", password=password))["csrf"]
        for name in ("node-a", "node-b"):
            configuration = write(root / f"{name}-agent.json", dict(managementUrl=f"wss://localhost:{port}/",
                nodeId=name, certificatePath=str(root / f"{name}.pfx"), managementCertificateSha256=pins["server"],
                policyPath=str(policy(name)), maxCpu=1, maxProcesses=1, maxMemoryBytes=805306368, tenants=["cluster"]))
            launch(name, [dotnet, worker, "agent", configuration])
        until(lambda: len([node for node in call("/api/workers") if node["online"] and node["remote"]]) == 2)
        document = dict(version=PROTOCOL_VERSION, id="remote-work", parameters={"payload": {"type": "utf8"}},
            connections=[dict(id="web", driver="http", resource=origin + "/effect")],
            controlFlow=dict(tasks=[dict(id="post", kind="http.request", config=dict(connection="web", method="POST",
                body={"$parameter": "payload"}, responseHeaders=["X-Trace"]))], constraints=[]), dataFlows=[])
        call("/api/packages/remote-work/versions/1", "PUT", document)
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
    with tempfile.TemporaryDirectory(prefix="ud-remote-nodes-") as directory:
        exercise(*sys.argv[1:], Path(directory))
