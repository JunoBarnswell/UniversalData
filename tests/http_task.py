import collections
import http.server
import json
import os
import pathlib
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from product_version import PROTOCOL_VERSION


with tempfile.TemporaryDirectory(prefix="ud-http-task-") as temporary:
    root = pathlib.Path(temporary)
    requests = []
    counts = collections.Counter()
    lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def handle_request(self):
            name, action = self.path.strip("/").split("/", 1)
            folder = root / name
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with lock:
                counts[name] += 1
                number = counts[name]
                requests.append((name, self.command, body, self.headers.get("Authorization"),
                                 self.headers.get("Content-Type")))
            if action == "delay":
                (folder / "marker").write_text("received")
                time.sleep(3)
            if action == "effect":
                (folder / "marker").write_text(str(number))
            code = 503 if action == "retry" and number < 3 else 200
            if action == "failure":
                code = 500
            if action == "redirect":
                code = 302
            response = b"x" * (65536 if action == "large" else 5)
            self.send_response(code)
            if action == "redirect":
                self.send_header("Location", f"http://127.0.0.1:{plain.server_port}/{name}/effect")
            if action == "large-header":
                for i in range(16):
                    self.send_header(f"X-Pad-{i}", "x" * 8192)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response)

        do_GET = do_HEAD = do_POST = do_PUT = do_PATCH = do_DELETE = handle_request

    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = True

        def handle_error(self, request, client_address):
            pass  # Deliberate client aborts (timeout/cancel/quota) close sockets.

    def certificate(name, san):
        cert, key = root / f"{name}.pem", root / f"{name}.key"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
                        "-addext", f"subjectAltName={san}"], check=True, capture_output=True)
        return cert, key

    ca, key = certificate("server", "DNS:localhost")
    other_ca, _ = certificate("other", "DNS:localhost")
    plain = Server(("127.0.0.1", 0), Handler)
    secure = Server(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(ca, key)
    secure.socket = tls.wrap_socket(secure.socket, server_side=True)
    for server in (plain, secure):
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def fixture(name, action="ok", config=None, https=False):
        folder = root / name
        folder.mkdir()
        url = f"https://localhost:{secure.server_port}" if https else f"http://127.0.0.1:{plain.server_port}"
        connections = [{"id": "endpoint", "driver": "http", "resource": f"{url}/{name}/{action}"}]
        cfg = {"connection": "endpoint", "statusVariable": "status", "timeoutMs": 5000, **(config or {})}
        if https:
            connections.append({"id": "ca", "driver": "file", "resource": str(ca)})
            cfg["caConnection"] = "ca"
        doc = {"version": PROTOCOL_VERSION, "id": "http", "variables": {"status": {"type": "int64", "default": -1}},
               "connections": connections,
               "controlFlow": {"tasks": [{"id": "request", "kind": "http.request", "config": cfg}], "constraints": []},
               "dataFlows": []}
        return folder, doc

    def run(folder, doc, mode=0, status=0, code=200, checkpoint=False, resume=0, exit=0, resolved=0):
        package = folder / "package.json"
        package.write_text(json.dumps(doc, ensure_ascii=False))
        started = time.monotonic()
        result = subprocess.run([sys.argv[1], str(package), str(mode), str(status), str(code),
                                 str(folder / "marker"), str(folder / "state") if checkpoint else "",
                                 str(resume), str(resolved)], capture_output=True, text=True, timeout=10)
        assert result.returncode == exit, (folder.name, result.returncode, result.stderr)
        assert "fixture-token" not in result.stdout + result.stderr
        return time.monotonic() - started

    folder, doc = fixture("get")
    run(folder, doc)
    for method in ("HEAD", "POST", "PUT", "PATCH", "DELETE"):
        cfg = {"method": method}
        if method in ("POST", "PUT", "PATCH"):
            cfg.update(body="中文\u0000payload", contentType="application/octet-stream")
        folder, doc = fixture(method.lower(), config=cfg)
        run(folder, doc)
        actual = requests[-1]
        assert actual[1] == method
        if "body" in cfg:
            assert actual[2] == cfg["body"].encode() and actual[4] == cfg["contentType"]
    for name, mode in (("default-deny", 1), ("network-denied", 2)):
        folder, doc = fixture(name)
        run(folder, doc, mode=mode, status=13, code=-1)
        assert counts[name] == 0
    folder, doc = fixture("credential")
    doc["connections"][0]["credentialReference"] = "token"
    run(folder, doc, resolved=1)
    assert requests[-1][3] == "Bearer fixture-token"
    for name, mode, status in (("missing-resolver", 6, 13), ("header-injection", 7, 5)):
        folder, doc = fixture(name)
        doc["connections"][0]["credentialReference"] = "token"
        run(folder, doc, mode=mode, status=status, code=-1, resolved=1 if mode == 7 else 0)
        assert counts[name] == 0
    folder, doc = fixture("tls", https=True)
    run(folder, doc)
    for name, change in (("wrong-ca", "ca"), ("wrong-host", "host"), ("untrusted", "system")):
        folder, doc = fixture(name, https=True)
        if change == "ca":
            doc["connections"][1]["resource"] = str(other_ca)
        elif change == "host":
            doc["connections"][0]["resource"] = doc["connections"][0]["resource"].replace("localhost", "127.0.0.1")
        else:
            del doc["controlFlow"]["tasks"][0]["config"]["caConnection"]
        run(folder, doc, status=3, code=-1)
        assert counts[name] == 0
    folder, doc = fixture("ca-denied", https=True)
    run(folder, doc, mode=8, status=13, code=-1)
    assert counts[folder.name] == 0
    folder, doc = fixture("failure", "failure")
    run(folder, doc, status=3, code=500)
    assert counts[folder.name] == 1
    folder, doc = fixture("expected-failure", "failure", {"expectedStatus": 500})
    run(folder, doc, code=500)
    folder, doc = fixture("retry", "retry", {"maxAttempts": 3, "retryDelayMs": 10})
    run(folder, doc)
    assert counts[folder.name] == 3
    folder, doc = fixture("retry-exhausted", "failure", {"maxAttempts": 2})
    run(folder, doc, status=3, code=500)
    assert counts[folder.name] == 2
    folder, doc = fixture("post-no-retry", "failure", {"method": "POST"})
    run(folder, doc, status=16, code=500)
    assert counts[folder.name] == 1
    folder, doc = fixture("invalid-post-retry", config={"method": "POST", "maxAttempts": 2})
    run(folder, doc, status=5, code=-1)
    assert counts[folder.name] == 0
    folder, doc = fixture("redirect", "redirect")
    run(folder, doc, status=3, code=302)
    assert counts[folder.name] == 1 and not (folder / "marker").exists()
    folder, doc = fixture("response-limit", "large", {"maxResponseBytes": 32})
    run(folder, doc, status=4)
    folder, doc = fixture("header-limit", "large-header")
    run(folder, doc, status=4)
    for name, mode, status, timeout in (("timeout", 0, 18, 700), ("cancel", 3, 8, 5000),
                                       ("package-deadline", 5, 18, 5000), ("revoked", 9, 13, 5000)):
        folder, doc = fixture(name, "delay", {"timeoutMs": timeout})
        elapsed = run(folder, doc, mode=mode, status=status, code=-1)
        assert (folder / "marker").read_text() == "received" and elapsed < 2.5, (name, elapsed)
    for name, cfg, status in (("unknown-field", {"verifyTLS": False}, 5),
                               ("invalid-content", {"contentType": "x\r\nHost: bad"}, 5),
                               ("get-body", {"body": "x"}, 5), ("no-budget", {"maxResponseBytes": 0}, 5)):
        folder, doc = fixture(name, config=cfg)
        run(folder, doc, status=status, code=-1)
        assert counts[name] == 0
    for name, url, status in (("bad-scheme", "file:///etc/hosts", 12),
                              ("url-credential", f"http://u:p@127.0.0.1:{plain.server_port}/bad/ok", 5),
                              ("url-fragment", f"http://127.0.0.1:{plain.server_port}/bad/ok#secret", 5)):
        folder, doc = fixture(name)
        doc["connections"][0]["resource"] = url
        run(folder, doc, status=status, code=-1)
        assert counts[name] == 0
    folder, doc = fixture("readonly-output")
    doc["variables"] = {}
    doc["parameters"] = {"status": {"type": "int64", "default": -1}}
    run(folder, doc, status=13, code=-1)
    assert counts[folder.name] == 0
    folder, doc = fixture("quota", config={"method": "POST", "body": "x" * 65536})
    run(folder, doc, mode=10, status=4, code=-1)
    assert counts[folder.name] == 0
    folder, doc = fixture("effect-crash", "effect", {"method": "POST"})
    run(folder, doc, mode=4, checkpoint=True, exit=91)
    assert (folder / "marker").read_text() == "1"
    run(folder, doc, checkpoint=True, resume=1, status=16, code=-1)
    assert counts[folder.name] == 1
    folder, doc = fixture("completed-resume", "effect", {"method": "POST"})
    run(folder, doc, checkpoint=True)
    run(folder, doc, checkpoint=True, resume=1)
    assert counts[folder.name] == 1
    if len(sys.argv) > 2:
        # Optional installed NuGet/Maven consumers; the same native HTTP Task
        # and host callbacks are exercised, with no managed HTTP implementation.
        dotnet, dll, java, classes, jar, native = [str(pathlib.Path(p).resolve()) for p in sys.argv[2:]]
        environment = dict(os.environ)
        environment["LD_LIBRARY_PATH"] = native + os.pathsep + environment.get("LD_LIBRARY_PATH", "")
        consumers = {"dotnet": [dotnet, dll], "java": [java, f"-Djava.library.path={native}", "-cp", classes + os.pathsep + jar, "HttpConsumer"]}
        for language, command in consumers.items():
            for mode in ("permit", "deny", "resolver-deny"):
                name = f"{language}-{mode}"
                folder, doc = fixture(name, https=True)
                doc["connections"][0]["credentialReference"] = "token"
                package = folder / "package.json"
                package.write_text(json.dumps(doc, ensure_ascii=False))
                result = subprocess.run(command + [str(package), doc["connections"][0]["resource"], str(ca), mode], env=environment, capture_output=True, text=True, check=True, timeout=10)
                actual = json.loads(result.stdout)
                assert actual["result"] == (0 if mode == "permit" else 13), (name, actual)
                assert actual["state"] == (5 if mode == "permit" else 6)
                assert actual["executedTasks"] == (1 if mode == "permit" else 0)
                assert actual["resolutions"] == (1 if mode == "permit" else 0)
                assert actual["networkGrants"] > 0
                assert counts[name] == (1 if mode == "permit" else 0)
                if mode == "permit":
                    assert requests[-1][3] == "Bearer fixture-token"
                assert "fixture-token" not in result.stdout + result.stderr
        print("Installed NuGet/.NET and Maven/Java: same HTTPS Task, explicit network authorization, credential resolver delivery, network denial and resolver exceptions/default deny passed")
    for server in (plain, secure):
        server.shutdown()
        server.server_close()
    print("HTTP Task: real methods/body/status, resolver/default and network denial, trusted TLS plus CA/hostname rejection, no redirects, GET-only bounded retry/exhaustion, POST no retry, bounded body/headers, local/package timeout, live cancellation/revocation, quota/schema/read-only rejection, actual request effect/crash/Unknown and completed resume passed")
