"""Launch actual SDK Workers with independent SQLite, HTTP and XA database fixtures.

UD_TEST_FLUENT_XA_CONFIG names a JSON file whose outer array contains groups of
two or three database objects. Each needs name, kind, connectionString and
nativeConnectionString. Keep the file private and use disposable databases.
UD_TEST_FLUENT_LOAD=1 enables the dedicated runner's slow-session P95 gate.
"""
import hashlib
import http.server
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import threading
from contextlib import closing


def atomic(dotnet, program, worker, *plugin_roots, decision_probe=None):
    supplied = os.environ.get("UD_TEST_FLUENT_XA_CONFIG")
    if not supplied or not Path(supplied).is_file():
        print("Blocked: configure UD_TEST_FLUENT_XA_CONFIG with actual XA database participants", flush=True)
        return 77
    plugin_roots = list(dict.fromkeys(plugin_roots))
    if decision_probe is None:
        return subprocess.run([dotnet, program, "--atomic", supplied, *plugin_roots],
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


def sessions(dotnet, program, worker, sqlite_plugin, latency=False):
    joined = set()
    entered = {}
    released = {}
    rendezvous = threading.Condition()
    locked = None

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
            networkOrigins=[origin], credentialFiles={}, maxConcurrency=12, memoryLimit=2147483648,
            executionTimeoutSeconds=90, cancellationGraceSeconds=10 if latency else 5,
            trustedAssemblyHashes=[hashlib.sha256(module.read_bytes()).hexdigest()], tenants={"fluent": tenant})))
        serving = threading.Thread(target=server.serve_forever, daemon=True)
        serving.start()
        try:
            args = ["--latency", str(policy), *map(str, databases[:2]), origin] if latency else [
                str(policy), *map(str, databases), origin, str(files / "atomic-admission")]
            run = subprocess.run([dotnet, program, *args], env={**os.environ, "UD_WORKER_PATH": worker},
                check=True, capture_output=True, text=True, timeout=165 if latency else 115)
            print(run.stdout, end="")
            if latency:
                return 0
            assert {result[0] for result in joined} == {"first", "second"}, joined
            assert {result[1] for result in joined} == {"orders", "ledger"}, joined
            assert len({result[2] for result in joined}) == 2, joined
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


if __name__ == "__main__":
    if sys.argv[-1] == "--atomic":
        raise SystemExit(atomic(*sys.argv[1:-1]))
    if sys.argv[-1] == "--decision":
        raise SystemExit(atomic(*sys.argv[1:-2], decision_probe=sys.argv[-2]))
    if sys.argv[-1] == "--latency":
        if os.environ.get("UD_TEST_FLUENT_LOAD") != "1":
            print("Blocked: set UD_TEST_FLUENT_LOAD=1 on a dedicated performance runner", flush=True)
            raise SystemExit(77)
        raise SystemExit(sessions(*sys.argv[1:-1], latency=True))
    raise SystemExit(sessions(*sys.argv[1:]))
