"""Real single-host Management/Worker cluster harness.

The harness is intentionally an orchestration layer only.  All execution,
placement, lease, mTLS and recovery decisions remain in the product's normal
Management and Worker binaries.  The loopback HTTP service is an external
effect sink: it durably records each key before replying and exposes a readback
endpoint used by the acceptance checks.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Iterable


WORKER_NAMES = ("worker-1", "worker-2", "worker-3")
TENANT = "cluster"
EFFECT_DELAY_SECONDS = 3.0
STATE_VERSION = 1


class ClusterFailure(RuntimeError):
    """A user-actionable failure with evidence printed by the caller."""


def json_write(path: Path, value: Any, mode: int = 0o600) -> None:
    """Atomically write a private JSON artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def tail(path: Path, limit: int = 120) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as error:
        return f"<unable to read {path}: {error}>"
    return "\n".join(lines[-limit:])


def _linux_process_identity(pid: int) -> tuple[int, str] | None:
    """Return Linux start ticks and command line for PID identity checks."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        fields = stat[stat.rfind(")") + 2 :].split()
        if len(fields) <= 19 or fields[0] in ("Z", "X"):
            return None
        start_ticks = int(fields[19])
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(
            "utf-8", errors="replace"
        ).strip()
        return start_ticks, command
    except (FileNotFoundError, ProcessLookupError, ValueError, OSError):
        return None


def process_record(process: subprocess.Popen[Any], identity: str, owner_token: str) -> dict[str, Any]:
    current = _linux_process_identity(process.pid)
    if current is None:
        raise ClusterFailure(f"Process {process.pid} exited before ownership could be recorded ({identity})")
    start_ticks, command = current
    return {"pid": process.pid, "startTicks": start_ticks, "identity": identity,
            "ownerToken": owner_token, "command": command}


def owned_process(record: dict[str, Any]) -> bool:
    current = _linux_process_identity(int(record["pid"]))
    if not current or current[0] != int(record["startTicks"]) or str(record["identity"]) not in current[1]:
        return False
    try:
        environment = Path(f"/proc/{int(record['pid'])}/environ").read_bytes().split(b"\0")
        marker = f"UD_UNIVERSALDATA_DEMO_OWNER={record['ownerToken']}".encode("utf-8")
        return marker in environment
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return False


def wait_exit(record: dict[str, Any], seconds: float = 12.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not owned_process(record):
            try:
                os.waitpid(int(record["pid"]), os.WNOHANG)
            except (ChildProcessError, OSError):
                pass
            return
        time.sleep(0.1)
    raise ClusterFailure(
        f"Owned process did not exit: pid={record['pid']} identity={record['identity']}"
    )


def terminate_owned(record: dict[str, Any], force: bool = False) -> None:
    if not owned_process(record):
        return
    pid = int(record["pid"])
    os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
    try:
        wait_exit(record, 12 if not force else 5)
    except ClusterFailure:
        if not force and owned_process(record):
            os.kill(pid, signal.SIGKILL)
            wait_exit(record, 5)
        else:
            raise


class EffectStore:
    """Durable external effect service used by both demo and sentinel."""

    @staticmethod
    def serve(directory: str, port: int) -> int:
        root = Path(directory).resolve()
        entries = root / "effects"
        entries.mkdir(parents=True, exist_ok=True)
        attempts = root / "attempts.ndjson"
        lock = threading.Lock()
        active = 0
        maximum = 0

        def key_path(key: str) -> Path:
            digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
            return entries / f"{digest}.json"

        def status() -> dict[str, Any]:
            keys: list[str] = []
            if attempts.exists():
                for line in attempts.read_text(encoding="utf-8").splitlines():
                    if not line:
                        continue
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if value.get("key") not in keys:
                        keys.append(value["key"])
            return {"keys": keys, "attempts": len(attempts.read_text(encoding="utf-8").splitlines()) if attempts.exists() else 0,
                    "active": active, "maximumActive": maximum}

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_: Any) -> None:
                return

            def reply(self, code: int, body: bytes, content_type: str = "application/json") -> None:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Trace", "cluster-demo-effect")
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
                path = urllib.parse.urlsplit(self.path).path
                if path == "/status":
                    self.reply(200, json.dumps(status(), sort_keys=True).encode("utf-8"))
                    return
                if path.startswith("/effect/"):
                    key = urllib.parse.unquote(path[len("/effect/") :])
                    record = key_path(key)
                    if not record.is_file():
                        self.reply(404, b'{"error":"effect-not-found"}')
                    else:
                        self.reply(200, record.read_bytes())
                    return
                self.reply(404, b'{"error":"not-found"}')

            def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
                nonlocal active, maximum
                if urllib.parse.urlsplit(self.path).path != "/effect":
                    self.reply(404, b'{"error":"not-found"}')
                    return
                length = int(self.headers.get("Content-Length", "0"))
                key = self.rfile.read(length).decode("utf-8")
                if not key or len(key) > 512 or any(ord(char) < 0x20 for char in key):
                    self.reply(400, b'{"error":"invalid-effect-key"}')
                    return
                record = key_path(key)
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                    first = not record.exists()
                    if first:
                        payload = json.dumps(
                            {"key": key, "firstSeenMs": int(time.time() * 1000)}, sort_keys=True
                        ).encode("utf-8")
                        temporary = record.with_suffix(".tmp")
                        temporary.write_bytes(payload)
                        with temporary.open("rb") as stream:
                            os.fsync(stream.fileno())
                        os.replace(temporary, record)
                        os.chmod(record, 0o600)
                    with attempts.open("a", encoding="utf-8") as stream:
                        os.chmod(attempts, 0o600)
                        stream.write(json.dumps({"key": key, "first": first, "pid": os.getpid()}) + "\n")
                        stream.flush()
                        os.fsync(stream.fileno())
                time.sleep(EFFECT_DELAY_SECONDS)
                with lock:
                    active -= 1
                self.reply(200, b"{}")

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True

            def handle_error(self, *_: Any) -> None:
                return

        server = Server(("127.0.0.1", port), Handler)
        (root / "server.ready").write_text(str(port), encoding="ascii")
        try:
            server.serve_forever(poll_interval=0.2)
        finally:
            server.server_close()
        return 0


class Api:
    def __init__(self, base: str, ca: Path):
        self.base = base.rstrip("/")
        self.jar = __import__("http.cookiejar", fromlist=["CookieJar"]).CookieJar()
        self.client = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            __import__("urllib.request", fromlist=["HTTPCookieProcessor"]).HTTPCookieProcessor(self.jar),
            urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(ca))),
        )
        self.csrf = ""

    def request(self, path: str, method: str = "GET", value: Any = None, allow_error: bool = False) -> tuple[int, Any]:
        data = None if value is None else json.dumps(value).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            method=method,
            data=data,
            headers={"Content-Type": "application/json", "X-UD-CSRF": self.csrf},
        )
        try:
            with self.client.open(request, timeout=5) as response:
                payload = response.read()
                return response.status, json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            payload = error.read()
            if not allow_error:
                raise
            return error.code, json.loads(payload) if payload else None

    def login(self, password: str) -> dict[str, Any]:
        status, result = self.request("/api/auth/login", "POST", {"subject": "operator", "password": password})
        if status != 200:
            raise ClusterFailure(f"Management login failed: HTTP {status}: {result}")
        self.csrf = result["csrf"]
        return result


class ClusterHarness:
    """Start, inspect, fail over and stop a real single-host cluster."""

    def __init__(
        self,
        dotnet: str,
        management: str,
        worker: str,
        openssl: str,
        root: Path,
        postgres: str | None,
        cluster: bool = True,
        worker_names: Iterable[str] = WORKER_NAMES,
    ):
        self.dotnet = str(Path(dotnet).resolve())
        self.management = str(Path(management).resolve())
        self.worker = str(Path(worker).resolve())
        self.openssl = str(Path(openssl).resolve())
        self.root = Path(root).resolve()
        self.postgres = str(Path(postgres).resolve()) if postgres else None
        self.cluster = cluster
        self.worker_names = tuple(worker_names)
        if cluster and self.worker_names != WORKER_NAMES:
            raise ClusterFailure("The HA harness requires exactly worker-1, worker-2 and worker-3")
        if cluster and not self.postgres:
            raise ClusterFailure("A dedicated PostgreSQL 17 connection file is required for HA mode")
        if cluster and not Path(self.postgres).is_file():
            raise ClusterFailure(f"Dedicated PostgreSQL connection file does not exist: {self.postgres}")
        self.state_path = self.root / "state.json"

    @property
    def cli(self) -> str:
        return str(Path(self.worker).parent.parent / "cli" / "ud_cli.dll")

    def _run(self, *arguments: Any, input_data: bytes | None = None, timeout: int = 40) -> subprocess.CompletedProcess[Any]:
        try:
            result = subprocess.run(
                [str(argument) for argument in arguments],
                cwd=self.root,
                input=input_data,
                capture_output=True,
                timeout=timeout,
                check=True,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            output = getattr(error, "stderr", b"") or b""
            if isinstance(output, bytes):
                output = output.decode("utf-8", errors="replace")
            raise ClusterFailure(f"Command failed: {' '.join(map(str, arguments))}\n{output}") from error
        return result

    def _load(self) -> dict[str, Any]:
        if not self.state_path.is_file():
            raise ClusterFailure(f"Cluster state does not exist: {self.state_path}; run start first")
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ClusterFailure(f"Cannot read cluster state {self.state_path}: {error}") from error
        if state.get("version") != STATE_VERSION or state.get("root") != str(self.root):
            raise ClusterFailure(f"Cluster state belongs to a different or unsupported instance: {self.state_path}")
        return state

    def _save(self, state: dict[str, Any]) -> None:
        json_write(self.state_path, state)

    def _launch(self, state: dict[str, Any], name: str, command: list[str], identity: str) -> dict[str, Any]:
        log_path = self.root / f"{name}.log"
        log = log_path.open("a", encoding="utf-8")
        os.chmod(log_path, 0o600)
        environment = self._environment()
        environment["UD_UNIVERSALDATA_DEMO_OWNER"] = state["ownerToken"]
        try:
            process = subprocess.Popen(command, cwd=self.root, env=environment, stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.close()
        record = process_record(process, identity, state["ownerToken"])
        record["log"] = str(log_path)
        state.setdefault("processes", {})[name] = record
        self._save(state)
        return record

    def _environment(self) -> dict[str, str]:
        environment = dict(os.environ)
        if (self.root / "ca.pem").is_file():
            environment["SSL_CERT_FILE"] = str(self.root / "ca.pem")
            environment["ASPNETCORE_Kestrel__Certificates__Default__Path"] = str(self.root / "server.pfx")
            environment["ASPNETCORE_Kestrel__Certificates__Default__Password"] = ""
            pins = self._load_pins()
            for worker_name in self.worker_names:
                environment[f"WorkerNodes__{worker_name}__CertificateSha256"] = pins[worker_name]
                environment[f"WorkerNodes__{worker_name}__Tenants__0"] = TENANT
        return environment

    def _load_pins(self) -> dict[str, str]:
        return json.loads((self.root / "certificate-pins.json").read_text(encoding="utf-8"))

    def start(self) -> dict[str, Any]:
        if self.state_path.exists():
            try:
                existing = self._load()
                live = [name for name, record in existing.get("processes", {}).items() if owned_process(record)]
            except ClusterFailure:
                live = []
            if live:
                raise ClusterFailure(f"A live cluster instance already owns {live}; use its stop action first")
            raise ClusterFailure(f"State already exists at {self.state_path}; choose a new private state directory")
        if sys.platform != "linux":
            raise ClusterFailure("This demo targets Linux/WSL2 because ownership checks use /proc")
        if self.root.exists():
            if not self.root.is_dir() or any(self.root.iterdir()):
                raise ClusterFailure(f"State directory is not a new private directory: {self.root}")
        else:
            self.root.mkdir(parents=True, exist_ok=False)
        os.chmod(self.root, 0o700)
        state: dict[str, Any] = {
            "version": STATE_VERSION,
            "root": str(self.root),
            "cluster": self.cluster,
            "tenant": TENANT,
            "workerNames": list(self.worker_names),
            "postgres": self.postgres,
            "password": secrets.token_urlsafe(24),
            "ownerToken": secrets.token_hex(16),
            "processes": {},
            "managers": {},
            "activeManager": "active",
        }
        self._save(state)
        try:
            self._start_effects(state)
            self._create_certificates(state)
            self._create_identity_and_policies(state)
            self._start_managers(state)
            self._start_workers(state)
            self._wait_membership(state)
            state["startedAtMs"] = int(time.time() * 1000)
            self._save(state)
            print(f"PASS: started active/passive Management and {len(self.worker_names)} mTLS Worker Agents")
            return state
        except Exception:
            self._print_logs(state)
            self._stop_state(state, quiet=True)
            raise

    def _start_effects(self, state: dict[str, Any]) -> None:
        port = reserve_port()
        command = [sys.executable, str(Path(__file__).resolve()), "effects-server", "--directory", str(self.root), "--port", str(port)]
        record = self._launch(state, "effects", command, str(self.root))
        state["effectPort"] = port
        state["effectUrl"] = f"http://127.0.0.1:{port}"
        state["effectPid"] = record["pid"]
        self._wait_effects(state)

    def _wait_effects(self, state: dict[str, Any]) -> None:
        url = state["effectUrl"] + "/status"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=1) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            if not owned_process(state["processes"]["effects"]):
                break
            time.sleep(0.1)
        raise ClusterFailure("The durable external-effect service did not become ready")

    def _create_certificates(self, state: dict[str, Any]) -> None:
        self._run(
            self.openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=UniversalData demo CA",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        )
        pins: dict[str, str] = {}
        for name in ("server", *self.worker_names):
            self._run(self.openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
                      "-keyout", f"{name}.key", "-out", f"{name}.csr")
            extension = self.root / f"{name}.cfg"
            usage = "serverAuth" if name == "server" else "clientAuth"
            extension.write_text(f"subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage={usage}\n", encoding="ascii")
            self._run(self.openssl, "x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                      "-CAcreateserial", "-out", f"{name}.pem", "-days", "2", "-extfile", extension)
            self._run(self.openssl, "pkcs12", "-export", "-out", f"{name}.pfx", "-inkey", f"{name}.key",
                      "-in", f"{name}.pem", "-certfile", "ca.pem", "-passout", "pass:")
            der = self._run(self.openssl, "x509", "-in", f"{name}.pem", "-outform", "der").stdout
            pins[name] = hashlib.sha256(der).hexdigest()
        json_write(self.root / "certificate-pins.json", {name: pins[name] for name in self.worker_names})
        state["serverPin"] = hashlib.sha256(
            self._run(self.openssl, "x509", "-in", "server.pem", "-outform", "der").stdout
        ).hexdigest()
        state["certificatePins"] = pins
        self._save(state)
        for path in self.root.glob("*.key"):
            os.chmod(path, 0o600)
        for path in self.root.glob("*.pfx"):
            os.chmod(path, 0o600)

    def _policy(self, name: str, identity: Path, origin: str) -> Path:
        directory = self.root / f"{name}-state"
        value = {
            "dataDirectory": str(directory),
            "identityFile": str(identity),
            "credentialFiles": {},
            "maxConcurrency": 3,
            "executionTimeoutSeconds": 90,
            "cancellationGraceSeconds": 2,
            "memoryLimit": 67108864,
            "tenants": {TENANT: {"fileRoots": [], "networkOrigins": [origin], "pluginRoots": [], "credentialReferences": []}},
            "fileRoots": [],
            "networkOrigins": [origin],
            "pluginRoots": [],
        }
        return json_write_path(self.root / f"{name}-policy.json", value)

    def _create_identity_and_policies(self, state: dict[str, Any]) -> None:
        identity = self.root / "identity.json"
        self._run(self.dotnet, self.cli, "identity-init", identity, "operator", TENANT,
                  input_data=(state["password"] + "\n").encode("utf-8"))
        os.chmod(identity, 0o600)
        origin = state["effectUrl"]
        manager_policy = self._policy("active", identity, origin)
        standby_policy = self._policy("standby", identity, origin)
        worker_policies = {name: str(self._policy(name, identity, origin)) for name in self.worker_names}
        key = self.root / "catalog-key"
        key.write_bytes(base64.b64encode(secrets.token_bytes(32)))
        os.chmod(key, 0o600)
        if self.cluster:
            assert self.postgres is not None
            self._run(self.dotnet, self.cli, "catalog", "migrate", "import", manager_policy, self.postgres, key)
        state.update({"identity": str(identity), "activePolicy": str(manager_policy), "standbyPolicy": str(standby_policy),
                      "workerPolicies": worker_policies, "catalogKey": str(key)})
        state["activePort"] = reserve_port()
        state["standbyPort"] = reserve_port() if self.cluster else None
        self._save(state)

    def _manager_command(self, state: dict[str, Any], name: str, policy: str, port: int) -> list[str]:
        command = [self.dotnet, self.management, "--policy", policy, "--worker", self.worker, "--node", name,
                   "--urls", f"https://localhost:{port}"]
        if self.cluster:
            command.extend(["--Cluster:ConnectionFile", str(self.postgres), "--Cluster:EncryptionKeyFile", state["catalogKey"]])
        return command

    def _start_managers(self, state: dict[str, Any]) -> None:
        active = self._launch(state, "manager-active", self._manager_command(state, "active", state["activePolicy"], state["activePort"]),
                              state["activePolicy"])
        state["managers"]["active"] = active
        state["activeUrl"] = f"https://localhost:{state['activePort']}"
        self._save(state)
        self._wait_ready(state["activeUrl"], state, "active")
        if self.cluster:
            standby = self._launch(state, "manager-standby", self._manager_command(state, "standby", state["standbyPolicy"], state["standbyPort"]),
                                   state["standbyPolicy"])
            state["managers"]["standby"] = standby
            state["standbyUrl"] = f"https://localhost:{state['standbyPort']}"
            self._save(state)
            self._wait_live(state["standbyUrl"], state, "standby")

    def _wait_live(self, url: str, state: dict[str, Any], name: str) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url + "/health/live", context=ssl.create_default_context(cafile=str(self.root / "ca.pem")), timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            if name in state.get("managers", {}) and not owned_process(state["managers"][name]):
                raise ClusterFailure(f"Management {name} exited during startup")
            time.sleep(0.15)
        raise ClusterFailure(f"Management {name} did not become live")

    def _wait_ready(self, url: str, state: dict[str, Any], name: str) -> None:
        self._wait_live(url, state, name)
        context = ssl.create_default_context(cafile=str(self.root / "ca.pem"))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url + "/health/ready", context=context, timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            if not owned_process(state["managers"][name]):
                raise ClusterFailure(f"Management {name} exited before becoming ready")
            time.sleep(0.15)
        raise ClusterFailure(f"Management {name} did not become ready")

    def _start_workers(self, state: dict[str, Any]) -> None:
        endpoints = [{"url": f"wss://localhost:{state['activePort']}/", "certificateSha256": state["serverPin"]}]
        if self.cluster:
            endpoints.append({"url": f"wss://localhost:{state['standbyPort']}/", "certificateSha256": state["serverPin"]})
        for name in self.worker_names:
            config = self.root / f"{name}-agent.json"
            json_write(config, {"managementEndpoints": endpoints, "nodeId": name,
                                "certificatePath": str(self.root / f"{name}.pfx"),
                                "policyPath": state["workerPolicies"][name], "maxCpu": 1, "maxProcesses": 1,
                                "maxMemoryBytes": 805306368, "tenants": [TENANT]})
            command = [self.dotnet, self.worker, "agent", config]
            self._launch(state, name, command, str(config))

    def _api(self, state: dict[str, Any], manager: str | None = None) -> Api:
        name = manager or state["activeManager"]
        return Api(state["activeUrl"] if name == "active" else state["standbyUrl"], self.root / "ca.pem")

    def _wait_membership(self, state: dict[str, Any], expected_online: bool = True,
                         manager: str | None = None) -> list[dict[str, Any]]:
        api = self._api(state, manager)
        if not api.csrf:
            api.login(state["password"])
        expected = set(self.worker_names)
        deadline = time.monotonic() + 40
        last: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            try:
                status, last = api.request("/api/workers")
                remote = [node for node in last if node.get("remote")]
                online = {node["nodeId"] for node in remote if node.get("online") and node.get("state") == "Online"}
                one_slot = all(node.get("maxProcesses") == 1 and node.get("maxCpu") == 1 for node in remote)
                if expected_online and online == expected and len(remote) == len(expected) and one_slot:
                    return remote
                if not expected_online and all(node["nodeId"] in expected for node in remote) and len(remote) == len(expected) and one_slot:
                    return remote
            except (OSError, urllib.error.HTTPError):
                pass
            for record in state.get("processes", {}).values():
                if record is not state.get("processes", {}).get("effects") and not owned_process(record):
                    # The active manager is allowed to be absent only after failover.
                    if state.get("activeManager") == "active" or record.get("identity", "").endswith("-agent.json"):
                        raise ClusterFailure(f"Owned process exited while waiting for membership: {record}")
            time.sleep(0.2)
        raise ClusterFailure(f"Worker membership did not converge to {sorted(expected)}; observed={last}")

    def _wait_state(self, api: Api, ticket: str, predicate: Callable[[dict[str, Any]], bool], seconds: float = 45) -> dict[str, Any]:
        deadline = time.monotonic() + seconds
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            status, last = api.request(f"/api/executions/{ticket}")
            if status == 200 and last is not None and predicate(last):
                return last
            time.sleep(0.15)
        raise ClusterFailure(f"Execution {ticket} did not converge: {last}")

    def _effect_status(self, state: dict[str, Any]) -> dict[str, Any]:
        with urllib.request.urlopen(state["effectUrl"] + "/status", timeout=3) as response:
            return json.loads(response.read())

    def _effect_read(self, state: dict[str, Any], key: str) -> dict[str, Any]:
        encoded = urllib.parse.quote(key, safe="")
        with urllib.request.urlopen(state["effectUrl"] + "/effect/" + encoded, timeout=3) as response:
            return json.loads(response.read())

    def _package(self, state: dict[str, Any], package_id: str) -> dict[str, Any]:
        return {"version": self._protocol_version(), "id": package_id, "parameters": {"payload": {"type": "utf8"}},
                "connections": [{"id": "effect", "driver": "http", "resource": state["effectUrl"] + "/effect"}],
                "controlFlow": {"tasks": [{"id": "post", "kind": "http.request", "config": {
                    "connection": "effect", "method": "POST", "body": {"$parameter": "payload"},
                    "timeoutMs": 30000, "responseHeaders": ["X-Trace"]}}], "constraints": []}, "dataFlows": []}

    @staticmethod
    def _protocol_version() -> int:
        # Keep the example aligned with the repository's product model without
        # copying a protocol number into the demo or importing test code.
        model = Path(__file__).resolve().parents[2] / "metadata" / "product.json"
        return int(json.loads(model.read_text(encoding="utf-8"))["product"]["protocolVersion"])

    def _enqueue_runs(self, state: dict[str, Any], prefix: str) -> tuple[Api, str, list[dict[str, Any]]]:
        api = self._api(state)
        api.login(state["password"])
        package_id = f"{prefix}-{secrets.token_hex(8)}"
        document = self._package(state, package_id)
        status, response = api.request("/api/versions", "POST", {"document": document, "expectedPackageRevision": "0"})
        if status != 200:
            raise ClusterFailure(f"Package publication failed: HTTP {status}: {response}")
        runs: list[dict[str, Any]] = []
        for worker_name in self.worker_names:
            key = f"{package_id}-{worker_name}-{secrets.token_hex(5)}"
            body = {"packageId": package_id, "packageRevision": "1", "requestId": key,
                    "parameters": json.dumps({"payload": key})}
            status, response = api.request("/api/executions", "POST", body)
            if status != 202:
                raise ClusterFailure(f"Execution enqueue failed: HTTP {status}: {response}")
            duplicate_status, duplicate = api.request("/api/executions", "POST", body)
            if duplicate_status != 202 or not isinstance(duplicate, dict) or duplicate.get("id") != response.get("id"):
                raise ClusterFailure(f"Durable request identity was not idempotent: first={response} duplicate={duplicate}")
            runs.append({"ticket": response["id"], "key": key})
        state.setdefault("runs", {})[prefix] = runs
        self._save(state)
        return api, package_id, runs

    def _wait_effects_and_capture(self, state: dict[str, Any], api: Api, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        keys = {run["key"] for run in runs}
        deadline = time.monotonic() + 35
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            try:
                status = self._effect_status(state)
                if keys.issubset(status.get("keys", [])) and status.get("maximumActive", 0) >= len(runs):
                    snapshots = [api.request(f"/api/executions/{run['ticket']}")[1] for run in runs]
                    if all(snapshot.get("executionId") and snapshot.get("processId", 0) > 0 and snapshot.get("workerNode") for snapshot in snapshots):
                        return snapshots
            except (OSError, urllib.error.HTTPError):
                pass
            time.sleep(0.1)
        raise ClusterFailure(f"External effects did not overlap and become observable: {status}")

    def _assert_common_runs(self, state: dict[str, Any], api: Api, runs: list[dict[str, Any]],
                            snapshots: list[dict[str, Any]], require_success: bool = True) -> list[dict[str, Any]]:
        nodes = {snapshot.get("workerNode") for snapshot in snapshots}
        if self.cluster:
            if nodes != set(self.worker_names):
                raise ClusterFailure(f"Cluster run was not placed on exactly the three remote Agents: {snapshots}")
        terminal: list[dict[str, Any]] = []
        for snapshot, run in zip(snapshots, runs):
            value = self._wait_state(api, run["ticket"], lambda item: item.get("state") not in {"Queued", "Claimed", "Running"})
            terminal.append(value)
            if require_success and value.get("state") != "Succeeded":
                raise ClusterFailure(f"Execution failed before failover: {value}")
            if value.get("executionId") != snapshot.get("executionId") or value.get("processId") != snapshot.get("processId") or value.get("workerNode") != snapshot.get("workerNode"):
                raise ClusterFailure(f"Run identity changed before failover: before={snapshot} after={value}")
            metrics_status, metrics = api.request(f"/api/executions/{run['ticket']}/metrics")
            tasks = metrics.get("diagnostics", {}).get("tasks", []) if metrics_status == 200 else []
            if tasks and (tasks[0].get("httpStatus") != 200 or "cluster-demo-effect" not in tasks[0].get("responseHeaders", "")):
                raise ClusterFailure(f"HTTP effect was not observable in Native diagnostics: {metrics}")
            readback = self._effect_read(state, run["key"])
            if readback.get("key") != run["key"]:
                raise ClusterFailure(f"Durable external readback mismatch for {run['key']}: {readback}")
        return terminal

    def verify(self) -> dict[str, Any]:
        state = self._load()
        manager = state["activeManager"]
        if manager != "active":
            raise ClusterFailure("verify must run before failover; the active Manager is already switched")
        membership = self._wait_membership(state, manager=manager)
        api, package_id, runs = self._enqueue_runs(state, "verify")
        snapshots = self._wait_effects_and_capture(state, api, runs)
        terminal = self._assert_common_runs(state, api, runs, snapshots)
        attempts = self._effect_status(state).get("attempts")
        state["verify"] = {"packageId": package_id, "runs": runs, "before": snapshots, "terminal": terminal,
                            "membership": membership, "attempts": attempts}
        self._save(state)
        print(json.dumps({"result": "PASS", "phase": "verify", "remoteAgents": sorted({item["workerNode"] for item in terminal}),
                          "runs": [{"ticket": run["ticket"], "executionId": value["executionId"],
                                    "processId": value["processId"], "workerNode": value["workerNode"]}
                                   for run, value in zip(runs, terminal)],
                          "maximumConcurrentEffects": self._effect_status(state)["maximumActive"]}, sort_keys=True))
        return state

    def _resume_recovery(self, state: dict[str, Any], api: Api, run: dict[str, Any], before: dict[str, Any]) -> dict[str, Any]:
        deadline = time.monotonic() + 10
        result: Any = None
        while time.monotonic() < deadline:
            status, result = api.request(f"/api/executions/{run['ticket']}/resume", "POST", allow_error=True)
            if status == 202:
                if result.get("executionId") != before.get("executionId"):
                    raise ClusterFailure(f"Recovery changed Native execution identity: {result}")
                break
            if status != 400:
                raise ClusterFailure(f"Unexpected RecoveryRequired resume response: HTTP {status}: {result}")
            time.sleep(0.2)
        else:
            raise ClusterFailure(f"Original RecoveryRequired run could not be resumed: {result}")
        return self._wait_state(api, run["ticket"], lambda item: item.get("state") in {"Succeeded", "RecoveryRequired"}, 30)

    def failover(self) -> dict[str, Any]:
        state = self._load()
        if not self.cluster:
            raise ClusterFailure("failover requires HA mode with the dedicated PostgreSQL Catalog")
        if state.get("activeManager") != "active":
            raise ClusterFailure("This instance has already failed over")
        self._wait_membership(state, manager="active")
        api, package_id, runs = self._enqueue_runs(state, "failover")
        snapshots = self._wait_effects_and_capture(state, api, runs)
        pre_members = self._wait_membership(state, manager="active")
        active_record = state["managers"]["active"]
        if not owned_process(active_record):
            raise ClusterFailure(f"The active Manager is not owned by this demo: {active_record}")
        terminate_owned(active_record, force=True)
        state["activeManager"] = "standby"
        state["failover"] = {"packageId": package_id, "runs": runs, "before": snapshots, "preMembers": pre_members,
                              "activeManagerPid": active_record["pid"]}
        self._save(state)
        self._wait_ready(state["standbyUrl"], state, "standby")
        standby_api = self._api(state, manager="standby")
        # The active session must validate on the standby.  This proves that
        # identities and the Data Protection key ring came from the shared
        # Catalog, rather than from either Management's private directory.
        for cookie in api.jar:
            standby_api.jar.set_cookie(cookie)
        standby_api.csrf = api.csrf
        session_status, session = standby_api.request("/api/auth/session", allow_error=True)
        if session_status != 200 or session.get("subject") != "operator":
            raise ClusterFailure(f"Shared Catalog identity/key session was not accepted after failover: {session_status} {session}")
        post_members = self._wait_membership(state, manager="standby")
        last_heartbeat = {item["nodeId"]: item["lastHeartbeatMs"] for item in pre_members}
        if any(item["lastHeartbeatMs"] <= last_heartbeat[item["nodeId"]] for item in post_members):
            raise ClusterFailure(f"Not all three Worker Agents reconnected after failover: {post_members}")
        terminal: list[dict[str, Any]] = []
        recovery = 0
        for before, run in zip(snapshots, runs):
            value = self._wait_state(standby_api, run["ticket"], lambda item: item.get("state") not in {"Queued", "Claimed", "Running"}, 50)
            resuming = value.get("state") == "RecoveryRequired"
            if resuming:
                recovery += 1
                value = self._resume_recovery(state, standby_api, run, before)
            if value.get("state") not in {"Succeeded", "RecoveryRequired"}:
                raise ClusterFailure(f"Execution failed after failover: {value}")
            if value.get("executionId") != before.get("executionId") or value.get("workerNode") != before.get("workerNode"):
                raise ClusterFailure(f"Failover changed run identity/node: before={before} after={value}")
            if not resuming and value.get("processId") != before.get("processId"):
                raise ClusterFailure(f"Failover changed remote Worker PID: before={before} after={value}")
            if value.get("state") == "Succeeded":
                metrics_status, metrics = standby_api.request(f"/api/executions/{run['ticket']}/metrics")
                tasks = metrics.get("diagnostics", {}).get("tasks", []) if metrics_status == 200 else []
                if tasks and (tasks[0].get("httpStatus") != 200 or "cluster-demo-effect" not in tasks[0].get("responseHeaders", "")):
                    raise ClusterFailure(f"Failover HTTP effect was not observable in Native diagnostics: {metrics}")
            readback = self._effect_read(state, run["key"])
            if readback.get("key") != run["key"]:
                raise ClusterFailure(f"Failover readback mismatch: {readback}")
            terminal.append(value)
        effect_status = self._effect_status(state)
        attempts = effect_status.get("attempts", 0)
        if attempts != len(runs) + len(state.get("verify", {}).get("runs", [])):
            raise ClusterFailure(f"External effects were duplicated during failover: {effect_status}")
        state["failover"].update({"terminal": terminal, "postMembers": post_members, "recoveryRequired": recovery,
                                   "sessionContinuity": True, "attempts": attempts})
        self._save(state)
        print(json.dumps({"result": "PASS", "phase": "failover", "activeManagerKilledPid": active_record["pid"],
                          "reconnectedAgents": sorted(item["nodeId"] for item in post_members),
                          "sameRunPidNode": recovery == 0, "recoveryRequired": recovery,
                          "automaticReplay": False, "sessionContinuity": True, "effectAttempts": attempts,
                          "runs": [{"ticket": run["ticket"], "executionId": value["executionId"],
                                    "processId": value.get("processId"), "workerNode": value["workerNode"],
                                    "state": value["state"]}
                                   for run, value in zip(runs, terminal)]}, sort_keys=True))
        return state

    def stop(self) -> dict[str, Any]:
        state = self._load()
        self._stop_state(state, quiet=False)
        state["stoppedAtMs"] = int(time.time() * 1000)
        self._save(state)
        print(f"PASS: stopped only processes owned by {self.root}; artifacts and logs retained")
        return state

    def _stop_state(self, state: dict[str, Any], quiet: bool) -> None:
        records = state.get("processes", {})
        # Managers first fences dispatch, then Agents and finally the effect sink.
        order = ["manager-active", "manager-standby", *self.worker_names, "effects"]
        errors: list[str] = []
        for name in order:
            record = records.get(name)
            if not record:
                continue
            if not owned_process(record):
                if _linux_process_identity(int(record["pid"])) is not None and not quiet:
                    errors.append(f"{name}: PID {record['pid']} is alive but no longer matches this demo instance")
                continue
            try:
                terminate_owned(record)
            except (OSError, ClusterFailure) as error:
                errors.append(f"{name}: {error}")
        if errors and not quiet:
            self._print_logs(state)
            raise ClusterFailure("Some owned processes did not stop: " + "; ".join(errors))

    def _print_logs(self, state: dict[str, Any]) -> None:
        for record in state.get("processes", {}).values():
            path = Path(record.get("log", ""))
            content = tail(path)
            if content:
                print(f"--- {path} ---\n{content}", file=sys.stderr)

    def run_full(self) -> dict[str, Any]:
        self.start()
        try:
            self.verify()
            return self.failover() if self.cluster else self._load()
        finally:
            self.stop()


def json_write_path(path: Path, value: Any) -> Path:
    json_write(path, value)
    return path


def load_executable_arguments(arguments: argparse.Namespace, state: dict[str, Any] | None = None) -> dict[str, str]:
    if state is not None:
        return {name: state[name] for name in ("dotnet", "management", "worker", "openssl")}
    missing = [name for name in ("dotnet", "management", "worker", "openssl") if not getattr(arguments, name, None)]
    if missing:
        raise ClusterFailure("start requires: " + ", ".join("--" + name for name in missing))
    resolved: dict[str, str] = {}
    for name in ("dotnet", "management", "worker", "openssl"):
        supplied = str(getattr(arguments, name))
        resolved[name] = shutil.which(supplied) or str(Path(supplied).resolve())
    return resolved


def run_cli(arguments: argparse.Namespace) -> int:
    if arguments.action == "effects-server":
        return EffectStore.serve(arguments.directory, arguments.port)
    state_dir = Path(arguments.state_dir).resolve()
    if arguments.action == "start":
        values = load_executable_arguments(arguments)
        harness = ClusterHarness(**values, root=state_dir, postgres=arguments.postgres, cluster=True)
        state = harness.start()
        state.update(values)
        json_write(state_dir / "state.json", state)
        return 0
    state_file = state_dir / "state.json"
    if not state_file.is_file():
        raise ClusterFailure(f"Cluster state does not exist: {state_file}; run start first")
    stored = json.loads(state_file.read_text(encoding="utf-8"))
    values = load_executable_arguments(arguments, stored)
    harness = ClusterHarness(**values, root=state_dir, postgres=stored.get("postgres"), cluster=True)
    if arguments.action == "verify":
        harness.verify()
    elif arguments.action == "failover":
        harness.failover()
    elif arguments.action == "stop":
        harness.stop()
    else:
        raise ClusterFailure(f"Unknown action: {arguments.action}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UniversalData real 2-Management/3-Agent cluster demo")
    parser.add_argument("action", choices=("start", "verify", "failover", "stop", "effects-server"))
    parser.add_argument("--state-dir", default="/tmp/universaldata-worker-cluster")
    parser.add_argument("--postgres", help="dedicated PostgreSQL 17 connection-file for start")
    parser.add_argument("--dotnet")
    parser.add_argument("--management")
    parser.add_argument("--worker")
    parser.add_argument("--openssl", default="openssl")
    parser.add_argument("--directory")
    parser.add_argument("--port", type=int)
    arguments = parser.parse_args(argv)
    if arguments.action == "effects-server" and (not arguments.directory or not arguments.port):
        parser.error("effects-server requires --directory and --port")
    try:
        return run_cli(arguments)
    except (ClusterFailure, OSError, urllib.error.URLError, subprocess.SubprocessError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
