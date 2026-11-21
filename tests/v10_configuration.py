"""Shared v10 YAML/SDK configuration and CLI boundary acceptance."""

from __future__ import annotations

import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import threading
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def start_server() -> tuple[ThreadingHTTPServer, type[BaseHTTPRequestHandler], str]:
    class Handler(BaseHTTPRequestHandler):
        requests: list[str] = []
        posts: list[dict[str, Any]] = []

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            type(self).requests.append(self.path)
            if self.path.split("?", 1)[0] != "/probe":
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"probe-ok")

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
                chunks: list[bytes] = []
                while True:
                    size_line = self.rfile.readline().split(b";", 1)[0].strip()
                    size = int(size_line, 16)
                    if size == 0:
                        while self.rfile.readline().strip():
                            pass
                        break
                    chunks.append(self.rfile.read(size))
                    self.rfile.read(2)
                raw = b"".join(chunks)
            else:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
            body = json.loads(raw.decode("utf-8")) if raw else None
            type(self).posts.append({"path": self.path, "body": body})
            if self.path != "/api/publications":
                self.send_response(404)
                self.end_headers()
                return
            response = json.dumps({
                "id": "v10-configured",
                "revision": "7",
                "deployment": {},
                "occurrence": None,
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *_: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, Handler, f"http://127.0.0.1:{server.server_port}"


def scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return json.dumps(value, ensure_ascii=False)
    return json.dumps(str(value), ensure_ascii=False)


def yaml_mapping(mapping: dict[str, Any], indent: int = 0) -> list[str]:
    """Render config mappings as YAML while preserving package JSON exactly."""
    lines: list[str] = []
    padding = " " * indent
    for key, value in mapping.items():
        if isinstance(value, dict):
            if not value:
                lines.append(f"{padding}{key}: {{}}")
            else:
                lines.append(f"{padding}{key}:")
                lines.extend(yaml_mapping(value, indent + 2))
        elif isinstance(value, list):
            if not value:
                lines.append(f"{padding}{key}: []")
            else:
                lines.append(f"{padding}{key}:")
                for item in value:
                    encoded = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
                    lines.append(f"{' ' * (indent + 2)}- {encoded}")
        else:
            lines.append(f"{padding}{key}: {scalar(value)}")
    return lines


def write_config(
    path: pathlib.Path,
    *,
    policy: pathlib.Path,
    worker: pathlib.Path,
    database: pathlib.Path,
    probe_origin: str,
    management_origin: str,
    assembly: pathlib.Path | None = None,
    package: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
    parameter_defaults: dict[str, Any] | None = None,
    duplicate_jobs: bool = False,
    unknown_execution: bool = False,
    unknown_root: bool = False,
    unknown_source_kind: bool = False,
) -> None:
    source_kind = "not-a-driver" if unknown_source_kind else "sqlite"
    root: dict[str, Any] = {
        "hostPolicy": {"path": str(policy), "tenant": "local"},
        "worker": {"workerPath": str(worker)},
        "sources": {
            "orders": {
                "kind": source_kind,
                "connectionString": f"Data Source={database}",
                "resource": str(database),
                "credentialReference": "orders",
            },
            "api": {"kind": "http", "baseUri": probe_origin},
        },
        "management": {"baseUrl": management_origin},
    }
    if assembly is not None:
        root["assemblies"] = [str(assembly)]
    if settings is not None:
        root["jobs"] = {package["id"] if package is not None else "v10-configured": dict(settings)}
    elif package is not None:
        job = dict(package)
        if parameter_defaults is not None:
            job["parameterDefaults"] = parameter_defaults
        if unknown_execution:
            job["execution"] = {"unrecognized": 1}
        if duplicate_jobs:
            root["jobs"] = [job, dict(job)]
        else:
            root["jobs"] = {package["id"]: job}
    if unknown_root:
        root["notAConfigurationOption"] = True
    path.write_text("\n".join(yaml_mapping(root)) + "\n", encoding="utf-8")


def invoke_cli(command: list[str], args: list[str], environment: dict[str, str],
               expected: int = 0) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command + args, env=environment, capture_output=True,
                            text=True, timeout=90)
    if result.returncode != expected:
        raise AssertionError(
            f"CLI {' '.join(args)} returned {result.returncode}, expected {expected}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}")
    return result


def package_from(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def result_metrics(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    document = json.loads(result.stdout)
    metrics = document.get("metrics") or document.get("Metrics")
    if not isinstance(metrics, dict):
        raise AssertionError(f"CLI did not return DataJobResult metrics: {document}")
    return metrics


def read_output(database: pathlib.Path) -> list[tuple[int, int]]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute("SELECT id, value FROM output ORDER BY id").fetchall()


def assert_successful_result(result: subprocess.CompletedProcess[str], expected_delta: int,
                             database: pathlib.Path) -> None:
    metrics = result_metrics(result)
    committed = metrics.get("committedRows", metrics.get("CommittedRows"))
    rejected = metrics.get("rejectedRows", metrics.get("RejectedRows"))
    if committed != 2 or rejected != 0:
        raise AssertionError(f"unexpected execution metrics: {metrics}")
    expected = [(1, 10 + expected_delta), (2, 20 + expected_delta)]
    actual = read_output(database)
    if actual != expected:
        raise AssertionError(f"expected SQLite output {expected}, got {actual}")


def assert_rejected(command: list[str], args: list[str], environment: dict[str, str],
                    text: str) -> None:
    result = invoke_cli(command, args, environment, expected=2)
    combined = result.stdout + result.stderr
    if text not in combined:
        raise AssertionError(f"expected '{text}' in rejection output:\n{combined}")


def main() -> None:
    if len(sys.argv) != 6:
        raise SystemExit("usage: v10_configuration.py DOTNET CLI WORKER SQLITE_PLUGIN JOBS_DLL")
    dotnet, cli, worker, sqlite_plugin, jobs_dll = map(pathlib.Path, sys.argv[1:])
    cli_command = [str(cli)] if cli.suffix != ".dll" else [str(dotnet), str(cli)]
    library_directories = [sqlite_plugin.resolve().parent, worker.resolve().parent, cli.resolve().parent]
    environment = dict(os.environ)
    for parameter_name in (
        "UD_PARAMETER_V10CONFIGURED_DELTA", "UNIVERSALDATA_PARAMETER_V10CONFIGURED_DELTA",
        "UD_PARAM_V10CONFIGURED_DELTA", "UNIVERSALDATA_PARAM_V10CONFIGURED_DELTA",
        "UD_V10CONFIGURED_DELTA", "UNIVERSALDATA_V10CONFIGURED_DELTA",
    ):
        environment.pop(parameter_name, None)
    if os.name == "nt":
        environment["PATH"] = os.pathsep.join(map(str, library_directories)) + os.pathsep + environment.get("PATH", "")
    else:
        environment["LD_LIBRARY_PATH"] = os.pathsep.join(map(str, library_directories)) + os.pathsep + environment.get("LD_LIBRARY_PATH", "")

    probe_server, probe_handler, probe_origin = start_server()
    management_server, management_handler, management_origin = start_server()
    try:
        with tempfile.TemporaryDirectory(prefix="ud-v10-config-") as temporary:
            root = pathlib.Path(temporary)
            resources = root / "resources"
            resources.mkdir()
            database = resources / "orders.db"
            credential = root / "orders.credential"
            credential.write_text(str(database), encoding="utf-8")
            with closing(sqlite3.connect(database)) as connection, connection:
                connection.execute("CREATE TABLE input(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
                connection.execute("CREATE TABLE output(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
                connection.executemany("INSERT INTO input VALUES(?, ?)", [(1, 10), (2, 20)])

            policy = root / "host-policy.json"
            policy.write_text(json.dumps({
                "dataDirectory": str(root / "state"),
                "fileRoots": [str(resources)],
                "networkOrigins": [probe_origin],
                "pluginRoots": [str(directory) for directory in library_directories],
                "credentialFiles": {"orders": str(credential)},
            }), encoding="utf-8")
            csharp_config = root / "csharp.yaml"
            write_config(csharp_config, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         assembly=jobs_dll)

            csharp_package_path = root / "csharp-package.json"
            invoke_cli(cli_command, ["export", "v10-configured", "--config", str(csharp_config),
                                     "--output", str(csharp_package_path)], environment)
            csharp_package = package_from(csharp_package_path)
            if csharp_package.get("id") != "v10-configured":
                raise AssertionError(f"C# export has the wrong id: {csharp_package}")
            tasks = csharp_package.get("controlFlow", {}).get("tasks", [])
            flows = csharp_package.get("dataFlows", [])
            if not tasks or tasks[0].get("id") != "flow-1" or not flows or flows[0].get("id") != "flow-1":
                raise AssertionError(f"first DataJob flow was not named flow-1: {csharp_package}")
            component_ids = [component.get("id") for component in flows[0].get("components", [])]
            if component_ids != ["read", "normalize", "write"]:
                raise AssertionError(f"staged DataJob steps changed: {component_ids}")

            yaml_config = root / "universaldata.yaml"
            write_config(yaml_config, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=csharp_package, parameter_defaults={"Delta": 1})
            yaml_package_path = root / "yaml-package.json"
            invoke_cli(cli_command, ["export", "v10-configured", "--config", str(yaml_config),
                                     "--output", str(yaml_package_path)], environment)
            yaml_package = package_from(yaml_package_path)
            if yaml_package != csharp_package:
                raise AssertionError("YAML and C# exports are not the same canonical Native package")

            params = root / "parameters.json"
            params.write_text(json.dumps({"v10-configured": {"Delta": 2}}), encoding="utf-8")
            csharp_run = invoke_cli(cli_command, ["run", "v10-configured", "--config", str(csharp_config),
                                                  "--params-file", str(params)], environment)
            assert_successful_result(csharp_run, 2, database)
            yaml_params_run = invoke_cli(cli_command, ["run", "v10-configured", "--config", str(yaml_config),
                                                       "--params-file", str(params)], environment)
            assert_successful_result(yaml_params_run, 2, database)
            yaml_default_run = invoke_cli(cli_command, ["run", "v10-configured", "--config", str(yaml_config)], environment)
            assert_successful_result(yaml_default_run, 1, database)

            environment_override = dict(environment)
            environment_override["UD_PARAMETER_V10CONFIGURED_DELTA"] = "3"
            yaml_environment_run = invoke_cli(cli_command, ["run", "v10-configured", "--config", str(yaml_config),
                                                            "--params-file", str(params)], environment_override)
            assert_successful_result(yaml_environment_run, 3, database)
            yaml_cli_run = invoke_cli(cli_command, ["run", "v10-configured", "--config", str(yaml_config),
                                                    "--params-file", str(params), "--set", "v10-configured.Delta=4"],
                                      environment_override)
            assert_successful_result(yaml_cli_run, 4, database)

            typed = subprocess.run([str(dotnet), str(jobs_dll), "per-call", str(csharp_config), "5"],
                                   env=environment, capture_output=True, text=True, timeout=90)
            if typed.returncode != 0:
                raise AssertionError(f"typed C# run failed: {typed.stdout}\n{typed.stderr}")
            assert_successful_result(typed, 5, database)

            if probe_handler.requests != ["/probe"] * 6:
                raise AssertionError(f"expected six Native HTTP calls, got {probe_handler.requests}")
            if management_handler.requests:
                raise AssertionError(f"local run contacted Management: {management_handler.requests}")

            settings = {
                "parameterDefaults": {"Delta": 1},
                "execution": {"batchRows": 64, "parallelism": 1},
                "schedules": [{
                    "id": "hourly", "frequency": "Interval",
                    "startAt": "2035-01-01T00:00:00Z", "enabled": True,
                    "intervalSeconds": 3600,
                }],
                "expectedPackageRevision": "7",
            }
            settings_csharp_config = root / "settings-csharp.yaml"
            write_config(settings_csharp_config, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         assembly=jobs_dll, settings=settings)
            settings_package_path = root / "settings-package.json"
            invoke_cli(cli_command, ["export", "v10-configured", "--config", str(settings_csharp_config),
                                     "--output", str(settings_package_path)], environment)
            settings_package = package_from(settings_package_path)
            settings_policy = settings_package.get("policy", {})
            settings_execution = settings_policy.get("execution", {})
            if settings_policy.get("maxConcurrency") != 1 or settings_execution.get("batchRows") != 64:
                raise AssertionError(f"settings-only execution policy was not compiled: {settings_package}")

            settings_yaml_config = root / "settings-yaml.yaml"
            write_config(settings_yaml_config, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=settings_package, parameter_defaults={"Delta": 1})
            settings_yaml_package_path = root / "settings-yaml-package.json"
            invoke_cli(cli_command, ["export", "v10-configured", "--config", str(settings_yaml_config),
                                     "--output", str(settings_yaml_package_path)], environment)
            if package_from(settings_yaml_package_path) != settings_package:
                raise AssertionError("settings-only C# and YAML exports are not the same canonical package")

            settings_csharp_run = invoke_cli(cli_command, ["run", "v10-configured",
                                                            "--config", str(settings_csharp_config)], environment)
            assert_successful_result(settings_csharp_run, 1, database)
            settings_yaml_run = invoke_cli(cli_command, ["run", "v10-configured",
                                                         "--config", str(settings_yaml_config),
                                                         "--set", "v10-configured.Delta=2"], environment)
            assert_successful_result(settings_yaml_run, 2, database)
            if probe_handler.requests != ["/probe"] * 8:
                raise AssertionError(f"expected eight Native HTTP calls after settings-only runs, got {probe_handler.requests}")
            if management_handler.posts:
                raise AssertionError(f"local export/run contacted Management: {management_handler.posts}")

            publish = invoke_cli(cli_command, ["publish", "v10-configured",
                                               "--config", str(settings_csharp_config),
                                               "--request-id", "settings-request"], environment)
            if publish.returncode != 0:
                raise AssertionError(f"settings-only publish failed: {publish.stdout}\n{publish.stderr}")
            if len(management_handler.posts) != 1:
                raise AssertionError(f"expected one Management publication, got {management_handler.posts}")
            publication = management_handler.posts[0]
            if publication["path"] != "/api/publications":
                raise AssertionError(f"unexpected Management path: {publication}")
            publication_body = publication["body"]
            if publication_body.get("expectedPackageRevision") != "7" or publication_body.get("requestId") != "settings-request":
                raise AssertionError(f"publication settings were not forwarded: {publication_body}")
            if [schedule.get("id") for schedule in publication_body.get("schedules", [])] != ["hourly"]:
                raise AssertionError(f"publication schedule was not forwarded: {publication_body}")
            if json.loads(publication_body.get("parameters", "{}")) != {"Delta": 1}:
                raise AssertionError(f"scheduled publication parameters were not forwarded: {publication_body}")

            duplicate_jobs = root / "duplicate-jobs.yaml"
            write_config(duplicate_jobs, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=csharp_package, parameter_defaults={"Delta": 1}, duplicate_jobs=True)
            assert_rejected(cli_command, ["export", "v10-configured", "--config", str(duplicate_jobs)],
                            environment, "Duplicate job")

            interchange = root / "duplicate-interchange.yaml"
            write_config(interchange, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         assembly=jobs_dll, package=csharp_package, parameter_defaults={"Delta": 1})
            assert_rejected(cli_command, ["export", "v10-configured", "--config", str(interchange)],
                            environment, "both YAML and a configured C# assembly")

            unknown_type = root / "unknown-source.yaml"
            write_config(unknown_type, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=csharp_package, parameter_defaults={"Delta": 1}, unknown_source_kind=True)
            assert_rejected(cli_command, ["export", "v10-configured", "--config", str(unknown_type)],
                            environment, "Unknown data source kind")

            unknown_option = root / "unknown-option.yaml"
            write_config(unknown_option, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=csharp_package, parameter_defaults={"Delta": 1}, unknown_execution=True)
            assert_rejected(cli_command, ["export", "v10-configured", "--config", str(unknown_option)],
                            environment, "unknown field")

            unknown_root = root / "unknown-root.yaml"
            write_config(unknown_root, policy=policy, worker=worker, database=database,
                         probe_origin=probe_origin, management_origin=management_origin,
                         package=csharp_package, parameter_defaults={"Delta": 1}, unknown_root=True)
            assert_rejected(cli_command, ["export", "v10-configured", "--config", str(unknown_root)],
                            environment, "unknown field")

            print("v10 YAML/C# package parity, SQLite+HTTP run, settings-only policy/schedule publication, parameter precedence, duplicate and unknown-boundary rejection passed")
    finally:
        probe_server.shutdown()
        management_server.shutdown()
        probe_server.server_close()
        management_server.server_close()


if __name__ == "__main__":
    main()
