"""Use only per-run SQLite files or explicitly provided external SQL fixtures."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from product_version import PROTOCOL_VERSION


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plugin", required=True)
    parser.add_argument("--native-test")
    parser.add_argument("--pool-test")
    parser.add_argument("--dotnet")
    parser.add_argument("--assembly")
    parser.add_argument("--provider", default="sqlite")
    parser.add_argument("--resource")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="ud-v14-sql-tests-") as temporary:
        directory = Path(temporary)
        sqlite = directory / "fixture.db"
        if args.pool_test:
            package = {"version": PROTOCOL_VERSION, "id": "pool-fixture",
                       "connections": [{"id": "db", "driver": str(Path(args.plugin).resolve()),
                                        "resource": "pool-resource", "credentialReference": "database"}],
                       "controlFlow": {"tasks": [], "constraints": []}, "dataFlows": []}
            return subprocess.run([args.pool_test, json.dumps(package), str(sqlite),
                                   str(directory / "rotated.db")], check=False).returncode
        if args.native_test:
            return subprocess.run([args.native_test, args.plugin, str(sqlite)], check=False).returncode
        if not args.dotnet or not args.assembly:
            parser.error("Provide a Native test or the ManagedSql host and assembly")
        if args.resource:
            fixture = os.environ.get(args.resource)
            if not fixture or not Path(fixture).is_file():
                print(f"Blocked: isolated SQL resource {args.resource} is unavailable")
                return 77
        else:
            fixture = directory / "sqlite.conf"
            fixture.write_text(f"Data Source={sqlite}", encoding="utf-8")
        command = [args.dotnet, args.assembly, "--provider", args.provider,
                   "--plugin", args.plugin, "--connection-file", str(fixture)]
        return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
