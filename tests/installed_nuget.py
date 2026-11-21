"""Restore both exact runtime dependencies and execute the .NET 8 SDK on the current platform."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PRODUCT = json.loads((ROOT / "metadata/product.json").read_text(encoding="utf-8"))["product"]
VERSION = PRODUCT["sdkVersion"]
RUNTIME_DRIVERS = ("sqlite", "postgresql", "mysql", "mariadb", "odbc", "oracle_oci",
                   "kafka", "rabbitmq", "rocketmq", "activemq")


def archives(source, runtime_rids=("linux-x64", "win-x64")):
    names = ("UniversalData", *(f"UniversalData.Runtime.{rid}" for rid in runtime_rids))
    required = {f"{name}.{VERSION}.nupkg" for name in names}
    required.update(f"{package.lower()}.{locked}.nupkg"
                    for package, locked in PRODUCT["dotnetPackages"].items())
    files = sorted(source.glob("*.nupkg"))
    if missing := required - {path.name for path in files}:
        raise FileNotFoundError("The isolated feed is missing " + ", ".join(sorted(missing)))
    return files


def hashes(source, runtime_rids):
    results = []
    for path in archives(source, runtime_rids):
        with path.open("rb") as package:
            digest = hashlib.file_digest(package, "sha256").hexdigest()
        results.append({"name": path.name, "bytes": path.stat().st_size, "sha256": digest})
    return results


def validate_runtime_archive(source, rid, complete):
    """Check the native plugin closure that the installed Worker will load."""
    archive = source / f"UniversalData.Runtime.{rid}.{VERSION}.nupkg"
    extension = ".dll" if rid == "win-x64" else ".so"
    worker = "ud_worker.exe" if rid == "win-x64" else "ud_worker"
    native_prefix = f"runtimes/{rid}/native/"
    worker_prefix = f"runtimes/{rid}/worker/"
    drivers = RUNTIME_DRIVERS if complete else ("sqlite",)
    with zipfile.ZipFile(archive) as package:
        files = set(package.namelist())
        required = {worker_prefix + worker} | {
            native_prefix + f"ud_connector_{driver}{extension}"
            for driver in drivers}
        native_assets = {name[len(native_prefix):] for name in files
                         if name.startswith(native_prefix) and name != native_prefix}
        worker_assets = {name[len(worker_prefix):] for name in files
                         if name.startswith(worker_prefix) and name != worker_prefix}
        licenses = {name for name in files if name.startswith("licenses/") and not name.endswith("/")}
        if (not required <= files or not native_assets or not licenses
                or not native_assets <= worker_assets):
            raise ValueError(f"The {rid} runtime package does not contain its actual plugin closure and notices")
        for entry in required - {worker_prefix + worker}:
            if not package.getinfo(entry).file_size:
                raise ValueError(f"The {rid} runtime package contains an empty plugin: {entry}")


def run(args, source):
    env = dict(os.environ)
    env.pop("UD_WORKER_PATH", None)
    env.pop("LD_LIBRARY_PATH", None)
    with tempfile.TemporaryDirectory(prefix="ud-installed-nuget-") as folder:
        work = Path(folder)
        env["NUGET_PACKAGES"] = str(work / "packages")
        env["NUGET_HTTP_CACHE_PATH"] = str(work / "http-cache")
        project = work / "InstalledSdk.csproj"
        project.write_text('''<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework><OutputType>Exe</OutputType>
    <ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup><PackageReference Include="UniversalData" Version="[''' + VERSION + ''']" /></ItemGroup>
</Project>
''', encoding="utf-8")
        for source_file in (ROOT / "tests/nuget_consumer").glob("*.cs"):
            shutil.copyfile(source_file, work / source_file.name)
        subprocess.run([args.dotnet, "restore", project, "--source", source, "--use-lock-file",
                        "-p:TreatWarningsAsErrors=true"], cwd=work, env=env, check=True, timeout=240)
        lock_hash = hashlib.sha256((work / "packages.lock.json").read_bytes()).hexdigest()
        subprocess.run([args.dotnet, "build", project, "-c", "Release", "--no-restore",
                        "-p:TreatWarningsAsErrors=true"], cwd=work, env=env, check=True, timeout=240)
        assembly = work / "bin/Release/net8.0/InstalledSdk.dll"
        command = [args.dotnet, str(assembly)] + (["--complete"] if args.complete else [])
        output = subprocess.check_output(command, cwd=work, env=env, text=True, timeout=120)
    facts = json.loads(output)
    facts["dependencyLockSha256"] = lock_hash
    expected_drivers = set(RUNTIME_DRIVERS) if args.complete else {"sqlite"}
    expected_brokers = {"kafka", "rabbitmq", "rocketmq", "activemq-classic", "activemq-artemis"} if args.complete else set()
    mq = facts.get("mq", {})
    mq_ok = set(mq) == expected_brokers and all(
        item.get("publishedCommitted") == 2 and item.get("consumedCommitted") == 2 and
        item.get("consumedAcknowledged") == 2 for item in mq.values())
    if (not facts["framework"].startswith("8.") or facts["version"] != VERSION
            or facts["http"] != 200 or facts["sqliteRows"] != 35
            or set(facts.get("drivers", [])) != expected_drivers
            or set(facts.get("brokers", [])) != expected_brokers or not mq_ok):
        raise ValueError(f"Installed SDK facts differ: {facts}")
    return facts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--dotnet", default="dotnet")
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--complete", action="store_true")
    args = parser.parse_args()
    source = args.source.resolve()
    rid = "win-x64" if os.name == "nt" else "linux-x64"
    required_rids = ("linux-x64", "win-x64") if args.complete else (rid,)
    validate_runtime_archive(source, rid, args.complete)
    checked = hashes(source, required_rids)
    facts = run(args, source)
    facts["archives"] = checked
    facts["sourceCommit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    args.evidence.write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    print(f".NET {facts['framework']} from a local NuGet feed on {facts['rid']} passed for {', '.join(facts['drivers'])}")


if __name__ == "__main__":
    main()
