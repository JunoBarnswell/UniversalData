"""Restore both exact runtime dependencies and execute the .NET 8 SDK on the current platform."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PRODUCT = json.loads((ROOT / "metadata/product.json").read_text(encoding="utf-8"))["product"]
VERSION = PRODUCT["sdkVersion"]


def archives(source):
    names = ("UniversalData", "UniversalData.Runtime.linux-x64", "UniversalData.Runtime.win-x64")
    required = {f"{name}.{VERSION}.nupkg" for name in names}
    required.update(f"{package.lower()}.{locked}.nupkg"
                    for package, locked in PRODUCT["dotnetPackages"].items())
    files = sorted(source.glob("*.nupkg"))
    if missing := required - {path.name for path in files}:
        raise FileNotFoundError("The isolated feed is missing " + ", ".join(sorted(missing)))
    return files


def hashes(source):
    results = []
    for path in archives(source):
        with path.open("rb") as package:
            digest = hashlib.file_digest(package, "sha256").hexdigest()
        results.append({"name": path.name, "bytes": path.stat().st_size, "sha256": digest})
    return results


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
        shutil.copyfile(ROOT / "tests/nuget_consumer/Program.cs", work / "Program.cs")
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
    if not facts["framework"].startswith("8.") or facts["version"] != VERSION or facts["http"] != 200 or facts["sqliteRows"] != 35:
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
    checked = hashes(source)
    facts = run(args, source)
    facts["archives"] = checked
    facts["sourceCommit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    args.evidence.write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    print(f".NET {facts['framework']} from a local NuGet feed on {facts['rid']} passed for {', '.join(facts['drivers'])}")


if __name__ == "__main__":
    main()
