"""Verify sealed core evidence from the authoritative product model."""
import argparse
import hashlib
import json
import os
import zipfile
import pathlib
import shutil
import subprocess
import sys
from xml.etree import ElementTree as ET
from product_model import generate, read_model

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_DRIVERS = ("sqlite", "postgresql", "mysql", "mariadb", "odbc", "oracle_oci",
                   "kafka", "rabbitmq", "rocketmq", "activemq")

def source_hash():
    files = [ROOT / name for name in (
        "CMakeLists.txt", "CMakePresets.json", "VERSION", ".github/workflows/ci.yml",
        "Directory.Build.props", "Directory.Build.targets", "UniversalData.sln", "global.json",
        ".github/workflows/publish-nuget.yml")]
    for folder in ("include", "components", "engines", "connectors", "catalog", "plugins",
                   "hosts", "products", "bindings", "cmake", "tests", "examples", "scripts", "packaging", "metadata", "benchmarks"):
        files.extend(p for p in (ROOT / folder).rglob("*") if p.is_file()
            and not set(p.relative_to(ROOT).parts) & {"bin", "obj", "target", "build", "__pycache__", "node_modules", "wwwroot"}
            and p.suffix in {".c", ".h", ".cpp", ".cs", ".java", ".py", ".ps1", ".cmake", ".json",
                             ".xml", ".nuspec", ".csproj", ".vcxproj", ".filters", ".txt", ".props", ".in", ".tsx", ".ts", ".css", ".html", ".js"}
            and p != ROOT / "metadata/product.json")
    digest = hashlib.sha256()
    # Candidate/gate evidence cannot hash itself. Contract product fields are
    # part of the source; execution results and merge receipts are not.
    product = read_model()["product"]
    digest.update(json.dumps(product, sort_keys=True, separators=(",", ":")).encode())
    digest.update(b"\0")
    for path in sorted(set(files)):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()

def check():
    model = read_model()
    product = model["product"]
    version, abi = product["sdkVersion"], product["abiVersion"]
    fingerprint = source_hash()
    errors = ["Product projection differs: " + name for name in generate(True)]
    if model["candidate"]["sourceFingerprint"] != fingerprint:
        errors.append("Source changed after sealed evidence")
    for gate in model["gates"]:
        identifier = gate["id"]
        if gate["status"] == "Exempt":
            continue
        if gate["status"] != "Passed":
            errors.append(identifier + ": required core gate did not pass")
            continue
        if not gate.get("evidence"):
            errors.append(identifier + ": no actual evidence")
        for evidence in gate.get("evidence", []):
            path = (ROOT / evidence["path"]).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():
                errors.append(identifier + ": missing evidence")
                continue
            if hashlib.sha256(path.read_bytes()).hexdigest() != evidence["sha256"]:
                errors.append(identifier + ": evidence hash mismatch")
    delivery = model["artifacts"] or {}
    archive = (ROOT / delivery.get("archive", "")).resolve()
    manifest = (ROOT / delivery.get("manifest", "")).resolve()
    if not archive.is_relative_to(ROOT) or not archive.is_file():
        errors.append("Missing current distributable archive")
    elif hashlib.sha256(archive.read_bytes()).hexdigest() != delivery.get("sha256"):
        errors.append("Distributable archive hash mismatch")
    if not manifest.is_relative_to(ROOT) or not manifest.is_file():
        errors.append("Missing current artifact manifest")
    else:
        bundle = json.loads(manifest.read_text(encoding="utf-8"))
        if (bundle.get("sourceFingerprint") != fingerprint or bundle.get("abiVersion") != abi
                or bundle.get("sdkVersion") != version or bundle.get("configuration") != "Release"
                or bundle.get("runtimeIdentifier") != delivery.get("runtimeIdentifier")):
            errors.append("Artifact manifest differs from current source/ABI")
        expected = {"sdk/bin/ud_data.dll", "sdk/bin/ud_java.dll", "sdk/bin/universaldata/cli/ud_cli.exe",
                    "sdk/bin/universaldata/worker/ud_worker.exe", "sdk/bin/universaldata/management/UniversalData.Management.dll",
                    "sdk/bin/universaldata/management/wwwroot/index.html", f"packages/UniversalData.{version}.nupkg",
                    f"packages/universal-data-{version}.jar", "sdk/include/ud/version.h",
                    "sdk/include/ud/package.h", "sdk/lib/ud_data.lib"}
        if delivery.get("runtimeIdentifier")=="linux-x64":
            expected={"sdk/lib/libud_data.so","sdk/lib/libud_java.so","sdk/bin/universaldata/cli/ud_cli","sdk/bin/universaldata/worker/ud_worker",
                      "sdk/bin/universaldata/management/UniversalData.Management.dll","sdk/bin/universaldata/management/wwwroot/index.html",
                      f"packages/UniversalData.{version}.nupkg",f"packages/universal-data-{version}.jar",
                      "sdk/include/ud/version.h","sdk/include/ud/package.h"}
        if not expected.issubset({entry["path"] for entry in bundle.get("files", [])}):
            errors.append("Incomplete artifact manifest")
        for entry in bundle.get("files", []):
            path = (manifest.parent / entry["path"]).resolve()
            if not path.is_relative_to(manifest.parent) or not path.is_file():
                errors.append("Missing delivered file: " + entry["path"])
                continue
            if path.stat().st_size != entry["bytes"] or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
                errors.append("Delivered file changed: " + entry["path"])
    return fingerprint, errors

def nuget_names(version):
    return [f"{identity}.{version}.nupkg" for identity in
            ("UniversalData", "UniversalData.Runtime.win-x64", "UniversalData.Runtime.linux-x64")]


def nuget_inventory(directory, version):
    files = {path.name: path for path in directory.glob("*.nupkg")}
    product = read_model()["product"]
    required = set(nuget_names(version)) | {
        f"{package.lower()}.{locked}.nupkg" for package, locked in product["dotnetPackages"].items()}
    if not required <= files.keys():
        raise ValueError("Incomplete NuGet feed: " + ", ".join(sorted(required - files.keys())))
    entries = []
    for name, path in sorted(files.items()):
        with path.open("rb") as archive:
            digest = hashlib.file_digest(archive, "sha256").hexdigest()
        entries.append({"name": name, "bytes": path.stat().st_size, "sha256": digest})
    return entries


def assemble_nuget(linux, windows, destination):
    version = read_model()["product"]["sdkVersion"]
    sdk, win, linux_runtime = nuget_names(version)
    destination.mkdir(parents=True, exist_ok=True)
    for folder, name in ((linux, sdk), (linux, linux_runtime), (windows, win)):
        paths = list(folder.rglob(name))
        if len(paths) != 1:
            raise ValueError(f"Expected one actual package for {name} in {folder}")
        shutil.copyfile(paths[0], destination / name)
    product_names = {name.lower() for name in (sdk, win, linux_runtime)}
    for folder in (linux, windows):
        for path in sorted(folder.rglob("*.nupkg")):
            if path.name.lower() in product_names:
                continue
            output = destination / path.name.lower()
            if not output.exists():
                shutil.copyfile(path, output)
            else:
                with path.open("rb") as source, output.open("rb") as copied:
                    if hashlib.file_digest(source, "sha256").digest() != hashlib.file_digest(copied, "sha256").digest():
                        raise ValueError(f"Dependency archive differs between inputs: {path.name}")
    nuget_inventory(destination, version)


def verify_nuget_installations(directory, commit, archives, version):
    drivers = set(RUNTIME_DRIVERS)
    for rid in ("linux-x64", "win-x64"):
        evidence = json.loads((directory / f"installed-{rid}.json").read_text())
        lock_hash = evidence.get("dependencyLockSha256", "")
        if (evidence["rid"] != rid or evidence["sourceCommit"] != commit or evidence["version"] != version
                or not evidence["framework"].startswith("8.") or evidence["http"] != 200
                or evidence["sqliteRows"] != 35 or set(evidence["drivers"]) != drivers
                or len(lock_hash) != 64 or any(character not in "0123456789abcdef" for character in lock_hash)
                or sorted(evidence["archives"], key=lambda item: item["name"]) != archives):
            raise ValueError(f"The {rid} installation evidence differs from the proposed release")


def seal_nuget(directory):
    version = read_model()["product"]["sdkVersion"]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    archives = nuget_inventory(directory, version)
    verify_nuget_installations(directory, commit, archives, version)
    approval = dict(version=version, sourceCommit=commit, sourceFingerprint=source_hash(), archives=archives)
    (directory / "approval.json").write_text(json.dumps(approval, indent=2) + "\n", encoding="utf-8")


def check_nuget(directory):
    product = read_model()["product"]
    expected = product["sdkVersion"]
    approved = json.loads((directory / "approval.json").read_text())
    if (os.environ.get("UD_APPROVED_VERSION") != expected or approved.get("version") != expected
            or approved.get("sourceCommit") != os.environ.get("UD_RELEASE_COMMIT")
            or approved.get("sourceFingerprint") != source_hash()):
        raise ValueError("NuGet approval differs from verified source or installation evidence")
    entries = nuget_inventory(directory, expected)
    if approved["archives"] != entries:
        raise ValueError("The approved NuGet archive hashes differ")
    verify_nuget_installations(directory, approved["sourceCommit"], entries, expected)
    product_names = set(nuget_names(expected))
    for entry in (entry for entry in entries if entry["name"] in product_names):
        path = directory / entry["name"]
        if entry["bytes"] > 250_000_000:
            raise ValueError(f"NuGet size limit exceeded by {path.name}")
        with zipfile.ZipFile(path) as bundle:
            files = set(bundle.namelist())
            if path.name.startswith("UniversalData.Runtime."):
                rid = path.name.removeprefix("UniversalData.Runtime.").split(".", 1)[0]
                extension = ".dll" if rid == "win-x64" else ".so"
                worker = "ud_worker.exe" if rid == "win-x64" else "ud_worker"
                required = {f"runtimes/{rid}/worker/{worker}"} | {
                    f"runtimes/{rid}/native/ud_connector_{driver}{extension}"
                    for driver in RUNTIME_DRIVERS}
                native_prefix = f"runtimes/{rid}/native/"
                worker_prefix = f"runtimes/{rid}/worker/"
                native_assets = {name[len(native_prefix):] for name in files
                                 if name.startswith(native_prefix) and name != native_prefix}
                mirrored_worker_assets = {name[len(worker_prefix):] for name in files
                                         if name.startswith(worker_prefix) and name != worker_prefix}
                licenses = {name for name in files if name.startswith("licenses/") and not name.endswith("/")}
                empty_plugins = [name for name in required if "/native/ud_connector_" in name
                                 and name in files and not bundle.getinfo(name).file_size]
                if (not required <= files or empty_plugins or not native_assets or not licenses
                        or not native_assets <= mirrored_worker_assets):
                    raise ValueError(f"Runtime plugin closure or license notices are incomplete in {path.name}")
            else:
                spec = ET.fromstring(bundle.read("UniversalData.nuspec"))
                dependencies = {element.get("id"): element.get("version") for element in
                                spec.findall(".//{*}dependencies/{*}group/{*}dependency")}
                expected_dependencies = {
                    f"UniversalData.Runtime.{rid}": f"[{expected}]" for rid in ("linux-x64", "win-x64")}
                expected_dependencies.update({package: f"[{locked}]"
                                              for package, locked in product["dotnetPackages"].items()})
                if "lib/net8.0/UniversalData.dll" not in files or dependencies != expected_dependencies:
                    raise ValueError("The SDK assembly or exact dependencies differ")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint", action="store_true")
    parser.add_argument("--nuget", type=pathlib.Path)
    parser.add_argument("--assemble-nuget", type=pathlib.Path, nargs=3, metavar=("LINUX", "WINDOWS", "OUTPUT"))
    parser.add_argument("--seal-nuget", type=pathlib.Path)
    arguments = parser.parse_args()
    if arguments.assemble_nuget:
        assemble_nuget(*arguments.assemble_nuget)
        sys.exit(0)
    if arguments.seal_nuget:
        seal_nuget(arguments.seal_nuget)
        sys.exit(0)
    if arguments.nuget:
        check_nuget(arguments.nuget.resolve())
        print("Approved NuGet packages match both installation records")
        sys.exit(0)
    if arguments.fingerprint:
        print(source_hash())
        sys.exit(0)
    fingerprint, errors = check()
    if errors:
        print("Core delivery evidence incomplete:\n" + "\n".join(errors))
        sys.exit(1)
    print("Core delivery evidence passed for source " + fingerprint)
