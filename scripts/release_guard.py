"""Verify sealed core evidence from the authoritative product model."""
import argparse
import hashlib
import json
import pathlib
import sys
from product_model import generate, read_model

ROOT = pathlib.Path(__file__).resolve().parents[1]

def source_hash():
    files = [ROOT / name for name in (
        "CMakeLists.txt", "CMakePresets.json", "VERSION", ".github/workflows/ci.yml",
        "Directory.Build.props", "Directory.Build.targets", "UniversalData.sln")]
    for folder in ("include", "components", "engines", "connectors", "catalog", "plugins",
                   "hosts", "bindings", "cmake", "tests", "examples", "scripts", "packaging", "metadata"):
        files.extend(p for p in (ROOT / folder).rglob("*") if p.is_file()
            and not set(p.relative_to(ROOT).parts) & {"bin", "obj", "target", "build", "__pycache__"}
            and p.suffix in {".c", ".h", ".cs", ".java", ".py", ".ps1", ".cmake", ".json",
                             ".xml", ".csproj", ".vcxproj", ".filters", ".txt", ".props", ".in"}
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
        expected = {"sdk/bin/ud_data.dll", "sdk/bin/ud_java.dll", "sdk/bin/ud_cli.exe",
                    "sdk/bin/ud_worker.exe", f"packages/UniversalData.{version}.nupkg",
                    f"packages/universal-data-{version}.jar", "sdk/include/ud/version.h",
                    "sdk/include/ud/package.h", "sdk/lib/ud_data.lib"}
        if delivery.get("runtimeIdentifier")=="linux-x64":
            expected={"sdk/lib/libud_data.so","sdk/lib/libud_java.so","sdk/bin/ud_cli","sdk/bin/ud_worker",
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint", action="store_true")
    if parser.parse_args().fingerprint:
        print(source_hash())
        sys.exit(0)
    fingerprint, errors = check()
    if errors:
        print("Core delivery evidence incomplete:\n" + "\n".join(errors))
        sys.exit(1)
    print("Core delivery evidence passed for source " + fingerprint)
