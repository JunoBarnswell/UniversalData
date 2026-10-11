"""Explicit optional dependency preparation; pinned official archives only."""
import argparse
import hashlib
import json
import pathlib
import urllib.request
import zipfile

p = argparse.ArgumentParser()
p.add_argument("--platform", choices=["windows", "linux"], required=True)
p.add_argument("--output", type=pathlib.Path, required=True)
a = p.parse_args()
root = pathlib.Path(__file__).resolve().parents[1]
lock = json.loads((root / "packaging/dependencies.lock.json").read_text())
archives = {
    "sqlite": lock["sqlite"]["source"],
    "duckdb": f'https://github.com/duckdb/duckdb/releases/download/v{lock["duckdb"]["version"]}/libduckdb-{a.platform}-amd64.zip',
}
a.output.mkdir(parents=True, exist_ok=True)
for name, url in archives.items():
    archive = a.output / f"{name}-{a.platform}.zip"
    if not archive.exists():
        request = urllib.request.Request(url, headers={"User-Agent": "UniversalData"})
        with urllib.request.urlopen(request, timeout=60) as response, archive.open("wb") as file:
            while chunk := response.read(1024*1024): file.write(chunk)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    expected = lock[name].get("sha256", {}).get(a.platform)
    if expected and digest != expected: raise RuntimeError(f"{name}: SHA-256 mismatch")
    destination = (a.output / name).resolve()
    with zipfile.ZipFile(archive) as source:
        for entry in source.infolist():
            target = (destination / entry.filename).resolve()
            if not target.is_relative_to(destination): raise RuntimeError("Unsafe archive path")
        source.extractall(destination)
    print(f"{name}: sha256={digest}")
