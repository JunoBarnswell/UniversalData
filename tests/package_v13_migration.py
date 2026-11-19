"""The ABI 14 migration preserves business values and refuses to replace a destination."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile


def invoke(source, target):
    tool = Path(__file__).resolve().parents[1] / "scripts/migrate_package_v13_to_v14.py"
    return subprocess.run([sys.executable, str(tool), str(source), str(target)],
                          capture_output=True, text=True, check=False)


with tempfile.TemporaryDirectory(prefix="ud-v13-migration-") as scratch:
    root = Path(scratch)
    source, target = root / "source.json", root / "target.json"
    field = {"name": "version", "type": "int64"}
    original = {
        "version": 13,
        "id": "migration",
        "controlFlow": {"tasks": []},
        "dataFlows": [{"components": [{"config": {
            "schema": {"version": 13, "fields": [field]},
            "rows": [{"version": 13, "schema": {"version": 13, "fields": []}}],
        }}]}],
    }
    source.write_text(json.dumps(original), encoding="utf-8")
    result = invoke(source, target)
    assert result.returncode == 0, result.stderr
    migrated = json.loads(target.read_text(encoding="utf-8"))
    config = migrated["dataFlows"][0]["components"][0]["config"]
    assert migrated["version"] == 14 and config["schema"] == {"version": 14, "fields": [field]}
    assert config["rows"] == original["dataFlows"][0]["components"][0]["config"]["rows"]
    assert json.loads(source.read_text(encoding="utf-8")) == original
    target_content = target.read_bytes()
    assert invoke(source, target).returncode == 1 and target.read_bytes() == target_content
    assert invoke(target, root / "second-copy.json").returncode == 1

    template = dict(version=13, id="template", controlFlow={"tasks": []},
                    templates=[], instances=[{"parameters": {
                        "schema": {"version": 13, "fields": [field]},
                    }}])
    source.write_text(json.dumps(template), encoding="utf-8")
    template_copy = root / "template-copy.json"
    result = invoke(source, template_copy)
    assert result.returncode == 0, result.stderr
    migrated = json.loads(template_copy.read_text(encoding="utf-8"))
    assert migrated["instances"][0]["parameters"]["schema"]["version"] == 14

print("Package migration upgraded ABI envelopes, preserved row values and refused replacement.")
