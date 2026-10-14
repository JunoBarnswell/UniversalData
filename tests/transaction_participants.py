"""Real prepared branches survive process death; never emulate a vendor resource."""
import os
import pathlib
import secrets
import subprocess
import sys

variable, executable, plugin = sys.argv[1:4]
extra = []
if len(sys.argv) > 4:
    other = os.environ.get(sys.argv[4])
    if not other or not pathlib.Path(other).is_file():
        print("Blocked: second actual participant resource is not configured", flush=True)
        raise SystemExit(77)
    extra = [sys.argv[5], other]
configuration = os.environ.get(variable)
if not configuration or not pathlib.Path(configuration).is_file():
    print(f"Blocked: real participant resource {variable} is not configured", flush=True)
    raise SystemExit(77)

for boundary, decision in (("prepare-kill", "recover-rollback"),
                           ("prepare-kill", "recover-commit"),
                           ("partial-commit-kill", "recover-commit")):
    identity = secrets.token_hex(32)
    for phase, expected in (("setup", 0), (boundary, 86), (decision, 0)):
        result = subprocess.run([executable, plugin, configuration, phase, identity] + extra, timeout=120)
        if result.returncode == 77:
            raise SystemExit(77)
        if result.returncode != expected:
            raise SystemExit(f"{phase}: expected process status {expected}, got {result.returncode}")
print("Real two-connection prepare/rollback, commit, partial-commit recovery passed")
