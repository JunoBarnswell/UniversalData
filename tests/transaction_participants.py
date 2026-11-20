"""Real prepared branches survive process death; never emulate a vendor resource."""
import os
import pathlib
import secrets
import subprocess
import sys

variable, executable, plugin = sys.argv[1:4]
assert len(sys.argv) in (4, 6, 8)
resources = []
for environment, driver in [(variable, plugin), *zip(sys.argv[4::2], sys.argv[5::2])]:
    configuration = os.environ.get(environment)
    if not configuration or not pathlib.Path(configuration).is_file():
        print(f"Blocked: real participant resource {environment} is not configured", flush=True)
        raise SystemExit(77)
    resources.append((driver, configuration))

cases = [(resources, (("active-rollback", 0),)),
         (resources, (("active-kill", 86), ("recover-abandoned", 0))),
         (resources, (("prepare-kill", 86), ("recover-rollback", 0))),
         (resources, (("prepare-kill", 86), ("recover-commit", 0)))]
# Every provider must be able to recover a prepared branch after a peer commits.
cases.extend((resources[first:] + resources[:first], (("partial-commit-kill", 86), ("recover-commit", 0)))
             for first in range(len(resources)))

for order, phases in cases:
    primary, configuration = order[0]
    remaining = [item for resource in order[1:] for item in resource]
    identity = secrets.token_hex(32)
    for phase, expected in (("setup", 0), *phases):
        result = subprocess.run([executable, primary, configuration, phase, identity, *remaining], timeout=120)
        if result.returncode == 77:
            raise SystemExit(77)
        if result.returncode != expected:
            raise SystemExit(f"{phase}: expected process status {expected}, got {result.returncode}")
print(f"Real {max(2, len(resources))}-connection rollback, abandoned branch recovery, prepare, "
      "commit, and partial-commit recovery passed")
