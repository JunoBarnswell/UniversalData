"""Run a registered real driver test; absent resources remain explicit Blocked."""
import os
import pathlib
import subprocess
import sys

variable, executable, plugin, *arguments = sys.argv[1:]
configuration = os.environ.get(variable)
if not configuration or not pathlib.Path(configuration).is_file():
    print(f"Blocked: real integration resource {variable} is not configured", flush=True)
    raise SystemExit(77)
raise SystemExit(subprocess.run([executable, plugin, configuration, *arguments]).returncode)
