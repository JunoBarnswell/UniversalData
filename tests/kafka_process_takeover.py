"""Crash a real Native Kafka execution after its output transaction commits."""
import os
import pathlib
import queue
import subprocess
import sys
import threading
import time

runner, module = sys.argv[1:]
configuration = os.environ.get("UD_TEST_KAFKA_TAKEOVER_CONFIG")
if not configuration or not pathlib.Path(configuration).is_file():
    print("Blocked: real integration resource UD_TEST_KAFKA_TAKEOVER_CONFIG is not configured")
    raise SystemExit(77)

command = [runner, module, configuration]
with subprocess.Popen([*command, "--crash-owner"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                      text=True, encoding="utf-8", errors="replace") as owner:
    events = queue.Queue()

    def read_events():
        for line in owner.stdout:
            events.put(line.rstrip())
        events.put(None)

    reader = threading.Thread(target=read_events, daemon=True)
    reader.start()
    deadline = time.monotonic() + 120
    try:
        while True:
            line = events.get(timeout=max(0, deadline - time.monotonic()))
            if line is None:
                raise AssertionError(f"Owner exited before the committed transaction: {owner.wait()}")
            print(line, flush=True)
            if line == "KAFKA_OWNER_COMMITTED":
                assert owner.poll() is None, "Owner already exited before the fault"
                break
    finally:
        if owner.poll() is None:
            owner.kill()
        owner.wait(timeout=15)
        reader.join(timeout=5)

assert owner.returncode != 0, "Owner must exit because of the process fault"
subprocess.run([*command, "--takeover"], check=True, timeout=90)
print("PASS: process killed after Kafka commit; fresh Journal restored one window result and exact offsets")
