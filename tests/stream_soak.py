"""Wall-clock Native event-window soak with an independent output oracle."""
import argparse
import hashlib
import json
import pathlib
import subprocess
import tempfile
import time


class NativeWindow:
    def __init__(self, runner, snapshot, restore=False):
        self.process = subprocess.Popen(
            [runner, str(snapshot), "restore" if restore else "start"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="ascii",
        )
        if self.read() != f"ready {int(restore)}":
            raise RuntimeError("Native window failed to initialize")

    def read(self):
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"Native window closed output (status {self.process.poll()})")
        return line.rstrip("\r\n")

    def send(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def collect(self, sink_delay):
        rows = []
        while True:
            line = self.read()
            if not line.startswith("window "):
                return rows, line
            rows.append(tuple(map(int, line.split()[1:])))
            if sink_delay:
                time.sleep(sink_delay)
            self.send("a")

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        status = self.process.wait(timeout=10)
        self.process.stdin.close()
        self.process.stdout.close()
        return status


def update_model(active, data, prior_mark, mark):
    late = 0
    for stamp, key, value in data:
        start = (stamp // 64) * 64
        end = start + 64
        if prior_mark is not None and end + 4 <= prior_mark:
            late += 1
            continue
        window = (start, end, key)
        active[window] = active.get(window, 0) + value
    closed = sorted(window for window in active if mark is None or window[1] + 4 <= mark)
    return [(*window, active.pop(window)) for window in closed], late


def update_digest(digest, rows):
    digest.update("".join(",".join(map(str, row)) + "\n" for row in rows).encode("ascii"))


def run(args):
    actual_digest, expected_digest = hashlib.sha256(), hashlib.sha256()
    state = {}
    prior_mark = None
    batches, inputs, outputs, recovered, blocked, delayed, late, high_water = (0,) * 8
    last_restart = 0
    with tempfile.TemporaryDirectory(prefix="ud-window-soak-") as directory:
        snapshot = pathlib.Path(directory) / "window.udwire"
        native = NativeWindow(args.runner, snapshot)
        started = time.monotonic()
        try:
            while time.monotonic() - started < args.seconds or recovered < 1:
                base = 32 * batches
                # A hot key dominates; the remaining keys exercise overlapping active groups.
                events = [
                    (base + i - (80 if i % 23 == 0 else 11 if i % 7 == 0 else 0),
                     0 if i % 5 < 3 else (base + i) % 997, (base + i) % 101 - 50)
                    for i in range(args.rows)
                ]
                mark = base + 17
                had_active_checkpoint = bool(state)
                expected, rejected = update_model(state, events, prior_mark, mark)
                text = "\n".join(" ".join(map(str, event)) for event in events)
                command = f"b {len(events)} {mark}\n{text}"
                native.send(command)
                if expected and had_active_checkpoint and (not recovered or batches - last_restart >= 256):
                    # Withhold the first acknowledgment so the worker cannot replace the
                    # committed Native snapshot. The partial result belongs to the lost attempt.
                    uncommitted = native.read().split()
                    assert uncommitted[0] == "window"
                    assert tuple(map(int, uncommitted[1:])) == expected[0]
                    assert snapshot.stat().st_size > 0
                    assert native.stop() != 0, "worker exited before interruption"
                    native = NativeWindow(args.runner, snapshot, restore=True)
                    native.send(command)
                    recovered += 1
                    last_restart = batches
                delay = args.sink_delay if batches % 16 == 0 else 0
                actual, message = native.collect(delay)
                prefix, count, checks, metrics = message.split(" ", 3)
                assert prefix == "checkpoint", message
                diagnostics = json.loads(metrics)
                late += rejected
                assert actual == expected, (batches, actual, expected)
                assert int(count) == len(state) == diagnostics["activeWindows"], diagnostics
                assert diagnostics["watermark"] == mark, diagnostics
                assert diagnostics["lateRows"] == late, diagnostics
                assert diagnostics["stateHighWater"] <= 8388608, diagnostics
                assert int(checks) >= len(actual), (checks, actual)
                high_water = max(high_water, diagnostics["stateHighWater"])
                update_digest(actual_digest, actual)
                update_digest(expected_digest, expected)
                prior_mark = diagnostics["watermark"]
                inputs += len(events)
                outputs += len(actual)
                blocked += int(checks)
                delayed += len(actual) if delay else 0
                batches += 1
            native.send("q")
            actual, message = native.collect(0)
            expected, _ = update_model(state, [], prior_mark, None)
            assert message == "done" and actual == expected, (message, actual, expected)
            assert native.process.wait(timeout=10) == 0
            outputs += len(actual)
            update_digest(actual_digest, actual)
            update_digest(expected_digest, expected)
        finally:
            native.stop()
        assert outputs and recovered and blocked and late
        assert not args.sink_delay or delayed
        assert actual_digest.digest() == expected_digest.digest()
        print(json.dumps({
            "elapsedSeconds": round(time.monotonic() - started, 3),
            "batches": batches, "inputRows": inputs, "outputWindows": outputs,
            "lastNativeWatermarkMicros": prior_mark, "lateRows": late,
            "stateHighWaterBytes": high_water, "noDemandChecks": blocked,
            "sinkDelayedWindows": delayed, "killRestores": recovered,
            "discardedUncommittedWindows": recovered,
            "committedSummary": actual_digest.hexdigest(),
            "independentSummary": expected_digest.hexdigest(),
        }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runner", help="path to the Native test_stream_soak executable")
    parser.add_argument("--seconds", type=float, default=3600)
    parser.add_argument("--rows", type=int, choices=range(8, 65), default=64,
                        metavar="8..64", help="rows in each source batch")
    parser.add_argument("--sink-delay", type=float, default=0.001)
    args = parser.parse_args()
    if args.seconds <= 0 or args.sink_delay < 0:
        parser.error("--seconds must be positive and --sink-delay must be nonnegative")
    run(args)


if __name__ == "__main__":
    main()
