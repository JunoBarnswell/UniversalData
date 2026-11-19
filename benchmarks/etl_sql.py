"""Reproducible Native SQL benchmarks on isolated, explicitly configured databases."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from itertools import product
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile

DRIVERS = {
    "sqlite": (1, None),
    "postgresql": (2, "UD_TEST_POSTGRESQL_CONFIG"),
    "mysql": (3, "UD_TEST_MYSQL_CONFIG"),
    "mariadb": (4, "UD_TEST_MARIADB_CONFIG"),
    "odbc": (5, "UD_TEST_ODBC_CONFIG"),
    "oracle_oci": (6, "UD_TEST_ORACLE_OCI_CONFIG"),
}


def run_sample(args, driver, plugin, variable, rows, width, batch, delay, directory):
    dialect = DRIVERS[driver][0]
    env = os.environ.copy()
    fixture = None
    config_path = None
    if variable:
        env["UD_SQL_BENCHMARK_CONFIG"] = env[variable]
    else:
        with tempfile.NamedTemporaryFile(dir=directory, suffix=".config", delete=False) as config:
            config_path = Path(config.name)
            fixture = config_path.with_suffix(".db")
            config.write(str(fixture).encode("utf-8"))
            env["UD_SQL_BENCHMARK_CONFIG"] = config.name
    command = [str(args.native), str(dialect), str(plugin), str(rows), str(batch), str(width),
               str(args.read_batch), str(delay)]
    try:
        result = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=args.timeout, check=False)
    except subprocess.TimeoutExpired as timeout:
        raise RuntimeError(f"{driver} sample with {batch} rows per write timed out after {args.timeout}s") from timeout
    finally:
        if config_path:
            config_path.unlink(missing_ok=True)
        if fixture:
            fixture.unlink(missing_ok=True)
            for suffix in ("-wal", "-shm", "-journal"):
                Path(str(fixture) + suffix).unlink(missing_ok=True)
    if result.returncode:
        raise RuntimeError(f"{driver} measurement returned {result.returncode}: {result.stderr.strip()}")
    record = json.loads(result.stdout)
    record["insertRowsPerSecond"] = rows * 1e9 / record["insertNs"]
    record["updateRowsPerSecond"] = rows * 1e9 / record["updateNs"]
    record["deleteRowsPerSecond"] = rows * 1e9 / record["deleteNs"]
    record["readRowsPerSecond"] = rows * 1e9 / record["readNs"]
    return record


def measure_case(args, driver, plugin, variable, rows, width, concurrency, delay, cache):
    samples = {mode: [] for mode in (1, args.batch)}
    case = dict(rows=rows, width=width, concurrency=concurrency, consumerDelayMs=delay,
                single=samples[1], batch=samples[args.batch])
    try:
        for trial in range(args.trials):
            batches = (1, args.batch) if trial % 2 == 0 else (args.batch, 1)
            for batch in batches:
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    work = [pool.submit(run_sample, args, driver, plugin, variable,
                                        rows, width, batch, delay, cache) for _ in range(concurrency)]
                    for future in work:
                        samples[batch].append(future.result())
        signatures = {run["readChecksum"] for runs in samples.values() for run in runs}
        if len(signatures) != 1:
            raise RuntimeError("Batch and single-row sample signatures differ")
        for chunk_rows, runs in samples.items():
            calls = (rows + chunk_rows - 1) // chunk_rows
            if any(run[field] != calls for run in runs for field in ("insertCalls", "updateCalls", "deleteCalls")):
                raise RuntimeError("Application write calls do not match the supplied chunks")
        ratios = {}
        for name in ("insertNs", "updateNs", "deleteNs"):
            single = statistics.median(sample[name] for sample in samples[1])
            multiple = statistics.median(sample[name] for sample in samples[args.batch])
            ratios[name] = single / multiple
        case.update(batchSpeedup=ratios, status="Pass" if min(ratios.values()) > 1 else "Fail")
    except (RuntimeError, ValueError) as error:
        case.update(status="Fail", reason=str(error))
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", required=True, type=lambda p: Path(p).resolve())
    parser.add_argument("--plugins", required=True, type=lambda p: Path(p).resolve())
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--drivers", nargs="+", choices=DRIVERS, default=list(DRIVERS))
    parser.add_argument("--rows", nargs="+", type=int, default=[100_000, 1_000_000])
    parser.add_argument("--width", nargs="+", type=int, default=[32, 1024])
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 4, 8])
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--batch", type=int, default=512)
    parser.add_argument("--read-batch", type=int, default=512)
    parser.add_argument("--slow-ms", nargs="+", type=int, default=[0, 2])
    args = parser.parse_args()
    if (not args.native.is_file() or not 2 <= args.batch <= 512 or args.trials < 1
            or not 1 <= args.read_batch <= 65536 or min(args.rows) < 1 or min(args.concurrency) < 1
            or min(args.width) < 1 or max(args.width) > 2000
            or min(args.slow_ms) < 0 or max(args.slow_ms) > 1000):
        parser.error("Use a Release benchmark binary and valid row, batch, width and delay bounds")
    revision = subprocess.run(["git", "rev-parse", "HEAD"],
                              cwd=Path(__file__).resolve().parents[1], text=True,
                              capture_output=True, check=False)
    report = dict(platform=platform.platform(), processor=platform.processor(),
                  logicalCpus=os.cpu_count(), sourceBaseline=revision.stdout.strip(),
                  drivers={})
    suffix = ".dll" if os.name == "nt" else ".so"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for driver in args.drivers:
        variable = DRIVERS[driver][1]
        plugin = args.plugins / f"ud_connector_{driver}{suffix}"
        if not plugin.is_file() or (variable and not Path(os.environ.get(variable, "")).is_file()):
            report["drivers"][driver] = dict(status="Blocked",
                                             reason="Native connector or isolated configuration unavailable")
            continue
        result = dict(status="Running", cases=[])
        report["drivers"][driver] = result
        with tempfile.TemporaryDirectory(prefix="ud-sql-perf-") as cache:
            cases = product(args.rows, args.width, args.concurrency, args.slow_ms)
            for rows, width, concurrency, delay in cases:
                result["cases"].append(measure_case(args, driver, plugin, variable, rows, width,
                                                    concurrency, delay, cache))
                args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        result["status"] = "Pass" if all(case["status"] == "Pass" for case in result["cases"]) else "Fail"
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    states = {result["status"] for result in report["drivers"].values()}
    return 1 if "Fail" in states else 77 if "Blocked" in states else 0


if __name__ == "__main__":
    raise SystemExit(main())
