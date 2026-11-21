"""Measure a RabbitMQ burst on three actual SDK Workers. Requires Docker and pika."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import secrets
import sqlite3
import subprocess
import tempfile
import time
from contextlib import closing

import pika


def broker_connection(port, user, password):
    parameters = pika.ConnectionParameters("127.0.0.1", port,
        credentials=pika.PlainCredentials(user, password), blocked_connection_timeout=60)
    deadline = time.monotonic() + 60
    while True:
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


def verify_files(directory, messages, workers, concurrency):
    seen = bytearray((messages + 7) // 8)
    verified = 0
    for index, worker in enumerate(workers):
        path = directory / f"sink-{index}.db"
        with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as database:
            count = 0
            for (sequence,) in database.execute("SELECT Id FROM mq_completed"):
                assert 0 <= sequence < messages, sequence
                offset, bit = divmod(sequence, 8)
                assert not seen[offset] & (1 << bit), f"Repeated message {sequence}"
                seen[offset] |= 1 << bit
                count += 1
        assert count == worker["CommittedRows"] == worker["AcknowledgedRows"], worker
        with (directory / f"sink-{index}.timing.jsonl").open() as timing:
            delayed = peak = 0
            shortest = float("inf")
            for line in timing:
                batch = json.loads(line)
                delayed += batch["Rows"]
                peak = max(peak, batch["PeakInFlight"])
                shortest = min(shortest, batch["MinimumDelayMs"])
        assert delayed == count and 1 < peak <= concurrency and shortest >= 5000, (delayed, count, peak, shortest)
        worker.update(PeakInFlight=peak, MinimumDelayMs=shortest, IndependentlyVerified=count)
        verified += count
    assert verified == messages, (verified, messages)


def measure(args, container, directory, user, password):
    port = int(subprocess.check_output(["docker", "port", container, "5672/tcp"], text=True).strip().split(":")[-1])
    queue = "ud-mq-burst"
    with broker_connection(port, user, password) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True, arguments={"x-queue-type": "classic"})
        properties = pika.BasicProperties(content_type="application/json", delivery_mode=2)
        submitted = time.monotonic()
        for sequence in range(args.messages):
            channel.basic_publish("", queue, b'{"Id":%d}' % sequence, properties=properties)
        ready = channel.queue_declare(queue=queue, passive=True).method.message_count
        pending_deadline = time.monotonic() + 10
        while ready < args.messages and time.monotonic() < pending_deadline:
            time.sleep(0.05)
            ready = channel.queue_declare(queue=queue, passive=True).method.message_count
        enqueue_seconds = time.monotonic() - submitted
        assert ready == args.messages, (ready, args.messages)
        print(json.dumps({"Stage": "enqueued", "Messages": ready, "ElapsedSeconds": enqueue_seconds}), flush=True)

    files = directory / "files"
    files.mkdir()
    for index in range(3):
        with closing(sqlite3.connect(files / f"sink-{index}.db")) as database:
            database.execute("CREATE TABLE mq_completed (Id INTEGER NOT NULL PRIMARY KEY)")
    program = Path(args.program).resolve(strict=True)
    plugin_roots = list(dict.fromkeys([str(program.parent), str(Path(args.plugins).resolve(strict=True))]))
    policy = directory / "policy.json"
    policy.write_text(json.dumps(dict(dataDirectory=str(directory / "state"), fileRoots=[str(files)],
        pluginRoots=plugin_roots, networkOrigins=[], credentialFiles={}, maxConcurrency=3,
        memoryLimit=1073741824, maxConnectionsPerSource=3,
        trustedAssemblyHashes=[hashlib.sha256(program.read_bytes()).hexdigest()], resources={
            "broker": ["Open", "Read", "Network"], f"broker/queue/{queue}": ["Read", "Write"],
            f"amqp://127.0.0.1:{port}": ["Network"]
        })))
    credential = directory / "rabbitmq.json"
    credential.write_text(json.dumps(dict(host="127.0.0.1", port=port, virtualHost="/", tls=False,
        username=user, password=password)))
    credential.chmod(0o600)

    command = [args.dotnet, str(program), "--mq-burst", str(policy), str(credential), queue, str(files),
        str(args.messages), str(args.in_flight), str(args.timeout)]
    complete = None
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True,
            env={**os.environ, "UD_WORKER_PATH": str(Path(args.worker).resolve(strict=True))}) as demo:
        for line in demo.stdout:
            print(line, end="", flush=True)
            record = json.loads(line)
            if record["Stage"] == "complete":
                complete = record
        if demo.wait() or complete is None:
            raise RuntimeError("The three Worker runs did not complete")
    finished = time.monotonic()
    verify_files(files, args.messages, complete["Workers"], args.in_flight)
    with broker_connection(port, user, password) as connection:
        stats = connection.channel().queue_declare(queue=queue, passive=True).method
        assert stats.message_count == stats.consumer_count == 0, stats

    return dict(messages=args.messages, workerCount=3, businessDelayMs=5000,
        maxInFlightPerWorker=args.in_flight, enqueueSeconds=enqueue_seconds,
        drainSeconds=complete["ElapsedSeconds"], endToEndSeconds=finished - submitted,
        drainMessagesPerSecond=args.messages / complete["ElapsedSeconds"], queueDepthAfter=stats.message_count,
        workers=complete["Workers"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dotnet")
    parser.add_argument("program")
    parser.add_argument("worker")
    parser.add_argument("plugins")
    parser.add_argument("--messages", type=int, default=900000)
    parser.add_argument("--in-flight", type=int, default=16384)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.messages < 3 * args.in_flight or not 2 <= args.in_flight <= 65535 or not 1 <= args.timeout <= 3600:
        parser.error("Require enough messages for one wave on all three Workers, 2–65,535 in-flight per Worker and a timeout up to 3,600 seconds")
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    container = "ud-mq-burst-" + secrets.token_hex(6)
    user, password = "ud-burst", secrets.token_hex(24)
    with tempfile.TemporaryDirectory(prefix="ud-mq-burst-") as scratch:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", container, "--memory=1g", "--shm-size=64m",
            "-p", "127.0.0.1::5672", "--tmpfs", "/var/lib/rabbitmq:rw,size=1g,mode=1777",
            "-e", "RABBITMQ_DEFAULT_USER", "-e", "RABBITMQ_DEFAULT_PASS", "-e",
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2:2", "rabbitmq:4.1-management-alpine"], check=True,
            stdout=subprocess.DEVNULL, env={**os.environ, "RABBITMQ_DEFAULT_USER": user,
                "RABBITMQ_DEFAULT_PASS": password})
        try:
            result = measure(args, container, Path(scratch), user, password)
        finally:
            subprocess.run(["docker", "stop", container], stdout=subprocess.DEVNULL, check=True, timeout=30)
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
