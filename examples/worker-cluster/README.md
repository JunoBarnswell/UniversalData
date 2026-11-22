# Real single-host Worker cluster demo

This directory contains a Linux/WSL2 demonstration of the product's real
remote Worker protocol:

```text
active Management  ─┐
                     ├─ synchronous PostgreSQL 17 Catalog + shared identity/key
standby Management ─┘
                     │
                     ├─ mTLS worker-1 (one process slot)
                     ├─ mTLS worker-2 (one process slot)
                     └─ mTLS worker-3 (one process slot)
```

The demo starts the product Management and Worker binaries with the normal placement and
recovery protocol. A private loopback HTTP service is an external destination. It fsyncs each unique key
before replying and provides an HTTP readback endpoint, so duplicate effects
and missing results are visible.

## Prerequisites

- Linux or WSL2 with Python 3 and `openssl`.
- Product binaries built with HTTP and PostgreSQL support: `dotnet`,
  `UniversalData.Management.dll`, `ud_worker.dll`, and the matching CLI.
- A connection file for a new, dedicated PostgreSQL 17 Catalog database with a synchronous streaming standby. Use the fenced writer endpoint. Set `hostaddr` to its numeric address and use `host` for TLS verification.
- The PostgreSQL role must be able to create/read the Catalog objects used by
  the normal `catalog migrate import` command and read `pg_stat_replication`.

The state directory is private (`0700`), contains generated certificates,
the shared Catalog key, identity password, PID/start-time ownership records,
logs and durable effect files. Keep it private while the demo is running.

## Commands

Run `start` once, then use the same state directory for the other actions:

```bash
STATE=/tmp/ud-worker-cluster-demo
PG=/private/path/to/a/dedicated-postgresql-17-connection-file
DOTNET=dotnet
MANAGEMENT=/absolute/path/to/UniversalData.Management.dll
WORKER=/absolute/path/to/ud_worker.dll

python3 examples/worker-cluster/cluster_demo.py start \
  --state-dir "$STATE" --postgres "$PG" --dotnet "$DOTNET" \
  --management "$MANAGEMENT" --worker "$WORKER" --openssl openssl
python3 examples/worker-cluster/cluster_demo.py verify --state-dir "$STATE"
python3 examples/worker-cluster/cluster_demo.py failover --state-dir "$STATE"
python3 examples/worker-cluster/cluster_demo.py stop --state-dir "$STATE"
```

`verify` publishes three independent HTTP jobs concurrently. The three effect calls
must overlap, and the result must identify distinct `worker-1`, `worker-2` and
`worker-3` nodes. `failover` submits another three jobs, records each run's
Native execution ID, Worker PID and node, then kills only the owned active
Management PID. The standby must become ready, all three agents must register
again, and each run must retain its ID/PID/node with exactly one durable
effect. If the protocol reports `RecoveryRequired`, the harness calls
`POST /api/executions/{original-run}/resume` and checks the same Native execution ID
and exactly one durable effect per key.

On success each action prints `PASS` and JSON evidence. On failure it prints
`FAIL` and tails the private Management/Agent logs. `stop` validates the
recorded Linux `/proc` start time, command identity and private instance token
before signaling a process, so a reused PID cannot cause an unrelated process
to be killed.
The state directory holds the evidence and logs after `stop`.

## Sentinel acceptance

`tests/management_remote_nodes.py --sentinel` imports the same
`cluster_harness.py` implementation. CTest already registers it as
`management_sentinel_ha` on Linux builds with HTTP and PostgreSQL. The test reads
`UD_TEST_SENTINEL_POSTGRESQL_CONNECTION_FILE`, returns CTest skip code 77 when
the dedicated synchronous PostgreSQL 17 fixture is absent, and uses a temporary private
artifact directory.
