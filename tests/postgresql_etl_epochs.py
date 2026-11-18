"""Real PostgreSQL CDC epoch recovery and keyed SQL scan proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import struct
import subprocess
import tempfile
import time
import uuid


FIXTURE_ID = uuid.uuid4().hex[:12]
SOURCE = f"ud_etl_cdc_source_{FIXTURE_ID}"
TARGET = f"ud_etl_cdc_target_{FIXTURE_ID}"
AUDIT = f"ud_etl_cdc_target_audit_{FIXTURE_ID}"
AUDIT_FUNCTION = f"ud_etl_cdc_audit_{FIXTURE_ID}"
SLOT = f"ud_etl_cdc_epoch_slot_{FIXTURE_ID}"
PUBLICATION = f"ud_etl_cdc_epoch_publication_{FIXTURE_ID}"
SCAN = f"ud_etl_cdc_scan_source_{FIXTURE_ID}"
CDC_TASK = f"pg-cdc-epochs-{FIXTURE_ID}"
JOB = f"next-phase//{len(CDC_TASK)}:{CDC_TASK};/destination"
PG_EPOCH_US = 946684800000000


def postgres_environment(config):
    settings = dict(item.split("=", 1) for item in shlex.split(config.read_text()))
    env = dict(os.environ)
    names = {
        "host": "PGHOST",
        "hostaddr": "PGHOSTADDR",
        "port": "PGPORT",
        "user": "PGUSER",
        "dbname": "PGDATABASE",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
        "sslrootcert": "PGSSLROOTCERT",
        "connect_timeout": "PGCONNECT_TIMEOUT",
    }
    for key, variable in names.items():
        if key in settings:
            env[variable] = settings[key]
    env["PGCLIENTENCODING"] = "UTF8"
    return env


def query(args, sql, timeout=30):
    result = subprocess.run(
        [str(args.psql), "-X", "-w", "-qAt", "-F", "\t", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True, encoding="utf-8", env=args.pg_env,
        timeout=timeout,
    )
    if result.returncode:
        raise AssertionError(f"PostgreSQL fixture query failed ({result.returncode}): {result.stderr}")
    return result.stdout.strip()


def native(args, document, state, resume=False, crash=False):
    result = subprocess.run(
        [str(args.runner), str(document), str(state), str(int(resume)), "2",
         "4" if crash else "0", "1", "destination"],
        capture_output=True, text=True, encoding="utf-8", timeout=90,
    )
    if crash:
        if result.returncode == 91:
            return None
        facts = json.loads(result.stdout) if result.stdout else {}
        sources = [(node["id"], node["result"], node["steps"])
                   for flow in facts.get("diagnostics", {}).get("pipelines", [])
                   for tree in flow["trees"] for node in tree["nodes"] if node["result"]]
        raise AssertionError(
            f"Expected crash code 91, got {result.returncode}; "
            f"native result={facts.get('result')}, failed components={sources}\n{result.stderr}"
        )
    assert result.returncode == 0, (result.returncode, result.stderr)
    facts = json.loads(result.stdout)
    assert facts["result"] == 0, (facts, result.stderr)
    return facts


def pipeline(args, root, task, components, paths):
    state, spill = root / "state", root / "spill"
    state.mkdir()
    spill.mkdir()
    postgres = dict(driver=str(args.plugin), resource="application_name=ud-etl-validation",
                    credentialReference="file:" + str(args.config))
    connections = [dict(id="source", **postgres), dict(id="target", **postgres),
                   dict(id="spill", driver="file", resource=str(spill)),
                   dict(id="output", driver="file", resource=str(root / "output.ndjson"))]
    document = root / "pipeline.json"
    document.write_text(json.dumps(dict(
        version=args.version, id=task, connections=connections,
        policy=dict(buffers=dict(connection="spill", residentBytes=128,
                                 workingBytes=8388608, spillBytes=16777216, maxBlocks=128)),
        controlFlow=dict(tasks=[dict(id=task, kind="dataFlow", config=dict(pipeline="etl"))],
                         constraints=[]),
        dataFlows=[dict(id="etl", components=components, paths=paths)],
    )), encoding="utf-8")
    return document, state


def discover(args, document, table):
    package = json.loads(document.read_text(encoding="utf-8"))
    package["connections"][0]["credentialReference"] = "fixture"
    metadata = document.with_name("discovery.json")
    metadata.write_text(json.dumps(package), encoding="utf-8")
    discovered = document.with_name("fields.json")
    result = subprocess.run(
        [str(args.queries), "--discover", str(metadata), "source", table, str(discovered)],
        capture_output=True, text=True, encoding="utf-8", env=args.pg_env, timeout=30,
    )
    assert result.returncode == 0, (result.returncode, result.stderr)
    return json.loads(discovered.read_text(encoding="utf-8"))["fields"]


def committed_records(state):
    """Read intact append-only journal frames, using the native length framing."""
    for path in sorted(state.rglob("segment-*.udg")):
        data = path.read_bytes()
        offset = 0
        while offset < len(data):
            length, = struct.unpack_from("<Q", data, offset)
            frame = data[offset + 8:offset + 8 + length]
            assert len(frame) == length and frame[-8:] == b"UCMTEND5", path.name
            assert hashlib.sha256(frame[:-40]).digest() == frame[-40:-8], path.name
            count, = struct.unpack_from("<Q", frame, 48)
            at = 56
            for _ in range(count):
                kind, = struct.unpack_from("<Q", frame, at)
                at += 8
                items = []
                for _ in range(3):
                    size, = struct.unpack_from("<Q", frame, at)
                    at += 8
                    items.append(frame[at:at + size])
                    at += size
                if items[2]:
                    yield kind, json.loads(items[2])
            assert at == len(frame) - 40
            offset += length + 8


def wal_commits(args):
    stream = query(args, f"""
        SELECT encode(data, 'hex')
        FROM pg_logical_slot_peek_binary_changes('{SLOT}', NULL, NULL,
             'proto_version', '1', 'publication_names', '{PUBLICATION}');
    """).splitlines()
    commits, changes = [], []
    for encoded in stream:
        message = bytes.fromhex(encoded)
        if message[:1] == b"B":
            _, transaction_time, xid = struct.unpack(">QQI", message[1:])
            changes = []
        elif message[:1] in (b"I", b"U", b"D"):
            changes.append(message[:1].decode("ascii"))
        elif message[:1] == b"C":
            _, _, end, timestamp = struct.unpack(">BQQQ", message[1:])
            assert timestamp == transaction_time
            commits.append((xid, f"{end >> 32:X}/{end & 0xffffffff:X}", timestamp + PG_EPOCH_US,
                            tuple(changes)))
    assert len({item[0] for item in commits}) == 3, commits
    assert [item[3] for item in commits] == [("I", "I"), ("U", "I"), ("D", "U")], commits
    assert [item[2] for item in commits] == sorted({item[2] for item in commits}), commits
    return commits


def cdc_setup(args):
    query(args, f"""
        CREATE TABLE {SOURCE}(id bigint PRIMARY KEY, flag boolean NOT NULL, label text NOT NULL);
        ALTER TABLE {SOURCE} REPLICA IDENTITY FULL;
        CREATE TABLE {TARGET}(LIKE {SOURCE} INCLUDING ALL);
        CREATE TABLE {AUDIT}(
          ordinal bigint GENERATED ALWAYS AS IDENTITY, operation text NOT NULL,
          id bigint NOT NULL, flag boolean NOT NULL, label text NOT NULL
        );
        CREATE FUNCTION {AUDIT_FUNCTION}() RETURNS trigger LANGUAGE plpgsql AS $audit$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            INSERT INTO {AUDIT}(operation,id,flag,label) VALUES(TG_OP,OLD.id,OLD.flag,OLD.label);
            RETURN OLD;
          END IF;
          INSERT INTO {AUDIT}(operation,id,flag,label) VALUES(TG_OP,NEW.id,NEW.flag,NEW.label);
          RETURN NEW;
        END
        $audit$;
        CREATE TRIGGER ud_etl_cdc_audit AFTER INSERT OR UPDATE OR DELETE ON {TARGET}
          FOR EACH ROW EXECUTE FUNCTION {AUDIT_FUNCTION}();
        CREATE PUBLICATION {PUBLICATION} FOR TABLE {SOURCE};
    """)
    lsn = query(args, f"SET statement_timeout='120s'; "
                      f"SELECT lsn FROM pg_create_logical_replication_slot('{SLOT}','pgoutput');",
                timeout=135)
    system_id, timeline = query(args, """
        SELECT system_identifier, timeline_id FROM pg_control_system(), pg_control_checkpoint();
    """).split("\t")
    for statements in (
        f"INSERT INTO {SOURCE} VALUES (1,true,'alpha'), (2,false,'beta');",
        f"UPDATE {SOURCE} SET flag=false,label='gamma' WHERE id=1; "
        f"INSERT INTO {SOURCE} VALUES (3,true,'delta');",
        f"DELETE FROM {SOURCE} WHERE id=2; "
        f"UPDATE {SOURCE} SET flag=true,label='omega' WHERE id=1;",
    ):
        query(args, "BEGIN; " + statements + " COMMIT;")
    return dict(systemId=system_id, timeline=int(timeline), lsn=lsn)


def cdc_cleanup(args):
    query(args, f"""
        SELECT pg_drop_replication_slot(slot_name)
          FROM pg_replication_slots WHERE slot_name = '{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS {TARGET}, {SOURCE}, {AUDIT};
        DROP FUNCTION IF EXISTS {AUDIT_FUNCTION}();
        DO $cleanup$ BEGIN
          IF to_regclass('public.ud_sdk_receipts_v11') IS NOT NULL THEN
            DELETE FROM public.ud_sdk_receipts_v11 WHERE tenant='fixture' AND job='{JOB}';
          END IF;
        END $cleanup$;
    """)


def rows(args, sql):
    return json.loads(query(args, sql))


def slot_confirmed(args, lsn):
    sql = f"""
        SELECT confirmed_flush_lsn >= '{lsn}'::pg_lsn
        FROM pg_replication_slots WHERE slot_name='{SLOT}';
    """
    deadline = time.monotonic() + 3
    while query(args, sql) != "t":
        assert time.monotonic() < deadline, "the provider did not confirm the last WAL position"
        time.sleep(0.1)


def cdc(args, root):
    cdc_cleanup(args)
    try:
        position = cdc_setup(args)
        commits = wal_commits(args)
        schema = dict(version=args.version, fields=[
            dict(name="id", type="int64", nullable=False, keyOrder=1),
            dict(name="flag", type="bool", nullable=False),
            dict(name="label", type="utf8", nullable=False),
        ])
        components = [
            dict(id="source", component="postgresql.cdc.source", config=dict(
                connection="source", table=SOURCE, schema=schema, mode="stream", slot=SLOT,
                publication=PUBLICATION, position=json.dumps(position, separators=(",", ":")),
                microBatchRows=1000, microBatchMs=30000, maxTransactions=3,
            )),
            dict(id="destination", component="sql.destination", config=dict(
                connection="target", table=TARGET, mode="changeset", beforeField="before",
                keys=[0], provisionReceipts=True, schemaPolicy=dict(
                    policy="explicitMapping",
                    mapping=[dict(expected=name, actual=name) for name in ("id", "flag", "label")],
                ),
            )),
        ]
        document, state = pipeline(args, root, CDC_TASK, components, [
            dict(**{"from": "source", "to": "destination"}, output=0, input=0),
        ])
        fields = discover(args, document, SOURCE)
        assert [field["name"] for field in fields] == ["id", "flag", "label"], fields
        assert fields[0].get("keyOrder") == 1, fields
        package = json.loads(document.read_text(encoding="utf-8"))
        package["dataFlows"][0]["components"][0]["config"]["schema"]["fields"] = fields
        document.write_text(json.dumps(package), encoding="utf-8")
        native(args, document, state, crash=True)
        intents = [payload for kind, payload in committed_records(state)
                   if kind == 11 and "cuts" in payload]
        assert len(intents) == 1 and intents[0]["epoch"] == 1, intents
        cuts = intents[0]["cuts"]
        assert [cut[0] for cut in cuts] == [1, 2, 3], cuts
        assert [cut[1] for cut in cuts] == [item[2] for item in commits], cuts
        final_position = dict(position, lsn=commits[-1][1])
        assert json.loads(intents[0]["position"]) == final_position, intents[0]
        receipt_sql = f"""
            SELECT coalesce(json_agg(json_build_array(batch_id, encode(digest,'hex'),
                                                      source_position) ORDER BY batch_id),'[]'::json)
            FROM public.ud_sdk_receipts_v11 WHERE tenant='fixture' AND job='{JOB}';
        """
        audit_sql = f"""
            SELECT json_agg(json_build_array(operation,id,flag,label) ORDER BY ordinal)
            FROM {AUDIT};
        """
        expected_audit = [["INSERT", 1, True, "alpha"], ["INSERT", 2, False, "beta"],
                          ["UPDATE", 1, False, "gamma"], ["INSERT", 3, True, "delta"],
                          ["DELETE", 2, False, "beta"], ["UPDATE", 1, True, "omega"]]
        first_receipts = rows(args, receipt_sql)
        assert len(first_receipts) == 1 and first_receipts[0][0] == "0", first_receipts
        assert rows(args, audit_sql) == expected_audit[:2], "first provider commit before resume"
        resumed = native(args, document, state, resume=True)
        flow = next(flow for flow in resumed["diagnostics"]["pipelines"] if flow["id"] == "etl")
        assert flow["confirmedEpoch"] == 1, flow
        completed = [payload for kind, payload in committed_records(state) if kind == 14]
        assert [payload["epoch"] for payload in completed] == [1], completed
        assert json.loads(completed[0]["sources"][0]["position"]) == final_position, completed
        receipts = rows(args, receipt_sql)
        assert receipts[:1] == first_receipts and [row[0] for row in receipts] == ["0", "1", "2"], receipts
        assert rows(args, audit_sql) == expected_audit, "resume must not repeat trigger effects"
        final_rows = f"SELECT json_agg(json_build_array(id,flag,label) ORDER BY id) FROM "
        expected = [[1, True, "omega"], [3, True, "delta"]]
        assert rows(args, final_rows + TARGET) == rows(args, final_rows + SOURCE) == expected
        slot_confirmed(args, commits[-1][1])
        print("PostgreSQL CDC passed: 3 real transactions, 6 ordered changes, WAL commit "
              "timestamps preserved in epoch 1, crash and idempotent receipt recovery")
    finally:
        cdc_cleanup(args)


def scan(args, root):
    query(args, f"DROP TABLE IF EXISTS {SCAN};")
    try:
        query(args, f"""
            CREATE TABLE {SCAN}(id bigint PRIMARY KEY, flag boolean NOT NULL,
                                discarded bigint NOT NULL);
            INSERT INTO {SCAN} SELECT n, n%2=0, n*100 FROM generate_series(1,13) n;
        """)
        expected = rows(args, f"""
            SELECT json_agg(json_build_object('id',id,'flag',flag) ORDER BY id)
            FROM {SCAN} WHERE id > 3 AND flag AND id > 5;
        """)
        for mode in ("pushed", "raw"):
            directory = root / mode
            directory.mkdir()
            config = dict(
                connection="source", table=SCAN, batchSize=2,
                schema=dict(version=args.version, fields=[
                    dict(name="id", type="int64", nullable=False, keyOrder=1),
                    dict(name="flag", type="bool", nullable=False),
                ]),
                schemaPolicy=dict(policy="compatible"),
                incremental=dict(mode="watermark", keys=["id"], after=[3], unique=True, monotonic=True),
                where="[flag] && [id] > 5L",
            )
            if mode == "raw":
                config["sql"] = f"SELECT id, flag, discarded FROM {SCAN}"
            components = [
                dict(id="scan", component="sql.source", config=config),
                dict(id="output", component="file.ndjson.destination",
                     config=dict(connection="output", fileMode="overwrite")),
            ]
            document, state = pipeline(args, directory, "pg-keyed-scan", components, [
                dict(**{"from": "scan", "to": "output"}, output=0, input=0),
            ])
            fields = discover(args, document, SCAN)
            assert [field["name"] for field in fields] == ["id", "flag", "discarded"], fields
            assert fields[0].get("keyOrder") == 1, fields
            package = json.loads(document.read_text(encoding="utf-8"))
            package["dataFlows"][0]["components"][0]["config"]["schema"]["fields"] = fields[:2]
            document.write_text(json.dumps(package), encoding="utf-8")
            facts = native(args, document, state)
            flow = next(flow for flow in facts["diagnostics"]["pipelines"] if flow["id"] == "etl")
            node = next(node for tree in flow["trees"] for node in tree["nodes"] if node["id"] == "scan")
            assert node["facts"]["predicatePushed"] is (mode == "pushed"), node["facts"]
            assert node["facts"]["readColumns"] == (2 if mode == "pushed" else 3), node["facts"]
            actual = [json.loads(line) for line in (directory / "output.ndjson").read_text().splitlines()]
            assert actual == expected, (mode, actual, expected)
        print("PostgreSQL keyed scan passed: projection read 2 of 3 columns and pushed the "
              "predicate, equivalent to raw SQL and to a provider-side row oracle")
    finally:
        query(args, f"DROP TABLE IF EXISTS {SCAN};")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("runner", "queries", "plugin", "psql", "config"):
        parser.add_argument("--" + name, type=lambda path: Path(path).resolve(), required=True)
    parser.add_argument("--case", choices=("all", "scan", "cdc"), default="all")
    args = parser.parse_args()
    product = json.loads((Path(__file__).resolve().parents[1] / "metadata/product.json").read_text(
        encoding="utf-8"))
    args.version = product["product"]["protocolVersion"]
    args.pg_env = postgres_environment(args.config)
    with tempfile.TemporaryDirectory(prefix="ud-pg-etl-epochs-") as work:
        for name, test in (("scan", scan), ("cdc", cdc)):
            if args.case in ("all", name):
                root = Path(work) / name
                root.mkdir()
                test(args, root)


if __name__ == "__main__":
    main()
