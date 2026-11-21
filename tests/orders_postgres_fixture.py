"""Disposable verified-TLS PostgreSQL fixture for the orders gate."""

import os
from pathlib import Path
import shutil
import uuid

from sdk_fluent_fixture import _postgres_environment

from orders_fixture_support import command, free_port, quote_identifier, quote_literal


class PostgresFixture:
    """A private TLS PostgreSQL cluster, or an explicitly designated one."""

    def __init__(self, root, config_path, order_time):
        self.root = root
        self.external_config = Path(config_path).resolve() if config_path else None
        self.order_time = order_time
        self.data = root / "postgres-data"
        self.socket = root / "postgres-socket"
        self.socket.mkdir()
        self.process_started = False
        self.psql = Path(os.environ.get("UD_TEST_PSQL", "psql"))
        self.pg_ctl = Path(os.environ.get("UD_TEST_PG_CTL", "pg_ctl"))
        self.initdb = Path(os.environ.get("UD_TEST_INITDB", "initdb"))
        self.openssl = Path(os.environ.get("UD_TEST_OPENSSL", "openssl"))
        pg_bin = os.environ.get(
            "UD_TEST_PG_BIN",
            "/workspace/work/universaldata-sql/deps/sysroot/usr/lib/postgresql/17/bin")
        if pg_bin:
            pg_bin = Path(pg_bin)
            self.psql = pg_bin / "psql"
            self.pg_ctl = pg_bin / "pg_ctl"
            self.initdb = pg_bin / "initdb"
        tag = uuid.uuid4().hex[:10]
        self.table = "ud_orders_" + tag
        # Native creates this explicitly named slot during snapshot-and-stream;
        # the fixture never pre-creates it, so cleanup can remove exactly this
        # tagged slot if the coordinator exits before the source closes.
        self.slot = "ud_orders_slot_" + tag
        self.publication = "ud_orders_pub_" + tag
        self.conninfo = None
        self.port = None

    @staticmethod
    def _find_binary(path):
        return path if path.is_file() else shutil.which(str(path))

    def _local(self):
        required = [self._find_binary(item) for item in
                    (self.psql, self.pg_ctl, self.initdb, self.openssl)]
        if any(item is None for item in required):
            raise RuntimeError("local PostgreSQL TLS fixture needs psql, initdb, pg_ctl and openssl")
        self.psql, self.pg_ctl, self.initdb, self.openssl = map(Path, required)
        self.data.mkdir(mode=0o700)
        password = "fixture-" + uuid.uuid4().hex
        password_file = self.root / "postgres-password"
        password_file.write_text(password, encoding="utf-8")
        password_file.chmod(0o600)
        command([self.initdb, "-D", self.data, "-U", "fixture", "--pwfile", password_file,
                 "-A", "scram-sha-256", "--no-locale", "-E", "UTF8"])
        command([self.openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                 "-keyout", self.data / "server.key", "-out", self.data / "server.crt",
                 "-subj", "/CN=localhost", "-addext",
                 "subjectAltName=DNS:localhost,IP:127.0.0.1", "-addext",
                 "extendedKeyUsage=serverAuth"])
        (self.data / "server.key").chmod(0o600)
        self.port = free_port()
        with (self.data / "postgresql.conf").open("a", encoding="utf-8") as stream:
            stream.write("\nlisten_addresses='127.0.0.1'\nport=%d\nssl=on\n"
                         "wal_level=logical\nmax_replication_slots=4\nmax_wal_senders=4\n" % self.port)
        (self.data / "pg_hba.conf").write_text(
            "local all all scram-sha-256\n"
            "hostssl all all 127.0.0.1/32 scram-sha-256\n"
            "hostssl replication all 127.0.0.1/32 scram-sha-256\n", encoding="utf-8")
        self.conninfo = ("host=localhost hostaddr=127.0.0.1 port=%d user=fixture "
                         "dbname=postgres password=%s sslmode=verify-full sslrootcert=%s "
                         "connect_timeout=10" %
                         (self.port, password, self.data / "server.crt"))
        self.config_path = self.root / "postgres-conninfo"
        self.config_path.write_text(self.conninfo, encoding="utf-8")
        self.config_path.chmod(0o600)
        command([self.pg_ctl, "-D", self.data, "-l", self.root / "postgres.log",
                 "-o", "-k %s" % self.socket, "-w", "start"])
        self.process_started = True

    def _run_sql(self, statement):
        # Keep the password in libpq's environment rather than passing the
        # full conninfo on argv, where psql errors and process listings expose it.
        command([self.psql, "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1",
                 "-c", statement], timeout=60,
                env=_postgres_environment(self.config_path))

    def start(self):
        try:
            if self.external_config:
                if not self.external_config.is_file():
                    raise RuntimeError("designated PostgreSQL config is not a file")
                text = self.external_config.read_text(encoding="utf-8").strip()
                if not text or "sslmode=verify" not in text or "sslrootcert=" not in text:
                    raise RuntimeError("PostgreSQL config must use sslmode=verify-* and sslrootcert")
                self.conninfo = text
                self.config_path = self.external_config
                self.psql = Path(self._find_binary(self.psql) or "")
                if not self.psql.is_file():
                    raise RuntimeError("psql is required for the designated PostgreSQL fixture")
            else:
                self._local()
            self._run_sql("CREATE TABLE %s (order_id BIGINT NOT NULL PRIMARY KEY, "
                          "shop_id BIGINT NOT NULL, order_time BIGINT NOT NULL)" %
                          quote_identifier(self.table))
            self._run_sql("ALTER TABLE %s REPLICA IDENTITY FULL" % quote_identifier(self.table))
            self._run_sql("CREATE PUBLICATION %s FOR TABLE %s" %
                          (quote_identifier(self.publication), quote_identifier(self.table)))
            # Snapshot-and-stream is a real PostgreSQL CDC flow.  Native creates
            # this tagged slot and captures a repeatable snapshot of this row,
            # then seals the WAL stream at the bounded transaction limit.
            self._run_sql("INSERT INTO %s VALUES (1, 7, %d)" %
                          (quote_identifier(self.table), self.order_time))
            return self
        except Exception:
            self.cleanup()
            raise

    def cleanup(self):
        if not self.conninfo:
            return
        for statement in (
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s" % quote_literal(self.slot),
                "DROP PUBLICATION IF EXISTS %s" % quote_identifier(self.publication),
                "DROP TABLE IF EXISTS %s" % quote_identifier(self.table)):
            try:
                self._run_sql(statement)
            except Exception as error:
                print("WARNING: PostgreSQL fixture cleanup failed: %s" % error, flush=True)
        if self.process_started:
            command([self.pg_ctl, "-D", self.data, "-m", "immediate", "-w", "stop"],
                    timeout=60, check=False)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, _value, _trace):
        self.cleanup()
