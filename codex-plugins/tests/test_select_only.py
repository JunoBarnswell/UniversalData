import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPTS = Path(__file__).resolve().parents[1] / "sql-select-only" / "skills" / "sql-select-only" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from select_only import validate_select


class SelectOnlyTests(unittest.TestCase):
    def test_match_mysql_reports_only_metadata_and_excludes_tunnels(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "fixture.ncx"
            source.write_text(
                '<Connections>'
                '<Connection ConnectionName="生产" ConnType="MYSQL" Host="internal-host" '
                'UserName="private-login" Password="private-password" />'
                '<Connection ConnectionName="另一库" ConnType="MYSQL" Host="INTERNAL-HOST" '
                'Database="another_db" UserName="different-login" />'
                '<Connection ConnectionName="隧道" ConnType="MYSQL" Host="internal-host" SSH="true" />'
                '<Connection ConnectionName="另一端口" ConnType="MYSQL" Host="internal-host" Port="3307" />'
                '</Connections>', encoding="utf-8",
            )
            command = [sys.executable, "-B", str(SCRIPTS / "select_only.py"),
                       "--connections", str(source), "match-mysql"]
            metadata = {"host": "internal-host", "port": 3306,
                        "database": "account_manage", "username": "private-login"}
            result = subprocess.run(command, input=json.dumps(metadata), text=True, encoding="utf-8",
                                    capture_output=True, check=True)
            for private in ("internal-host", "private-login", "private-password", "different-login"):
                self.assertNotIn(private, result.stdout)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["database"], "account_manage")
            self.assertEqual([match["name"] for match in payload["matches"]], ["生产", "另一库"])
            primary, other = payload["matches"]
            self.assertTrue(primary["login_matches"])
            self.assertIsNone(primary["database_matches"])
            self.assertFalse(other["login_matches"])
            self.assertFalse(other["database_matches"])

            result = subprocess.run(command, input=json.dumps({**metadata, "password": "private-input"}),
                                    text=True, encoding="utf-8", capture_output=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertNotIn("private-input", result.stdout)

    def test_read_only_queries(self):
        queries = {
            "mysql": (
                "SELECT a.id, COUNT(b.id) AS n FROM a LEFT JOIN b ON a.id = b.a_id "
                "GROUP BY a.id HAVING COUNT(b.id) > 0 ORDER BY a.id LIMIT 20",
                "SELECT STR_TO_DATE('2026-11-23', '%Y-%m-%d')",
                r"SELECT '文本; UPDATE' AS `内容`, 'it\'s' AS quoted",
                "SELECT 1 UNION ALL SELECT 2",
                "SELECT id FROM t WHERE id IN (SELECT other_id FROM x)",
            ),
            "postgres": (
                "WITH c AS (SELECT id FROM app.t) SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM c",
                "SELECT CAST(amount AS numeric(12, 2)), date_trunc('month', booked_at) FROM sales",
                "SELECT '/* 普通文本 */' AS label",
                "SELECT id FROM inventory WHERE name ILIKE 'ab%' OFFSET 10 LIMIT 20",
            ),
            "tsql": (
                "SELECT TOP (20) id, display_name FROM dbo.people ORDER BY id",
                "SELECT id FROM dbo.people ORDER BY id OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY",
                "SELECT CAST(amount AS decimal(12, 2)), DATEADD(day, -1, GETDATE()) FROM dbo.sales",
                "SELECT N'中文' AS [说明]",
            ),
        }
        for dialect, statements in queries.items():
            for statement in statements:
                with self.subTest(dialect=dialect, statement=statement):
                    approved = validate_select(statement, dialect)
                    self.assertTrue(approved.startswith(("SELECT", "WITH")))
                    self.assertEqual(validate_select(approved, dialect), approved)

    def test_rejected_queries(self):
        shared = (
            "",
            "SELECT",
            "SELECT * FROM",
            "SELECT 1; SELECT 2",
            "SELECT 1; DELETE FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "TRUNCATE TABLE t",
            "DROP TABLE t",
            "CREATE TABLE t (id INT)",
            "GRANT SELECT ON t TO app",
            "SELECT unknown_function(x) FROM t",
        )
        dialect_specific = {
            "mysql": (
                "SELECT 1 INTO OUTFILE '/tmp/results'",
                "SELECT 1 INTO @result",
                "SELECT @result := 1",
                "SELECT * FROM t FOR UPDATE",
                "SELECT LOAD_FILE('/tmp/results')",
                "SELECT 1 /*!50000 INTO OUTFILE '/tmp/results' */",
                "SELECT 1 /*!50000 ; DELETE FROM t */",
                "SELECT /*+ MAX_EXECUTION_TIME(1) */ id FROM t",
                "CALL test_procedure()",
            ),
            "postgres": (
                "WITH c AS (DELETE FROM t RETURNING id) SELECT id FROM c",
                "SELECT id INTO new_table FROM t",
                "SELECT * FROM t FOR SHARE",
                "SELECT nextval('seq')",
                "SELECT pg_catalog.pg_advisory_lock(123)",
                "SELECT CAST('x' AS app.custom_type)",
                "COPY t TO STDOUT",
            ),
            "tsql": (
                "SELECT id INTO dbo.result FROM dbo.t",
                "SELECT @result = id FROM t",
                "SELECT * FROM dbo.t WITH (UPDLOCK)",
                "SELECT NEXT VALUE FOR dbo.sequence_id",
                "SELECT * FROM OPENQUERY(remote, 'SELECT 1')",
                "SELECT * FROM remote.catalog.dbo.t",
                "SELECT 1\nGO\nSELECT 2",
                "EXEC dbo.test_procedure",
            ),
        }
        for dialect, statements in dialect_specific.items():
            for statement in shared + statements:
                with self.subTest(dialect=dialect, statement=statement):
                    with self.assertRaises(ValueError):
                        validate_select(statement, dialect)

    def test_cli_uses_only_listed_sql_sources_and_emits_only_accepted_sql(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "fixture.ncx"
            source.write_text(
                '<Connections><Connection ConnectionName="销售" ConnType="MYSQL" '
                'Host="internal-host" UserName="secret-user" Password="secret-password" />'
                '<Connection ConnectionName="文档" ConnType="MONGODB" /></Connections>',
                encoding="utf-8",
            )
            cmd = [sys.executable, "-B", str(SCRIPTS / "select_only.py"),
                   "--connections", str(source)]
            result = subprocess.run(cmd + ["list"], text=True, encoding="utf-8",
                                    capture_output=True, check=True)
            self.assertNotIn("secret-", result.stdout)
            self.assertNotIn("internal-host", result.stdout)
            mysql, mongo = json.loads(result.stdout)["sources"]
            self.assertEqual(mysql["name"], "销售")
            self.assertTrue(mysql["supported"])
            self.assertFalse(mongo["supported"])

            cases = (
                (mysql["id"], "SELECT '中文'", 0),
                (mysql["id"], "DELETE FROM t", 2),
                (mongo["id"], "SELECT 1", 2),
                ("unknown", "SELECT 1", 2),
            )
            for source_id, query, exit_code in cases:
                with self.subTest(source_id=source_id, exit_code=exit_code):
                    result = subprocess.run(
                        cmd + ["validate", "--source-id", source_id], input=query,
                        text=True, encoding="utf-8", capture_output=True,
                    )
                    self.assertEqual(result.returncode, exit_code, result.stderr)
                    output = json.loads(result.stdout)
                    if exit_code == 0:
                        self.assertEqual(output["status"], "accepted")
                        self.assertIn("中文", output["sql"])
                    else:
                        self.assertEqual(output["status"], "rejected")
                        self.assertNotIn("sql", output)


if __name__ == "__main__":
    unittest.main()
