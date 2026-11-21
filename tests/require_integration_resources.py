"""Complete-product CI refuses missing resources instead of counting skips."""
import os
import itertools
import json
import pathlib
import sys

required = ["UD_TEST_" + driver + "_CONFIG" for driver in ("POSTGRESQL", "MYSQL", "MARIADB", "ODBC", "ORACLE_OCI")]
required += ["UD_TEST_" + broker + "_CONFIG" for broker in (
    "ACTIVEMQ_CLASSIC", "ACTIVEMQ_ARTEMIS", "RABBITMQ", "ROCKETMQ", "KAFKA", "KAFKA_REGISTRY", "KAFKA_TAKEOVER")]
required += ["UD_TEST_" + feature + "_CONFIG" for feature in ("FTP", "SFTP", "SMTP", "S3", "AZURE_BLOB", "ADLS", "SSAS")]
required += ["UD_SSIS_GOLDEN_MANIFEST", "UD_SSIS_NATIVE_OBSERVATIONS"]
required += ["UD_TEST_CATALOG_POSTGRESQL_CONNECTION_FILE", "UD_TEST_CLUSTER_KAFKA_ELASTIC_CONFIG"]
required += ["UD_TEST_FLUENT_XA_CONFIG"]
required += ["UD_TEST_ORDERS_PG_CONFIG", "UD_TEST_ORDERS_MYSQL_CONFIG", "UD_TEST_ORDERS_KAFKA_CONFIG",
             "UD_TEST_ICEBERG_S3_CREDENTIALS"]
if sys.platform.startswith("linux"):
    required += ["UD_TEST_SENTINEL_POSTGRESQL_CONNECTION_FILE"]
if sys.platform == "win32":
    required += ["UD_TEST_WMI_CONFIG"]
missing = [name for name in required if not os.environ.get(name) or not pathlib.Path(os.environ[name]).is_file()]
missing += [name for name in ("UD_TEST_ICEBERG_CATALOG", "UD_TEST_TRINO_URL", "UD_TEST_ICEBERG_S3_RESOURCE")
            if not os.environ.get(name)]
storage = os.environ.get("UD_TEST_ICEBERG_STORAGE")
if not storage or not pathlib.Path(storage).is_dir():
    missing.append("UD_TEST_ICEBERG_STORAGE")
if os.environ.get("UD_TEST_FLUENT_LOAD") != "1" or os.environ.get("UD_TEST_FLUENT_LOAD_REQUIRE_LIMITS") == "0":
    missing.append("UD_TEST_FLUENT_LOAD=1 with per-process limits")
if missing:
    raise SystemExit("Blocked: complete product resources missing: " + ", ".join(missing))
try:
    groups = json.loads(pathlib.Path(os.environ["UD_TEST_FLUENT_XA_CONFIG"]).read_text(encoding="utf-8"))
    kinds = [tuple(sorted(source["kind"].lower() for source in group)) for group in groups]
    expected = set(itertools.combinations_with_replacement(sorted(("mariadb", "mysql", "oracle", "postgresql", "sqlserver")), 2))
    if not expected.issubset(kinds) or not any(len(group) == 3 and len(set(group)) == 3 for group in kinds):
        raise ValueError("Incomplete XA fixture coverage")
except (OSError, ValueError, KeyError, TypeError, AttributeError):
    raise SystemExit("SDK XA release coverage requires all 15 database pairs and one three-provider group") from None
print("All independent acceptance resource references are configured")
