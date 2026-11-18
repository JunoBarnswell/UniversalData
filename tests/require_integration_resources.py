"""Complete-product CI refuses missing resources instead of counting skips."""
import os
import pathlib
import sys

required = ["UD_TEST_" + driver + "_CONFIG" for driver in ("POSTGRESQL", "MYSQL", "MARIADB", "ODBC", "ORACLE_OCI")]
required += ["UD_TEST_" + broker + "_CONFIG" for broker in (
    "ACTIVEMQ_CLASSIC", "ACTIVEMQ_ARTEMIS", "RABBITMQ", "ROCKETMQ", "KAFKA", "KAFKA_REGISTRY", "KAFKA_TAKEOVER")]
required += ["UD_TEST_" + feature + "_CONFIG" for feature in ("FTP", "SFTP", "SMTP", "S3", "AZURE_BLOB", "ADLS", "SSAS")]
required += ["UD_SSIS_GOLDEN_MANIFEST", "UD_SSIS_NATIVE_OBSERVATIONS"]
if sys.platform == "win32":
    required += ["UD_TEST_WMI_CONFIG"]
missing = [name for name in required if not os.environ.get(name) or not pathlib.Path(os.environ[name]).is_file()]
if missing:
    raise SystemExit("Blocked: complete product resources missing: " + ", ".join(missing))
print("All independent acceptance resource references are configured")
