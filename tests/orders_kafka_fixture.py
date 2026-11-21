"""Disposable real Kafka fixture for the orders gate."""

import json
import os
from pathlib import Path
import re
import shutil
import uuid

from orders_fixture_support import command


class KafkaFixture:
    """Publish a bounded set of JSON records to a private topic."""

    def __init__(self, root, config_path, records):
        self.root = root
        self.config_path = Path(config_path).resolve()
        self.records = tuple(records)
        self.topic = None
        self.group = "ud-orders-payments-" + uuid.uuid4().hex[:12]
        self.bootstrap = None
        self.command_config = None
        self.credential_path = None
        self.tools = {}
        self.container = None
        self.tool_bootstrap = None

    def _config(self):
        try:
            value = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError("Kafka fixture config must be JSON: %s" % error) from error
        if value.get("broker") != "kafka":
            raise RuntimeError("Kafka fixture config must identify broker=kafka")
        credentials = value.get("credentials") or {}
        self.bootstrap = credentials.get("bootstrapServers") or value.get("bootstrapServers")
        if not self.bootstrap:
            endpoints = value.get("endpoints") or []
            if endpoints:
                self.bootstrap = re.sub(r"^kafkas?://", "", endpoints[0])
        if not self.bootstrap:
            raise RuntimeError("Kafka fixture config has no bootstrapServers")
        requested_topic = value.get("ordersTopic") or value.get("topic") or value.get("jsonSource") or os.environ.get(
            "UD_TEST_ORDERS_KAFKA_TOPIC")
        if not requested_topic:
            raise RuntimeError("set UD_TEST_ORDERS_KAFKA_TOPIC or ordersTopic in Kafka config")
        self.topic = requested_topic + "-" + uuid.uuid4().hex[:10]
        self.command_config = value.get("commandConfig") or credentials.get("commandConfig")
        tools = value.get("tools") or {}
        for key in ("topicsTool", "producerTool", "offsetsTool"):
            env_name = {"topicsTool": "UD_TEST_KAFKA_TOPICS_TOOL",
                        "producerTool": "UD_TEST_KAFKA_PRODUCER_TOOL",
                        "offsetsTool": "UD_TEST_KAFKA_OFFSETS_TOOL"}[key]
            self.tools[key] = value.get(key) or tools.get(key) or os.environ.get(env_name)
        self.container = value.get("container") or value.get("kafkaContainer")
        self.tool_bootstrap = (value.get("containerBootstrapServers", "127.0.0.1:9092")
                               if self.container else self.bootstrap)
        self.credential_path = self.root / "kafka-credentials.json"
        credential = {key: credentials[key] for key in
                      ("bootstrapServers", "tls", "username", "password", "caFile")
                      if key in credentials}
        if "bootstrapServers" not in credential:
            credential["bootstrapServers"] = self.bootstrap
        self.credential_path.write_text(json.dumps(credential), encoding="utf-8")
        self.credential_path.chmod(0o600)

    def _tool(self, key, default_name):
        configured = self.tools.get(key)
        if configured:
            path = shutil.which(str(configured)) or (str(configured) if Path(configured).is_file() else None)
            if path:
                return [path]
        path = shutil.which(default_name)
        if path:
            return [path]
        if self.container:
            return ["docker", "exec", "-i", str(self.container), "/opt/kafka/bin/" + default_name]
        raise RuntimeError("Kafka %s is not available; configure tools or container" % key)

    def _config_args(self):
        if not self.command_config:
            return []
        path = Path(self.command_config)
        if not path.is_file():
            raise RuntimeError("Kafka commandConfig is not a file")
        return ["--command-config", str(path)]

    def _run(self, args, input_text=None):
        return command(args, timeout=90, input_text=input_text)

    def start(self):
        if not self.config_path.is_file():
            raise RuntimeError("designated Kafka config is unavailable")
        self._config()
        topics = self._tool("topicsTool", "kafka-topics.sh")
        self._run(topics + ["--bootstrap-server", self.tool_bootstrap, "--create", "--if-not-exists",
                            "--topic", self.topic, "--partitions", "1", "--replication-factor", "1"] +
                 self._config_args())
        producer = self._tool("producerTool", "kafka-console-producer.sh")
        payload = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in self.records)
        self._run(producer + ["--bootstrap-server", self.tool_bootstrap, "--topic", self.topic] +
                  self._config_args(), input_text=payload)
        offsets = self._tool("offsetsTool", "kafka-get-offsets.sh")
        result = self._run(offsets + ["--bootstrap-server", self.tool_bootstrap, "--topic", self.topic,
                                      "--time", "-1"] + self._config_args())
        values = re.findall(r"(?:^|:)\s*(\d+)\s*$", result.stdout, re.MULTILINE)
        if not values:
            raise RuntimeError("Kafka offset tool returned no end offset: " + result.stdout)
        self.end_offset = int(values[-1])
        if self.end_offset != len(self.records):
            raise RuntimeError("Kafka topic was not empty before fixture publish; expected end offset "
                               "%d, got %d" % (len(self.records), self.end_offset))
        return self

    def cleanup(self):
        if not self.topic:
            return
        try:
            self._run(self._tool("topicsTool", "kafka-topics.sh") +
                      ["--bootstrap-server", self.tool_bootstrap, "--delete", "--topic", self.topic] +
                      self._config_args())
        except Exception as error:
            print("WARNING: Kafka fixture cleanup failed: %s" % error, flush=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, _value, _trace):
        self.cleanup()
