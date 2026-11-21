"""Disposable verified-TLS MySQL fixture for the orders gate."""

import json
from pathlib import Path
import uuid

from orders_fixture_support import command, mysql_identifier


class MySqlFixture:
    """Use only an explicitly designated disposable TLS MySQL database."""

    def __init__(self, root, config_path, helper, plugin):
        self.root = root
        self.config = Path(config_path).resolve()
        self.helper = Path(helper)
        self.plugin = Path(plugin)
        self.table = "ud_orders_shops_" + uuid.uuid4().hex[:10]

    def _config_has_tls(self):
        try:
            value = json.loads(self.config.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError("MySQL fixture config must be JSON: %s" % error) from error

        def walk(item):
            if isinstance(item, dict):
                for key, child in item.items():
                    normalized = str(key).lower()
                    if normalized in ("tls", "ssl", "usessl") and child is True:
                        return True
                    if normalized in ("cafile", "ca", "sslca", "sslrootcert") and child:
                        return Path(str(child)).is_file()
                    if walk(child):
                        return True
            elif isinstance(item, list):
                return any(walk(child) for child in item)
            return False

        if not walk(value):
            raise RuntimeError("MySQL fixture must explicitly enable TLS and point to a CA file")

    def _sql(self, statement):
        query = self.root / "mysql-query.sql"
        query.write_text(statement, encoding="utf-8")
        command([self.helper, self.plugin, self.config, "execute", query], timeout=120)

    def start(self):
        if not self.config.is_file() or not self.helper.is_file() or not self.plugin.is_file():
            raise RuntimeError("MySQL TLS config/helper/plugin is unavailable")
        self._config_has_tls()
        self._sql("CREATE TABLE %s (shop_id BIGINT NOT NULL PRIMARY KEY, "
                  "shop_name VARCHAR(128) NOT NULL)" % mysql_identifier(self.table))
        self._sql("INSERT INTO %s VALUES (7, 'shop-seven')" % mysql_identifier(self.table))
        return self

    def cleanup(self):
        try:
            self._sql("DROP TABLE IF EXISTS %s" % mysql_identifier(self.table))
        except Exception as error:  # preserve the test failure but report cleanup hazard
            print("WARNING: MySQL fixture cleanup failed: %s" % error, flush=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, _value, _trace):
        self.cleanup()
