# Managed SQL acceptance runner

`ManagedSql.csproj` is a real-driver console acceptance test. It does not create an in-memory substitute or use a fake connector. The runner executes Native parameter binding, typed query batches, stable pagination, raw `IDataReader`, insert/update/delete, a rollback transaction, cancellation, and bounded streaming batches.

Connection secrets are read from a local file and are never printed:

```powershell
dotnet run --project tests/managed_sql/ManagedSql.csproj -- --provider Sqlite --connection-file .artifacts/sqlite.connection --plugin D:/build/plugins/ud_connector_sqlite.dll --rows 1000000 --batch 512 --benchmark .artifacts/sqlite-managed-sql.json
```

An isolated remote fixture is a JSON file kept outside source control. Relative `connectionFile`, `plugin`, and `setupSql` paths are resolved relative to the fixture:

```json
{
  "provider": "MariaDb",
  "connectionFile": "mariadb.connection",
  "plugin": "../../build/plugins/ud_connector_mariadb.dll",
  "setupSql": "managed_sql.setup.sql"
}
```

Run it with `--fixture path/to/mariadb.fixture.json`. The acceptance entity is deliberately mapped to the fixed `managed_sql_orders` table, so the fixture cannot silently create a different table from the ORM mapping. Command-line values override fixture values; `--connection-file` takes precedence over `--connection`. `--benchmark` writes first-batch latency, CPU time, managed allocations, peak working-set, throughput inputs, and a result signature. The benchmark inserts rows in bounded batches inside one transaction, so a million-row run never materializes the whole source list in managed memory.

## 千万行流式验收

`--stream-copy` 使用两个 SDK 会话，生成输入表并运行强类型读取、Unicode 字符串转换、十进制运算和原生批量写入。计时从打开读取游标开始，到目标事务提交结束。随后遍历目标全部记录，核对业务值、行数和校验值。

```bash
cmake --build build/product --config Release --target ud_managed_sql_test
LD_LIBRARY_PATH="$PWD/build/product${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
dotnet build/product/tests/managed-sql/ManagedSql.dll --stream-copy \
  --provider postgresql --connection-file "$UD_TEST_POSTGRESQL_CONFIG" \
  --plugin "$PWD/build/product/plugins/ud_connector_postgresql.so" \
  --rows 10000000 --batch 4096 --benchmark postgresql-stream-10m.json
```

在 SQLite 上使用 `--provider sqlite` 和 SQLite 插件，并通过 `--connection-file`、`--target-connection-file` 分别提供两个独立文件的连接配置。两个模式均在隔离的验收库中创建并清理各自的测试表。
