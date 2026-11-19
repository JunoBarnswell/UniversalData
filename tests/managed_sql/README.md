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
