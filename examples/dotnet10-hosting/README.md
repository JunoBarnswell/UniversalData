# .NET 10 hosting, SQLite, and PostgreSQL streaming standbys

This example is one application with two entry points:

* ASP.NET Core endpoints register the SDK with
  `builder.Services.AddUniversalData(builder.Configuration.GetRequiredSection("UniversalData"), builder.Environment.ContentRootPath)`.
* The Generic Host command registers the same singleton with
  `builder.Services.AddUniversalData(configPath)`.

Both paths resolve the same `OrdersService` through dependency injection. The service authors a
real `QueryTable<OrderRecord>` job, applies the database-side filter, integer-key ordering and
offset/page-size, then consumes the Native result with .NET 10 `ToListAsync`. Writes are executed
by the installed UniversalData Worker and are read back through the selected named source before
the operation is considered successful. Both databases store amounts as integer cents and the host
exposes them as decimal totals.

## Package and worker

The project imports its .NET 10 target framework and exact SDK version from
`metadata/ProductVersion.props`. The runtime package supplies the Native Worker and connectors.

The current metadata version is the package version used by the sample. The generated local host
policy grants the output directory as a plugin root so the matching Worker and SQLite/PostgreSQL
connector selected from the installed package can be authorized.

## SQLite / IoC quick start

From this directory, with the .NET 10 SDK and the UniversalData NuGet feed available:

```bash
dotnet restore --source /absolute/path/to/nuget-feed
dotnet build --no-restore
bash scripts/create-fixtures.sh sqlite
dotnet run --no-build -- --command init
dotnet run --no-build -- --command write --id 1 --shop-id 7 --sequence 100 --total 12.50
dotnet run --no-build -- --command write --id 2 --shop-id 7 --sequence 101 --total 18.75
dotnet run --no-build -- --command list --shop-id 7 --offset 0 --page-size 20
dotnet run --no-build
```

The last command starts the HTTP host. The equivalent endpoint calls are:

```bash
curl 'http://127.0.0.1:5088/orders?shopId=7&offset=0&pageSize=20'
curl -X POST 'http://127.0.0.1:5088/orders' \
  -H 'content-type: application/json' \
  -d '{"id":3,"shopId":7,"sequence":102,"total":21.00}'
```

The fixture stores its data and credentials in `.local/sqlite`. Remove that directory to clean up the fixture.

PowerShell users can run `.\scripts\create-fixtures.ps1 -Mode sqlite` and use the same `dotnet`
commands. Set `UD_APP_BASE` when the output directory is not `bin/Debug/net10.0`.

## PostgreSQL 17 primary and two independent streaming standbys

The Compose file starts exactly three PostgreSQL 17 services:

* `orders-primary` is the only write target and exposes host port 55432 by default.
* `orders-replica-1` and `orders-replica-2` each have their own named Docker volume and are
  initialized independently with `pg_basebackup` from the primary. They expose 55433 and 55434.

`scripts/postgres.sh prepare` creates private random passwords, a private host policy, five named
source credentials (including a writer probe for each standby), `pg_hba.conf`, and the primary init SQL under `.local/postgres`. The SQL uses
separate `orders_writer` and `orders_reader` roles. The reader role has `SELECT` only. The
`orders-replica-1-write` probe supplies a valid writer credential and checks that PostgreSQL
rejects writes while in hot standby.

```bash
dotnet restore --source /absolute/path/to/nuget-feed
dotnet build --no-restore
bash scripts/postgres.sh prepare
bash scripts/postgres.sh up
bash scripts/postgres.sh verify

UD_UNIVERSALDATA_CONFIG="$PWD/.local/postgres/universaldata.yaml" \
Orders__DefaultSource=orders-primary \
dotnet run --no-build -- --command list --shop-id 1 --offset 0 --page-size 20
```

`verify` performs the complete repeatable proof:

1. Writes a business key through the Generic Host and confirms immediate primary read-after-write.
2. Captures `pg_current_wal_lsn()` and waits for each standby's independent
   `pg_last_wal_replay_lsn()` to reach that exact position.
3. Reads the business key independently from both standby containers and checks
   `pg_is_in_recovery()` on all three instances.
4. Pauses `orders-replica-1`, writes a second key to the primary, confirms the primary result while
   paused, resumes the standby, and waits for its own replay position.
5. Sends an `INSERT` to `orders-replica-1` with the writer credential, verifies the rejection,
   and checks that the existing business row keeps its original values.

Pause and resume are also available independently:

```bash
bash scripts/postgres.sh pause orders-replica-1
bash scripts/postgres.sh resume orders-replica-1
```

Run the PostgreSQL fixture on Linux or WSL2 with Docker Compose.

`stop` shuts down the three instances. `clean` removes the volumes for this Compose project
and the generated configuration in `.local/postgres`.
