# UniversalData NuGet MySQL demo

This standalone .NET 8 console project references `UniversalData` 7.0.0 as a NuGet package. It uses the MariaDB connector included in the runtime package. With no arguments it runs two read-only queries against a local MySQL-compatible server.

- `SELECT 1` checks the connection.
- `SELECT VERSION()` prints the database server version.

The `--etl` mode copies transformed rows between two tables in a dedicated test database.

## Runtime requirements

The NuGet runtime package contains the self-contained UniversalData Worker, the native SDK, the database connector, and the Visual C++ runtime DLLs used by this build. NuGet copies them into the application output, and the SDK finds them automatically. Use the .NET 8 SDK to build and run the demo against a local MySQL-compatible server. No C or C++ compiler, CMake, native SDK installation, `PATH` change, `PluginPath`, or `WorkerPath` setting is needed.

`ConnectionConfig.ConnectionString` accepts `Server`, `Port`, `Database`, `User ID`, and `Password`. The connector uses `utf8mb4`, which can also be specified as `Charset=utf8mb4`. The example uses `SslMode=None` for the local MariaDB service. This option is limited to `127.0.0.1` and `::1`. For verified TLS, omit `SslMode` and supply the server's CA with `SslCa`.

## Run with the local Windows demo feed

The Windows-only feed built for this local demo contains these two packages:

```text
UniversalData.7.0.0.nupkg
UniversalData.Runtime.win-x64.7.0.0.nupkg
```

The SDK package in this feed depends on the Windows runtime. When building the feed from source, pass `--single-rid-feed` to `packaging/pack_sdk.py` with the built native inputs. Use a separate local feed and package cache when working with this build, since it shares its package ID and version with the cross-platform release.

For this Windows build, the staging directory passed to `--dependencies` contains the database and native dependency DLLs plus `msvcp140.dll`, `vcruntime140.dll`, and `vcruntime140_1.dll` from the matching Visual Studio x64 redistributable directory. This staging step is only needed by the package producer.

Run these commands from PowerShell. If your installed SDK differs from the version pinned in the repository's `global.json`, run from outside the repository using the absolute project path shown below. Replace the placeholder database settings with your local connection string.

```powershell
$project = 'D:\Code\UniversalData\examples\dotnet-nuget-mysql\UniversalData.MySqlDemo.csproj'
$packageSource = 'D:\Code\UniversalData\artifacts\nuget-demo\windows-x64'
$packageCache = Join-Path ([IO.Path]::GetTempPath()) "UniversalDataDemoPackages-$([guid]::NewGuid().ToString('N'))"
$env:UD_MYSQL_CONNECTION_STRING = 'Server=127.0.0.1;Port=3306;Database=demo;User ID=demo_user;Password=change-me;Charset=utf8mb4;SslMode=None;'

dotnet restore $project --source $packageSource --packages $packageCache
dotnet build $project --configuration Release --no-restore
dotnet run --project $project --configuration Release --no-build --no-restore
```

The connection check reads `UD_MYSQL_CONNECTION_STRING` and prints the query results. Both modes keep credentials out of console output.

## Run the ETL with three Workers

Ask the local database administrator to create a dedicated schema with `CREATE DATABASE ud_nuget_demo CHARACTER SET utf8mb4`. Use a test account with `CREATE`, `SELECT`, and `INSERT` permissions in this schema. Use the package feed built from the same source revision as the demo, which exposes the SQL session's Worker process ID.

```powershell
$env:UD_ETL_CONNECTION_STRING = 'Server=127.0.0.1;Port=3306;Database=ud_nuget_demo;User ID=demo_user;Password=change-me;Charset=utf8mb4;SslMode=None;'
dotnet run --project $project --configuration Release --no-build --no-restore -- --etl
```

The ETL follows a pipeline and the producer-consumer pattern:

1. Prepare 1,003 source rows with a new `RunId`.
2. Open two source sessions in independent Worker processes. One reads IDs 1–500 and the other reads 501–1,003. Each trims the name and adds 1.25 to the amount.
3. Pass batches of at most 128 rows through a queue that holds up to two batches. A third Worker writes them with `Fastest<T>.BulkCopyAsync` in a single target transaction.
4. Compare every target row with the expected values before committing and read the result again after commit. The output gives the run ID, both reader PIDs and counts, and the writer PID and committed count.

The database tables are `ud_demo_etl_source` and `ud_demo_etl_target`. Each successful run adds its own rows. To inspect the output, replace the placeholder with the printed ID:

```sql
SELECT COUNT(*) AS rows_copied, MIN(Id), MAX(Id), SUM(Amount)
FROM ud_demo_etl_target
WHERE RunId = 'printed-run-id';
```

Expected results are 1,003 rows, IDs 1 through 1,003, and a sum of 6,288.81. The local demo uses three SDK Worker processes. The separately deployed Management service and its remote nodes have their own acceptance example.
