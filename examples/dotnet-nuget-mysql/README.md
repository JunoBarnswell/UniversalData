# UniversalData NuGet MariaDB example

The .NET 8 console project consumes the `UniversalData` 8.0 NuGet package. Its read-only mode uses the official SqlSugarCore API in a leased Worker to run `SELECT 1` and `SELECT VERSION()`. The `--etl` mode compiles a typed `DataClient.Etl` chain and runs it in the same Native Package engine as other ETL workloads.

## Build and configure

Restore and build with the published 8.0 packages, or use the matching local feed and NuGet.org for the SDK's SqlSugarCore dependency. The RID-specific package supplies the Worker, Native engine and database connector.

```sh
dotnet restore ./UniversalData.MySqlDemo.csproj
dotnet build ./UniversalData.MySqlDemo.csproj --configuration Release --no-restore
```

Set `UD_POLICY_FILE` to the absolute path of the host policy. Substitute the demo output directory and a separate writable state directory in this policy. Compute the lowercase SHA-256 of `UniversalData.MySqlDemo.dll` after building the demo, and update the digest when the assembly changes. On Windows, use drive-qualified paths and escape backslashes in JSON.

```json
{
  "dataDirectory": "/absolute/path/to/demo-state",
  "fileRoots": [],
  "networkOrigins": [],
  "pluginRoots": ["/absolute/path/to/bin/Release/net8.0"],
  "credentialFiles": {},
  "maxConcurrency": 2,
  "resources": {
    "demo": ["Open", "Discover", "Read", "Write", "Receipt"]
  },
  "trustedAssemblyHashes": ["lowercase-sha256-of-UniversalData.MySqlDemo.dll"]
}
```

Set `UD_MYSQL_CONNECTION_STRING` to the connection for the read-only mode. An example localhost connection string is `Server=127.0.0.1;Port=3306;Database=demo;User ID=demo_user;Password=change-me;Charset=utf8mb4;SslMode=None;`.

```sh
dotnet run --project ./UniversalData.MySqlDemo.csproj --configuration Release --no-build --no-restore
```

## Run the typed ETL

Create the dedicated database using `CREATE DATABASE ud_nuget_demo CHARACTER SET utf8mb4`. The example account needs `CREATE`, `SELECT`, `INSERT`, and `UPDATE` permissions in that database, including access to the Native receipt table. Set `UD_ETL_CONNECTION_STRING` to the connection for this database.

```sh
dotnet run --project ./UniversalData.MySqlDemo.csproj --configuration Release --no-build --no-restore -- --etl
```

The example creates uniquely named source and target tables and seeds 1,003 rows. A registered C# method trims the Unicode name and adds 1.25 to the exact decimal value as Native batches pass through the data flow. Preview samples ten rows and verifies that the target is empty. Execution upserts by `Id`, waits for the Native terminal metrics, then reads back each row through SqlSugar to verify the result. The console prints the Worker process ID, verified count, run ID, and target table name.
