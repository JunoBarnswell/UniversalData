# UniversalData NuGet MySQL demo

This standalone .NET 8 console project references `UniversalData` 7.0.0 as a NuGet package. It runs two read-only queries against a local MySQL-compatible server. The development machine used for this demo runs MariaDB 12.3, so the example selects `DbType.MariaDb` and loads the MariaDB connector included in the runtime package:

- `SELECT 1` checks the connection.
- `SELECT VERSION()` prints the database server version.

The demo does not create tables or change database data.

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
$packageCache = Join-Path ([IO.Path]::GetTempPath()) 'UniversalDataDemoPackages'
$env:UD_MYSQL_CONNECTION_STRING = 'Server=127.0.0.1;Port=3306;Database=demo;User ID=demo_user;Password=change-me;Charset=utf8mb4;SslMode=None;'

dotnet restore $project --source $packageSource --packages $packageCache
dotnet build $project --configuration Release --no-restore
dotnet run --project $project --configuration Release --no-build --no-restore
```

The program reads only `UD_MYSQL_CONNECTION_STRING` and never prints it. The successful output shows the result of `SELECT 1` and the server version.
