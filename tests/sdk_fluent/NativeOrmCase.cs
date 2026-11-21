using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

internal static class NativeOrmCase
{
    private static readonly DataSourceKind[] Providers = [DataSourceKind.PostgreSql, DataSourceKind.MySql,
        DataSourceKind.MariaDb, DataSourceKind.SqlServer, DataSourceKind.Oracle, DataSourceKind.Sqlite];

    internal static async Task RunAsync(string fixture, string[] pluginRoots)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var json = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        json.Converters.Add(new JsonStringEnumConverter());
        var external = JsonSerializer.Deserialize<AtomicSource[][]>(File.ReadAllText(fixture), json)
            ?.SelectMany(group => group).GroupBy(source => source.Kind).ToDictionary(group => group.Key, group => group.First())
            ?? throw new InvalidDataException("Expected the actual XA database configuration");
        var root = Directory.CreateTempSubdirectory("ud-native-orm-");
        try
        {
            var sqlite = Path.Combine(root.CreateSubdirectory("files").FullName, "orm.db");
            external[DataSourceKind.Sqlite] = new("sqlite", DataSourceKind.Sqlite, "", sqlite);
            if (Providers.Any(provider => !external.TryGetValue(provider, out var source) ||
                string.IsNullOrWhiteSpace(source.NativeConnectionString)))
                throw new InvalidDataException("Ordinary ORM coverage requires all six Native connectors");
            var module = typeof(NativeOrmJobs).Assembly.Location;
            string[] plugins = [Path.GetDirectoryName(module)!, .. pluginRoots];
            var resources = external.Values.ToDictionary(source => source.Name,
                _ => new[] { "Open", "Discover", "Read", "Write", "Delete" }, StringComparer.Ordinal);
            var tenant = new { fileRoots = new[] { Path.GetDirectoryName(sqlite)! }, networkOrigins = Array.Empty<string>(),
                pluginRoots = plugins, credentialReferences = Array.Empty<string>(), resources };
            var policy = Path.Combine(root.FullName, "policy.json");
            File.WriteAllText(policy, JsonSerializer.Serialize(new { dataDirectory = Path.Combine(root.FullName, "state"),
                tenant.fileRoots, tenant.networkOrigins, tenant.pluginRoots, credentialFiles = new Dictionary<string, string>(),
                maxConcurrency = 8, memoryLimit = 2_147_483_648UL,
                trustedAssemblyHashes = new[] { Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(module))).ToLowerInvariant() },
                tenants = new { fluent = tenant } }));
            var builder = DataClient.CreateBuilder(policy, "fluent").AddModule(typeof(NativeOrmJobs).Assembly);
            foreach (var source in external.Values)
                builder.AddSource(new(source.Name, source.Kind, "ManagedProviderMustNotOpen=fixture",
                    resource: source.Kind == DataSourceKind.Sqlite ? sqlite : source.Name,
                    nativeConnectionString: source.NativeConnectionString));
            await using var sdk = builder.Build();
            var failures = new List<Exception>();
            foreach (var kind in Providers)
            {
                var source = external[kind];
                var table = "udorm_" + Guid.NewGuid().ToString("N")[..18];
                if (kind == DataSourceKind.Oracle) table = table.ToUpperInvariant();
                var work = new NativeOrmWork(source.Name, kind, table);
                try
                {
                    var written = await sdk.RunAsync(NativeOrmJobs.Exercise, work, deadline.Token);
                    var independent = await sdk.RunAsync(NativeOrmJobs.Readback, work, deadline.Token);
                    if (written.ProcessId == independent.ProcessId || independent.Rows != 4)
                        throw new InvalidOperationException("The ordinary writes must be independently read in another Worker");
                    Console.WriteLine($"{kind} Native ordinary ORM passed via database identity {written.PhysicalId}; " +
                        $"Worker {written.ProcessId} wrote and Worker {independent.ProcessId} independently verified four rows.");
                }
                catch (Exception error) when (!deadline.IsCancellationRequested)
                { failures.Add(new InvalidOperationException(kind.ToString(), error)); }
                finally
                {
                    try { await sdk.RunAsync(NativeOrmJobs.Remove, work, deadline.Token); }
                    catch (Exception error) when (!deadline.IsCancellationRequested)
                    { failures.Add(new InvalidOperationException($"{kind} fixture cleanup", error)); }
                }
            }
            if (failures.Count != 0) throw new AggregateException("Ordinary Native ORM provider coverage failed", failures);
        }
        finally { root.Delete(recursive: true); }
    }
}

public sealed record NativeOrmWork(string Name, DataSourceKind Kind, string Table);
public sealed record NativeOrmProof(int ProcessId, string PhysicalId, int Rows);

public static class NativeOrmJobs
{
    public static async Task<NativeOrmProof> Exercise(DataContext context, NativeOrmWork work, CancellationToken token)
    {
        var database = context.Database(work.Name);
        var identity = await database.UseAsync(async (db, cancellation) =>
        {
            Reject(() => _ = db.Ado.Connection);
            Reject(() => _ = db.Ado.GetCommand(IdentitySql(work.Kind), []));
            string number = work.Kind == DataSourceKind.Oracle ? "NUMBER(18)" : "BIGINT";
            string text = work.Kind == DataSourceKind.SqlServer ? "NVARCHAR(100)" : "VARCHAR(100)";
            await db.Ado.ExecuteCommandAsync($"CREATE TABLE {work.Table} (owner_id {number} NOT NULL, " +
                $"entry_id {number} NOT NULL, label {text}, PRIMARY KEY (owner_id,entry_id))");
            if (work.Kind == DataSourceKind.Sqlite)
            {
                var rows = new[] { Row(1, 1, "原始"), Row(9, 2, "one ' quote; two") };
                Require(await db.Insertable(rows).AS(work.Table).ExecuteCommandAsync() == rows.Length,
                    "SQLite typed list insert must return its affected row count");
                Require(await db.Queryable<LocalOrmRow>().AS(work.Table).Where(row => row.Owner == 9)
                    .Select(row => row.Label).SingleAsync() == rows[1].Label,
                    "SQLite typed list insert must preserve a quote and statement separator as data");
            }
            else
                Require(await db.Insertable(Row(1, 1, "原始")).AS(work.Table).ExecuteCommandAsync() == 1,
                    "Ordinary typed insert count");
            var fastest = db.Fastest<LocalOrmRow>().AS(work.Table).BatchSize(2);
            Require(await fastest.BulkCopyAsync([Row(2, 1, "before"), Row(2, 2, null), Row(9, 1, "delete")]) == 3,
                "Ordinary Native bulk insert count");
            Require(await fastest.BulkUpdateAsync([Row(2, 1, "after")]) == 1, "Ordinary composite-key bulk update count");
            Require(await db.Updateable<LocalOrmRow>().AS(work.Table).SetColumns(row => row.Label == "更新")
                .Where(row => row.Owner == 1 && row.Id == 1).ExecuteCommandAsync() == 1, "Ordinary typed update count");
            int deletes = work.Kind == DataSourceKind.Sqlite ? 2 : 1;
            Require(await db.Deleteable<LocalOrmRow>().AS(work.Table).Where(row => row.Owner == 9).ExecuteCommandAsync() == deletes,
                "Ordinary typed delete count");
            string parameter = work.Kind == DataSourceKind.Oracle ? ":owner" : "@owner";
            Require(await db.Ado.GetIntAsync($"SELECT COUNT(*) FROM {work.Table} WHERE owner_id = {parameter}",
                new SugarParameter(parameter, 2L)) == 2, "Native named parameters must filter the correct rows");
            var page = await db.Queryable<LocalOrmRow>().AS(work.Table).Where(row => row.Owner == 2)
                .OrderBy(row => row.Id).Select(row => new { row.Id, row.Label }).ToPageListAsync(1, 2);
            Require(page.Count == 2 && page[0].Id == 1 && page[0].Label == "after" && page[1].Label is null,
                "Native ordinary projection and pagination must preserve nullable values and order");
            return (await db.Ado.GetScalarAsync(IdentitySql(work.Kind))).ToString()
                ?? throw new InvalidOperationException("Database identity query returned no value");
        }, token);

        await database.TransactionAsync(async (tx, ct) =>
        {
            await tx.Database(work.Name).UseAsync(db => db.Insertable(Row(3, 1, "commit")).AS(work.Table).ExecuteCommandAsync(), ct);
            var count = await DataContext.Current.Database(work.Name).UseAsync(db => db.Queryable<LocalOrmRow>().AS(work.Table)
                .Where(row => row.Owner == 3).CountAsync(), ct);
            Require(count == 1, "The Native local branch must see its own uncommitted write");
        }, token);
        try
        {
            await database.TransactionAsync(async (tx, ct) =>
            {
                await tx.Database(work.Name).UseAsync(db => db.Insertable(Row(4, 1, "rollback")).AS(work.Table).ExecuteCommandAsync(), ct);
                throw new InvalidOperationException("abort native local fixture");
            }, token);
            throw new InvalidOperationException("Failed local transaction returned success");
        }
        catch (InvalidOperationException error) when (error.Message == "abort native local fixture") { }
        return new(Environment.ProcessId, identity, 4);
    }

    public static Task<NativeOrmProof> Readback(DataContext context, NativeOrmWork work, CancellationToken token) =>
        context.Database(work.Name).UseAsync(async (db, _) =>
        {
            var rows = await db.Queryable<LocalOrmRow>().AS(work.Table).OrderBy(row => new { row.Owner, row.Id }).ToListAsync();
            (long Owner, long Id, string? Label)[] wanted = [(1, 1, "更新"), (2, 1, "after"), (2, 2, null), (3, 1, "commit")];
            Require(rows.Select(row => (row.Owner, row.Id, row.Label)).SequenceEqual(wanted),
                "Independent readback must preserve every Native ORM write and discard aborted rows");
            return new NativeOrmProof(Environment.ProcessId, "", rows.Count);
        }, token);

    public static Task Remove(DataContext context, NativeOrmWork work, CancellationToken token) =>
        context.Database(work.Name).UseAsync(db => db.Ado.ExecuteCommandAsync($"DROP TABLE {work.Table}"), token);

    private static LocalOrmRow Row(long owner, long id, string? label) => new() { Owner = owner, Id = id, Label = label };

    private static string IdentitySql(DataSourceKind kind) => kind switch
    {
        DataSourceKind.PostgreSql => "SELECT pg_backend_pid()",
        DataSourceKind.MySql or DataSourceKind.MariaDb => "SELECT CONNECTION_ID()",
        DataSourceKind.SqlServer => "SELECT @@SPID",
        DataSourceKind.Oracle => "SELECT SYS_CONTEXT('USERENV','SID') FROM DUAL",
        DataSourceKind.Sqlite => "SELECT sqlite_version()",
        _ => throw new ArgumentException("Expected database provider")
    };

    private static void Reject(Action call)
    {
        try { call(); }
        catch (UniversalDataException error) when (error.Code == StatusCode.Unsupported) { return; }
        throw new InvalidOperationException("The ORM allowed access to a physical provider object");
    }

    private static void Require(bool success, string message)
    {
        if (!success) throw new InvalidOperationException(message);
    }
}
