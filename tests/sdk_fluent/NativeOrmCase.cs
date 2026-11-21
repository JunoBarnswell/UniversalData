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
            ?? throw new InvalidDataException("Expected the actual database configuration");
        var root = Directory.CreateTempSubdirectory("ud-native-orm-");
        try
        {
            var sqlite = Path.Combine(root.CreateSubdirectory("files").FullName, "orm.db");
            external[DataSourceKind.Sqlite] = new("sqlite", DataSourceKind.Sqlite,
                "ManagedProviderMustNotOpen=fixture", sqlite);
            if (Providers.Any(provider => !external.TryGetValue(provider, out var source) ||
                string.IsNullOrWhiteSpace(source.NativeConnectionString)))
                throw new InvalidDataException("Native SQL coverage requires all six installed connectors");

            await using var client = BuildClient(root.FullName, external.Values, pluginRoots, sqlite);
            var failures = new List<Exception>();
            foreach (var kind in Providers)
            {
                var source = external[kind];
                var table = "udorm_" + Guid.NewGuid().ToString("N")[..18];
                if (kind == DataSourceKind.Oracle) table = table.ToUpperInvariant();
                try
                {
                    await Exercise(client, source, table, deadline.Token);
                    await LinqTableCase.RunAsync(client, source,
                        table + (kind == DataSourceKind.Oracle ? "P" : "p"), deadline.Token);
                    Console.WriteLine($"{kind} Native SQL job/query, transaction, and independent oracle passed");
                }
                catch (Exception error) when (!deadline.IsCancellationRequested)
                { failures.Add(new InvalidOperationException(kind.ToString(), error)); }
                finally
                {
                    try { await client.RunAsync(SqlJob(source.Name, "drop", $"DROP TABLE {table}"), deadline.Token); }
                    catch (Exception error) when (!deadline.IsCancellationRequested)
                    { failures.Add(new InvalidOperationException($"{kind} fixture cleanup", error)); }
                }
            }
            if (failures.Count != 0) throw new AggregateException("Native SQL provider coverage failed", failures);
        }
        finally { root.Delete(recursive: true); }
    }

    private static async Task Exercise(DataClient client, AtomicSource source, string table,
        CancellationToken cancellation)
    {
        if (source.Kind == DataSourceKind.Oracle)
        {
            await ExerciseRows<OracleOrmRow>(client, source, table,
                row => (decimal.ToInt64(row.Owner), decimal.ToInt64(row.Id), row.Label), cancellation);
            return;
        }
        await ExerciseRows<LocalOrmRow>(client, source, table,
            row => (row.Owner, row.Id, row.Label), cancellation);
    }

    private static async Task ExerciseRows<TRow>(DataClient client, AtomicSource source, string table,
        Func<TRow, (long Owner, long Id, string? Label)> projection, CancellationToken cancellation)
    {
        await client.RunAsync(SqlJob(source.Name, "create", CreateTable(source.Kind, table)), cancellation);

        var bulk = DataJob.Define<object>($"native-bulk-{Guid.NewGuid():N}")
            .FromSql<TRow>(source.Name, BulkRows(source.Kind), stream: true)
            .ToTable(source.Name, table, durable: false).Append();
        await client.RunAsync(bulk, cancellation);

        await client.RunAsync(SqlJob(source.Name, "update", $"UPDATE {table} SET label = 'after' " +
            "WHERE owner_id = 2 AND entry_id = 1"), cancellation);
        var parameter = source.Kind switch
        {
            DataSourceKind.PostgreSql => "$1",
            DataSourceKind.Oracle => ":owner",
            DataSourceKind.Sqlite => "?1",
            _ => "?"
        };
        var ownerRows = await Read(client, DataJob.Define<object>($"native-owner-{Guid.NewGuid():N}")
            .FromSql<TRow>(source.Name, $"SELECT owner_id, entry_id, label FROM {table} " +
                $"WHERE owner_id = {parameter} ORDER BY entry_id", OwnerParameter(), stream: true).Return(), cancellation);
        Require(ownerRows.Count == 2 && projection(ownerRows[0]).Label == "after" && projection(ownerRows[1]).Label is null,
            "Native named SQL parameters did not preserve compound-key rows and NULL");

        var deleteResult = await client.RunAsync(SqlJob(source.Name, "delete",
            $"DELETE FROM {table} WHERE owner_id = 9"), cancellation);
        Require(deleteResult.Metrics.ErrorCount == 0,
            "Native SQL delete did not complete without a provider error");

        var commit = DataJob.Define<object>($"native-commit-{Guid.NewGuid():N}").Transaction(source.Name)
            .ExecuteSql(source.Name, $"INSERT INTO {table} (owner_id, entry_id, label) VALUES (3, 1, 'commit')");
        await client.RunAsync(commit, cancellation);

        var rollback = DataJob.Define<object>($"native-rollback-{Guid.NewGuid():N}").Transaction(source.Name)
            .ExecuteSql(source.Name, $"INSERT INTO {table} (owner_id, entry_id, label) VALUES (4, 1, 'rollback')")
            .ExecuteSql(source.Name, $"INSERT INTO {table} (owner_id, entry_id, label) VALUES (1, 1, 'duplicate')");
        try
        {
            await client.RunAsync(rollback, cancellation);
            throw new InvalidOperationException("A failed Native provider transaction returned normally");
        }
        catch (UniversalDataException error) when (error.Code is not (StatusCode.CommitUnknown or StatusCode.WorkerLost)) { }

        var rows = await Read(client, Query<TRow>(source.Name, table, "native-readback"), cancellation);
        (long Owner, long Id, string? Label)[] expected =
            [(1, 1, "original"), (2, 1, "after"), (2, 2, null), (3, 1, "commit")];
        Require(rows.Select(projection).SequenceEqual(expected),
            "Independent Native SQL readback preserved neither the committed rows nor rollback boundary");

        var identity = await ReadIdentity(client, source, cancellation);
        Require(identity.Length != 0, "Native connector identity SQL returned no physical identity");
    }

    private static DataClient BuildClient(string directory, IEnumerable<AtomicSource> sources,
        string[] pluginRoots, string sqlite)
    {
        var module = typeof(NativeOrmCase).Assembly.Location;
        string[] plugins = [Path.GetDirectoryName(module)!, .. pluginRoots];
        var definitions = sources.ToArray();
        var resources = definitions.ToDictionary(source => source.Name,
            _ => new[] { "Open", "Discover", "Read", "Write", "Delete", "Receipt" }, StringComparer.Ordinal);
        var tenant = new { fileRoots = new[] { Path.GetDirectoryName(sqlite)! },
            networkOrigins = Array.Empty<string>(), pluginRoots = plugins,
            credentialReferences = Array.Empty<string>(), resources };
        var policy = Path.Combine(directory, "policy.json");
        File.WriteAllText(policy, JsonSerializer.Serialize(new
        {
            dataDirectory = Path.Combine(directory, "state"), tenant.fileRoots, tenant.networkOrigins,
            tenant.pluginRoots, credentialFiles = new Dictionary<string, string>(), maxConcurrency = 8,
            memoryLimit = 2_147_483_648UL, tenants = new { fluent = tenant }
        }));
        var builder = DataClient.CreateBuilder(policy, "fluent");
        foreach (var source in definitions)
            builder.AddSource(new(source.Name, source.Kind, source.ConnectionString,
                resource: source.Kind == DataSourceKind.Sqlite ? sqlite : source.Name,
                nativeConnectionString: source.NativeConnectionString));
        return builder.Build();
    }

    private static DataJob<object> SqlJob(string source, string action, string statement) =>
        DataJob.Define<object>($"native-{action}-{Guid.NewGuid():N}").ExecuteSql(source, statement);

    private static DataJobQuery<object, TRow> Query<TRow>(string source, string table, string id) =>
        DataJob.Define<object>(id).FromSql<TRow>(source,
            $"SELECT owner_id, entry_id, label FROM {table} ORDER BY owner_id, entry_id", stream: true).Return();

    private static async Task<List<TRow>> Read<TRow>(DataClient client,
        DataJobQuery<object, TRow> query, CancellationToken cancellation)
    {
        var rows = new List<TRow>();
        await foreach (var row in client.RunAsync(query, cancellation)) rows.Add(row);
        return rows;
    }

    private static async Task<string> ReadIdentity(DataClient client, AtomicSource source,
        CancellationToken cancellation)
    {
        var query = DataJob.Define<object>($"native-identity-{Guid.NewGuid():N}")
            .FromSql<NativeIdentityRow>(source.Name, IdentitySql(source.Kind), stream: true).Return();
        await foreach (var row in client.RunAsync(query, cancellation)) return row.PhysicalId;
        throw new InvalidOperationException("Native connector identity SQL returned no row");
    }

    private static SqlParameterBatch OwnerParameter()
    {
        var json = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var schema = JsonSerializer.SerializeToElement(new
        {
            version = ProductInfo.Protocol,
            fields = new[] { new { name = "owner", type = "int64", nullable = false } }
        }, json);
        var row = JsonSerializer.SerializeToElement(new { owner = 2L }, json);
        return new(schema, [row]);
    }

    private static string CreateTable(DataSourceKind kind, string table)
    {
        string number = kind == DataSourceKind.Oracle ? "NUMBER(18)" : "BIGINT";
        string text = kind switch
        {
            DataSourceKind.SqlServer => "NVARCHAR(100)",
            DataSourceKind.Oracle => "VARCHAR2(100)",
            _ => "VARCHAR(100)"
        };
        return $"CREATE TABLE {table} (owner_id {number} NOT NULL, entry_id {number} NOT NULL, " +
            $"label {text}, PRIMARY KEY (owner_id, entry_id))";
    }

    private static string BulkRows(DataSourceKind kind)
    {
        const string rows = "SELECT 1 AS owner_id, 1 AS entry_id, 'original' AS label " +
            "UNION ALL SELECT 2, 1, 'before' UNION ALL SELECT 2, 2, NULL " +
            "UNION ALL SELECT 9, 1, 'delete'";
        return kind == DataSourceKind.Oracle
            ? "SELECT 1 AS owner_id, 1 AS entry_id, 'original' AS label FROM DUAL UNION ALL " +
              "SELECT 2, 1, 'before' FROM DUAL UNION ALL SELECT 2, 2, NULL FROM DUAL UNION ALL " +
              "SELECT 9, 1, 'delete' FROM DUAL"
            : rows;
    }

    private static string IdentitySql(DataSourceKind kind) => kind switch
    {
        DataSourceKind.PostgreSql => "SELECT CAST(pg_backend_pid() AS VARCHAR(100)) AS physical_id",
        DataSourceKind.MySql or DataSourceKind.MariaDb => "SELECT CAST(CONNECTION_ID() AS CHAR) AS physical_id",
        DataSourceKind.SqlServer => "SELECT CAST(@@SPID AS NVARCHAR(100)) AS physical_id",
        DataSourceKind.Oracle => "SELECT SYS_CONTEXT('USERENV','SID') AS \"physical_id\" FROM DUAL",
        DataSourceKind.Sqlite => "SELECT sqlite_version() AS physical_id",
        _ => throw new ArgumentException("Expected database provider")
    };

    private static void Require(bool success, string message)
    {
        if (!success) throw new InvalidOperationException(message);
    }
}
