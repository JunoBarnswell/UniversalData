using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

public sealed record AtomicSource(string Name, DataSourceKind Kind, string ConnectionString,
    string NativeConnectionString);

public sealed class AtomicRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "entry_id")]
    public long Id { get; set; }
}

/* Oracle NUMBER is exposed by the Native connector as a fixed decimal, even
 * when the declaration has scale zero. Keep the readback schema faithful to
 * that provider contract and convert only at the assertion boundary. */
public sealed class OracleAtomicRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "ENTRY_ID", Length = 18, DecimalDigits = 0)]
    public decimal Id { get; set; }
}

internal static class AtomicCase
{
    internal static async Task RunAsync(string fixturePath, string[] pluginRoots, string? decisionArms = null)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var cancellation = timeout.Token;
        var json = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        json.Converters.Add(new JsonStringEnumConverter());
        var groups = JsonSerializer.Deserialize<AtomicSource[][]>(File.ReadAllText(fixturePath), json)
            ?? throw new InvalidDataException("Expected arrays of actual database participants");
        if (decisionArms is not null)
            groups = groups.Where(group => group.Length == 2 && group.Any(source => source.Kind == DataSourceKind.PostgreSql) &&
                group.Any(source => source.Kind == DataSourceKind.SqlServer)).ToArray();
        if (groups.Length == 0) throw new InvalidDataException("No actual database participants configured");

        var failures = new List<Exception>();
        foreach (var group in groups)
        {
            if (group.Length is < 2 or > 3 || group.Select(source => source.Name).Distinct().Count() != group.Length ||
                group.Any(source => source.Kind is DataSourceKind.Http or DataSourceKind.Sqlite ||
                    string.IsNullOrWhiteSpace(source.ConnectionString) ||
                    string.IsNullOrWhiteSpace(source.NativeConnectionString)))
                throw new InvalidDataException("Each fixture must name two or three distinct XA sources with both provider settings");
            try { await RunGroup(group, pluginRoots, decisionArms, cancellation); }
            catch (Exception error) when (!cancellation.IsCancellationRequested)
            {
                var pair = string.Join(" / ", group.Select(source => source.Kind));
                var status = error is UniversalDataException sdk ? $"[{sdk.Code}/{sdk.Detail?.Cause}] " : "";
                Console.Error.WriteLine($"FAIL {pair} ({error.GetType().Name}): {status}{error.Message}");
                failures.Add(new InvalidOperationException(pair, error));
            }
        }
        if (failures.Count != 0) throw new AggregateException("One or more Native transaction fixture groups failed", failures);
    }

    private static async Task RunGroup(AtomicSource[] sources, string[] pluginRoots, string? decisionArms,
        CancellationToken cancellation)
    {
        var directory = Directory.CreateTempSubdirectory("ud-fluent-xa-");
        try
        {
            await using var data = BuildClient(directory.FullName, sources, pluginRoots);
            var participants = sources.Select(source => source.Name).ToArray();
            var table = "udxfa_" + Guid.NewGuid().ToString("N")[..20];
            var installed = new List<string>(participants.Length);
            string stage = "setup";
            Exception? failed = null;
            try
            {
                foreach (var source in sources)
                {
                    stage = $"setup {source.Kind}";
                    await data.RunAsync(SqlJob("create", source.Name,
                        $"CREATE TABLE {table} (entry_id {NumberType(source.Kind)} PRIMARY KEY)"), cancellation);
                    installed.Add(source.Name);
                }

                stage = "commit posting";
                var committedJob = TransactionJob("commit", participants, table, 31);
                var committed = await data.RunAsync(committedJob, cancellation);
                await RequireRows(data, sources, table, [31], cancellation);

                stage = "replay confirmed journal";
                await using (var replay = await data.ResumeAsync(committedJob, new object(),
                    new DataRunReference(committed.ExecutionId, JournalTicket: committed.JournalTicket), cancellation))
                    await replay.WaitAsync(cancellation);
                await RequireRows(data, sources, table, [31], cancellation);

                if (decisionArms is not null)
                {
                    stage = "recover after durable journal decision before participant commit";
                    await RecoverDecision(data, sources, table, participants, decisionArms, cancellation);
                    return;
                }

                stage = "rollback failing Native transaction";
                var rollbackJob = TransactionJob("rollback", participants, table, 32, duplicateId: 31);
                try
                {
                    await data.RunAsync(rollbackJob, cancellation);
                    throw new InvalidOperationException("A failed Native transaction returned normally");
                }
                catch (UniversalDataException error) when (error.Code is not (StatusCode.CommitUnknown or StatusCode.WorkerLost)) { }
                await RequireRows(data, sources, table, [31], cancellation);

                stage = "terminate Worker during an uncommitted Native transaction";
                var interruptedJob = TransactionJob("crash", participants, table, 33);
                await using (var run = await data.StartAsync(interruptedJob, new object(), cancellation))
                {
                    try
                    {
                        using var worker = Process.GetProcessById(run.WorkerProcessId);
                        worker.Kill(entireProcessTree: true);
                        await run.WaitAsync(cancellation);
                        throw new InvalidOperationException("An interrupted Native transaction returned normally");
                    }
                    catch (DataExecutionException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown)
                    {
                        await using var recovery = await data.ResumeAsync(interruptedJob, new object(), error.Reference, cancellation);
                        await recovery.WaitAsync(cancellation);
                    }
                }
                await RequireRows(data, sources, table, [31, 33], cancellation);
                Console.WriteLine("Native transaction commit, rollback, Worker loss, and journal recovery passed on " +
                    string.Join(" / ", sources.Select(source => source.Kind)));
            }
            catch (Exception error)
            {
                failed = error;
                Console.Error.WriteLine($"Atomic {stage} failed for {string.Join(" / ", sources.Select(value => value.Kind))}. {error.Message}");
                throw;
            }
            finally
            {
                foreach (string source in installed)
                {
                    using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(8));
                    try { await data.RunAsync(SqlJob("drop", source, $"DROP TABLE {table}"), cleanup.Token); }
                    catch (Exception error) when (failed is not null)
                    { Console.Error.WriteLine($"Could not remove test table {table} from {source}. {error.GetType().Name}"); }
                }
            }
        }
        finally { directory.Delete(recursive: true); }
    }

    private static async Task RecoverDecision(DataClient data, AtomicSource[] sources, string table,
        string[] participants, string arms, CancellationToken cancellation)
    {
        var job = TransactionJob("decision", participants, table, 34);
        await using var run = await data.StartAsync(job, new object(), cancellation);
        // The crash probe arms itself from the Native run directory and only needs
        // the run marker to exist before the durable commit decision is fsynced.
        // The journal is owned by the ticket directory, not the Native
        // execution identity. The crash probe keys its marker to that actual
        // directory so recovery tests follow the host's scoped state layout.
        File.WriteAllText(Path.Combine(arms, run.JournalTicket), "armed");
        try
        {
            await run.WaitAsync(cancellation);
            throw new InvalidOperationException("The durable-decision probe did not interrupt the Worker");
        }
        catch (DataExecutionException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown)
        {
            await using var recovery = await data.ResumeAsync(job, new object(), error.Reference, cancellation);
            await recovery.WaitAsync(cancellation);
        }
        foreach (var source in sources)
            await RequireRows(data, source, table, [31, 34], cancellation);
        Console.WriteLine("Native journal commit decision recovered without replaying the SQL posting");
    }

    private static DataClient BuildClient(string directory, AtomicSource[] sources, string[] pluginRoots)
    {
        var module = typeof(AtomicCase).Assembly.Location;
        string[] plugins = [Path.GetDirectoryName(module)!, .. pluginRoots];
        var allowed = sources.ToDictionary(source => source.Name,
            _ => new[] { "Open", "Discover", "Read", "Write", "Delete", "Receipt" }, StringComparer.Ordinal);
        var tenant = new { fileRoots = Array.Empty<string>(), networkOrigins = Array.Empty<string>(),
            pluginRoots = plugins, credentialReferences = Array.Empty<string>(), resources = allowed };
        var policy = Path.Combine(directory, "policy.json");
        File.WriteAllText(policy, JsonSerializer.Serialize(new
        {
            dataDirectory = Path.Combine(directory, "state"), tenant.fileRoots, tenant.networkOrigins,
            tenant.pluginRoots, credentialFiles = new Dictionary<string, string>(), maxConcurrency = 8,
            memoryLimit = 2_147_483_648UL, tenants = new { fluent = tenant }
        }));
        var builder = DataClient.CreateBuilder(policy, "fluent");
        foreach (var source in sources)
            builder.AddSource(new(source.Name, source.Kind, source.ConnectionString,
                nativeConnectionString: source.NativeConnectionString, provisionTransactionReceipts: true));
        return builder.Build();
    }

    private static DataJob<object> SqlJob(string action, string source, string statement) =>
        DataJob.Define<object>($"atomic-{action}-{Guid.NewGuid():N}").ExecuteSql(source, statement);

    private static DataJob<object> TransactionJob(string action, string[] participants, string table,
        long id, long? duplicateId = null)
    {
        var job = DataJob.Define<object>($"atomic-{action}-{Guid.NewGuid():N}").Transaction(participants);
        foreach (string source in participants)
        {
            job.ExecuteSql(source, $"INSERT INTO {table} (entry_id) VALUES ({id})");
            if (duplicateId is not null)
                job.ExecuteSql(source, $"INSERT INTO {table} (entry_id) VALUES ({duplicateId.Value})");
        }
        return job;
    }

    private static async Task RequireRows(DataClient data, AtomicSource[] sources, string table,
        long[] expected, CancellationToken cancellation)
    {
        foreach (var source in sources)
            await RequireRows(data, source, table, expected, cancellation);
    }

    private static async Task RequireRows(DataClient data, AtomicSource source, string table,
        long[] expected, CancellationToken cancellation)
    {
        var rows = new List<long>();
        if (source.Kind == DataSourceKind.Oracle)
        {
            var query = DataJob.Define<object>($"atomic-read-{Guid.NewGuid():N}")
                .FromSql<OracleAtomicRow>(source.Name, $"SELECT entry_id FROM {table} ORDER BY entry_id", stream: true)
                .Return();
            await foreach (var row in data.RunAsync(query, cancellation)) rows.Add(decimal.ToInt64(row.Id));
        }
        else
        {
            var query = DataJob.Define<object>($"atomic-read-{Guid.NewGuid():N}")
                .FromSql<AtomicRow>(source.Name, $"SELECT entry_id FROM {table} ORDER BY entry_id", stream: true)
                .Return();
            await foreach (var row in data.RunAsync(query, cancellation)) rows.Add(row.Id);
        }
        if (!rows.SequenceEqual(expected))
            throw new InvalidOperationException($"Independent SQL readback for {source.Kind} returned [{string.Join(',', rows)}], expected [{string.Join(',', expected)}]");
    }

    private static string NumberType(DataSourceKind kind) => kind == DataSourceKind.Oracle ? "NUMBER(18)" : "BIGINT";
}
