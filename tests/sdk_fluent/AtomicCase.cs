using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

public sealed record AtomicSource(string Name, DataSourceKind Kind, string ConnectionString,
    string NativeConnectionString);
public sealed record AtomicRequest(AtomicSourceInfo[] Participants, string Table, long Id, string Action,
    string? DecisionArms = null);
public sealed record AtomicSourceInfo(string Name, DataSourceKind Kind);
public sealed record AtomicReceipt(long Id, string[] Connections, DataRunReference Reference);
public sealed record OrdinaryWork(AtomicSourceInfo Source, string Table, string Action, long[]? Expected = null);

internal static class AtomicCase
{
    internal static async Task RunAsync(string fixturePath, string[] pluginRoots, string? decisionArms = null)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
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
                    string.IsNullOrWhiteSpace(source.ConnectionString) || string.IsNullOrWhiteSpace(source.NativeConnectionString)))
                throw new InvalidDataException("Each fixture must name two or three distinct XA sources with both provider connection settings");
            try { await RunGroup(group, pluginRoots, decisionArms, token); }
            catch (Exception error) when (!token.IsCancellationRequested)
            {
                var pair = string.Join(" / ", group.Select(source => source.Kind));
                var status = error is UniversalDataException sdk ? $"[{sdk.Code}/{sdk.Detail?.Cause}] " : "";
                Console.Error.WriteLine($"FAIL {pair} ({error.GetType().Name}): {status}{error.Message}");
                failures.Add(new InvalidOperationException(pair, error));
            }
        }
        if (failures.Count != 0) throw new AggregateException("One or more native Atomic fixture groups failed", failures);
    }

    private static async Task RunGroup(AtomicSource[] sources, string[] pluginRoots, string? decisionArms,
        CancellationToken cancellation)
    {
        var directory = Directory.CreateTempSubdirectory("ud-fluent-xa-");
        try
        {
            var module = typeof(AtomicJobs).Assembly.Location;
            string[] plugins = [Path.GetDirectoryName(module)!, .. pluginRoots];
            var allowed = sources.ToDictionary(source => source.Name,
                _ => new[] { "Open", "Discover", "Read", "Write", "Delete", "Receipt" }, StringComparer.Ordinal);
            var policy = Path.Combine(directory.FullName, "policy.json");
            var tenant = new { fileRoots = Array.Empty<string>(), networkOrigins = Array.Empty<string>(),
                pluginRoots = plugins, credentialReferences = Array.Empty<string>(), resources = allowed };
            File.WriteAllText(policy, JsonSerializer.Serialize(new { dataDirectory = Path.Combine(directory.FullName, "state"),
                tenant.fileRoots, tenant.networkOrigins, tenant.pluginRoots, credentialFiles = new Dictionary<string, string>(),
                maxConcurrency = 8, memoryLimit = 2_147_483_648UL,
                trustedAssemblyHashes = new[] { Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(module))).ToLowerInvariant() },
                tenants = new { fluent = tenant } }));
            var builder = DataClient.CreateBuilder(policy, "fluent").AddModule(typeof(AtomicJobs).Assembly);
            foreach (var source in sources)
                builder.AddSource(new(source.Name, source.Kind, source.ConnectionString,
                    nativeConnectionString: source.NativeConnectionString, provisionTransactionReceipts: true));
            await using var data = builder.Build();
            var participants = Array.ConvertAll(sources, source => new AtomicSourceInfo(source.Name, source.Kind));
            var table = "udxfa_" + Guid.NewGuid().ToString("N")[..20];
            var installed = new List<AtomicSourceInfo>(participants.Length);
            string stage = "setup";
            Exception? failed = null;
            try
            {
                foreach (var source in participants)
                {
                    stage = $"setup {source.Kind}";
                    await data.RunAsync(AtomicJobs.Ordinary, new OrdinaryWork(source, table, "create"), cancellation);
                    installed.Add(source);
                }
                stage = "commit posting";
                var confirmed = await data.RunAsync(AtomicJobs.Post, new AtomicRequest(participants, table, 31, "commit"), cancellation);
                if (confirmed.Id != 31 || confirmed.Connections.Length != participants.Length)
                    throw new InvalidOperationException("The Atomic receipt differs from its physical session evidence");
                stage = "recover confirmed posting";
                var commitment = await data.RecoverAsync(confirmed.Reference, cancellation);
                if (commitment.State != DataRecoveryState.Committed || commitment.Transactions.Count != 1)
                    throw new InvalidOperationException("Recovery did not preserve the confirmed transaction decision");
                if (decisionArms is not null)
                {
                    stage = "recover after durable journal decision before participant commit";
                    await RecoverDecision(data, participants, table, decisionArms, cancellation);
                    return;
                }
                try
                {
                    stage = "rollback failing callback";
                    await data.RunAsync(AtomicJobs.Post, new AtomicRequest(participants, table, 32, "rollback"), cancellation);
                    throw new InvalidOperationException("A failed business callback received an Atomic receipt");
                }
                catch (DataBusinessException error) when (error.RemoteType == typeof(InvalidOperationException).FullName &&
                    error.Message.Contains("abort posting", StringComparison.Ordinal)) { }
                catch (InvalidOperationException error) when (error.Message == "abort posting") { }
                DataRunReference interrupted;
                try
                {
                    stage = "terminate Worker in uncommitted callback";
                    await data.RunAsync(AtomicJobs.Post, new AtomicRequest(participants, table, 33, "crash"), cancellation);
                    throw new InvalidOperationException("An interrupted Atomic scope received a receipt");
                }
                catch (DataExecutionException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown)
                {
                    interrupted = error.Reference;
                }
                stage = "recover interrupted posting";
                var rollback = await data.RecoverAsync(interrupted, cancellation);
                if (rollback.State is not (DataRecoveryState.NoDecision or DataRecoveryState.RolledBack) ||
                    rollback.Transactions.Count != 1)
                    throw new InvalidOperationException($"Interrupted run {interrupted.RunId} returned {rollback.State} " +
                        $"for {rollback.Transactions.Count} transactions before any commit decision");
                foreach (var source in participants)
                {
                    stage = $"independent readback {source.Kind}";
                    var rows = await data.RunAsync(AtomicJobs.Ordinary, new OrdinaryWork(source, table, "read"), cancellation);
                    if (rows != 1) throw new InvalidOperationException("Independent database readback detected an uncommitted or rolled back row");
                }
                Console.WriteLine("Atomic commit and recovery after real process exit passed on " +
                    string.Join(" / ", participants.Select(value => value.Kind)));
            }
            catch (Exception error)
            {
                failed = error;
                Console.Error.WriteLine($"Atomic {stage} failed for {string.Join(" / ", participants.Select(value => value.Kind))}. {error.Message}");
                throw;
            }
            finally
            {
                foreach (var source in installed)
                {
                    using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(8));
                    try { await data.RunAsync(AtomicJobs.Ordinary, new OrdinaryWork(source, table, "drop"), cleanup.Token); }
                    catch (Exception error) when (failed is not null)
                    { Console.Error.WriteLine($"Could not remove test table {table} from {source.Kind}. {error.GetType().Name}"); }
                }
            }
        }
        finally { directory.Delete(recursive: true); }
    }

    private static async Task RecoverDecision(DataClient data, AtomicSourceInfo[] participants, string table,
        string arms, CancellationToken cancellation)
    {
        DataRunReference interrupted;
        try
        {
            await data.RunAsync(AtomicJobs.Post, new AtomicRequest(participants, table, 34, "decision", arms), cancellation);
            throw new InvalidOperationException("A Worker remained alive after the durable decision probe fired");
        }
        catch (DataExecutionException error) when (error.Code is StatusCode.CommitUnknown or StatusCode.WorkerLost)
        { interrupted = error.Reference; }
        string invocation = Path.Combine(arms, interrupted.RunId);
        string[] before = File.ReadAllLines(invocation);
        if (before.Length != 1 || !int.TryParse(before[0], out int businessPid) || businessPid <= 0)
            throw new InvalidOperationException("The request must leave exactly one durable business invocation record");
        var postgres = participants.Single(source => source.Kind == DataSourceKind.PostgreSql);
        await data.RunAsync(AtomicJobs.Ordinary, new OrdinaryWork(postgres, table, "read", [31]), cancellation);
        var decision = await data.RecoverAsync(interrupted, cancellation);
        if (decision.State != DataRecoveryState.Committed || decision.Transactions.Count != 1)
            throw new InvalidOperationException("The original SDK run did not recover its durable commit decision");
        foreach (var source in participants)
            await data.RunAsync(AtomicJobs.Ordinary, new OrdinaryWork(source, table, "read", [31, 34]), cancellation);
        if (!File.ReadAllLines(invocation).SequenceEqual(before))
            throw new InvalidOperationException("Recovery replayed business code");
        Console.WriteLine($"PostgreSQL and SQL Server committed after Worker {businessPid} exited at durable decision publication. Business invoked once.");
    }
}

public sealed class AtomicRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "entry_id")]
    public long Id { get; set; }
}

public static class AtomicJobs
{
    public static Task<int> Ordinary(DataContext context, OrdinaryWork work, CancellationToken cancellation) =>
        context.Database(work.Source.Name).UseAsync(async (database, _) =>
        {
            return work.Action switch
            {
                "create" => await database.Ado.ExecuteCommandAsync($"CREATE TABLE {work.Table} (entry_id {NumberType(work.Source.Kind)} PRIMARY KEY)"),
                "drop" => await database.Ado.ExecuteCommandAsync($"DROP TABLE {work.Table}"),
                "read" => await ReadProof(database, work.Table, work.Expected ?? [31]),
                _ => throw new ArgumentException("Unknown fixture action")
            };
        }, cancellation);

    private static async Task<int> ReadProof(ISqlSugarClient database, string table, long[] expected)
    {
        var readback = await database.Queryable<AtomicRow>().AS(table).OrderBy(row => row.Id).ToListAsync();
        if (!readback.Select(row => row.Id).SequenceEqual(expected))
            throw new InvalidOperationException("Physical readback must contain exactly the confirmed posting");
        return readback.Count;
    }

    public static Task<AtomicReceipt> Post(DataContext context, AtomicRequest request,
        CancellationToken cancellation) => context.Atomic(request.Participants.Select(source => source.Name).ToArray())
        .RunAsync(async (transaction, token) =>
        {
            if (!ReferenceEquals(DataContext.Current, transaction))
                throw new InvalidOperationException("The transaction callback lost its ambient context");
            var connections = new string[request.Participants.Length];
            for (int index = 0; index < request.Participants.Length; index++)
            {
                var source = request.Participants[index];
                string explicitId = await transaction.Database(source.Name).UseAsync(async (database, _) =>
                {
                    var id = await Proof("read physical session", source.Kind, () => PhysicalConnection(database, source.Kind));
                    int rows = await Proof("insert posting", source.Kind, () =>
                        database.Insertable(new AtomicRow { Id = request.Id }).AS(request.Table).ExecuteCommandAsync());
                    if (rows != 1) throw new InvalidOperationException("An enlisted insert did not affect one row");
                    return id;
                }, token);
                string implicitId = await DataContext.Current.Database(source.Name).UseAsync(async (database, _) =>
                {
                    int rows = await Proof("read uncommitted posting", source.Kind, () => database.Queryable<AtomicRow>()
                        .AS(request.Table).Where(value => value.Id == request.Id).CountAsync());
                    if (rows != 1) throw new InvalidOperationException("An enlisted query cannot read its uncommitted write");
                    return await Proof("read ambient physical session", source.Kind, () => PhysicalConnection(database, source.Kind));
                }, token);
                if (explicitId != implicitId) throw new InvalidOperationException("The ambient API leased a different physical transaction");
                connections[index] = implicitId;
            }
            if (request.Action == "rollback") throw new InvalidOperationException("abort posting");
            if (request.Action == "crash") Environment.Exit(47);
            if (request.Action == "decision")
            {
                using var entry = new FileStream(Path.Combine(request.DecisionArms!, context.RunReference.RunId),
                    FileMode.Append, FileAccess.Write, FileShare.Read);
                entry.Write(System.Text.Encoding.ASCII.GetBytes(Environment.ProcessId + "\n"));
                entry.Flush(flushToDisk: true);
            }
            return new AtomicReceipt(request.Id, connections, transaction.RunReference);
        }, cancellation);

    private static async Task<T> Proof<T>(string operation, DataSourceKind kind, Func<Task<T>> body)
    {
        try { return await body(); }
        catch (UniversalDataException error)
        {
            throw new InvalidOperationException($"{kind} cannot {operation} [{error.Code}]: {error.Message}", error);
        }
    }

    private static async Task<string> PhysicalConnection(ISqlSugarClient database, DataSourceKind kind)
    {
        string sql = kind switch
        {
            DataSourceKind.PostgreSql => "SELECT pg_backend_pid()",
            DataSourceKind.MySql or DataSourceKind.MariaDb => "SELECT CONNECTION_ID()",
            DataSourceKind.SqlServer => "SELECT @@SPID",
            DataSourceKind.Oracle => "SELECT SYS_CONTEXT('USERENV','SID') FROM DUAL",
            _ => throw new ArgumentException("Expected an XA-capable provider")
        };
        return (await database.Ado.GetScalarAsync(sql))?.ToString()
            ?? throw new InvalidOperationException("The database omitted its physical session identifier");
    }

    private static string NumberType(DataSourceKind kind) => kind == DataSourceKind.Oracle ? "NUMBER(18)" : "BIGINT";
}
