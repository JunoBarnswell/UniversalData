using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using UniversalData;
using static ManagedSqlAcceptance.SqlAcceptance;

namespace ManagedSqlAcceptance;

internal static class ManagedSqlStreamScale
{
    internal static async Task RunAsync(ManagedSqlOptions options)
    {
        if (options.Provider is not (DataSourceKind.Sqlite or DataSourceKind.PostgreSql) || options.BenchmarkPath is null)
            throw new ArgumentException("Stream-copy benchmarking requires SQLite or PostgreSQL and --benchmark.");
        var sqlite = options.Provider == DataSourceKind.Sqlite;
        if (sqlite && (string.IsNullOrWhiteSpace(options.TargetConnectionString) ||
                       options.ConnectionString == options.TargetConnectionString))
            throw new ArgumentException("Provide separate SQLite files for the streaming source and target.");

        const string sourceTable = "ud_stream_scale_source";
        const string targetTable = "ud_stream_scale_target";
        var plugin = Path.GetFullPath(options.PluginPath);
        var amountType = sqlite ? "TEXT" : "DECIMAL(18,2)";
        var targetConnection = options.TargetConnectionString ?? options.ConnectionString;
        using var sourceFixture = OpenFixture(options.Provider, plugin, options.ConnectionString);
        using var targetFixture = OpenFixture(options.Provider, plugin, targetConnection);
        var source = sourceFixture.Session;
        var target = targetFixture.Session;
        Execute(source, $"CREATE TABLE {sourceTable}(id BIGINT PRIMARY KEY, label TEXT NOT NULL, amount {amountType} NOT NULL)");
        try
        {
            Execute(target, $"CREATE TABLE {targetTable}(id BIGINT PRIMARY KEY, label TEXT NOT NULL, amount {amountType} NOT NULL)");
            try
            {
                var count = options.RowCount.ToString(CultureInfo.InvariantCulture);
                var generate = sqlite
                    ? $"WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<{count}) " +
                      "SELECT n,printf('  订单-%08d  ',n),printf('%d.%02d',n/4,n%4*25) FROM ids"
                    : $"SELECT n,'  订单-'||lpad(n::text,8,'0')||'  ',n*0.25 FROM generate_series(1,{count}) n";
                Execute(source, $"INSERT INTO {sourceTable} {generate}");

                var directory = Directory.CreateTempSubdirectory("ud-sdk-scale-");
                try
                {
                    string policy = Path.Combine(directory.FullName, "policy.json");
                    var fileRoots = sqlite ? new[] { Path.GetDirectoryName(SqlitePath(options.ConnectionString))!,
                        Path.GetDirectoryName(SqlitePath(targetConnection))! }.Distinct().ToArray() : [];
                    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
                    {
                        dataDirectory = Path.Combine(directory.FullName, "state"), fileRoots,
                        networkOrigins = Array.Empty<string>(),
                        pluginRoots = new[] { Path.GetDirectoryName(plugin)!, AppContext.BaseDirectory },
                        credentialFiles = new Dictionary<string, string>(),
                        resources = new Dictionary<string, string[]>
                        {
                            ["source"] = ["Open", "Read", "Discover"],
                            ["target"] = ["Open", "Read", "Write", "Discover"]
                        }
                    }));
                    DataSourceDefinition Definition(string name, string value) => new(name, options.Provider,
                        sqlite ? $"Data Source={SqlitePath(value)}" : value, nativeConnectionString: value);
                    await using var data = DataClient.CreateBuilder(policy, "local")
                        .AddSource(Definition("source", options.ConnectionString))
                        .AddSource(Definition("target", targetConnection))
                        .ConfigureWorkers(worker => worker.ExecutionTimeout(TimeSpan.FromHours(1)))
                        .Build();
                    using var process = Process.GetCurrentProcess();
                    var before = GC.GetTotalAllocatedBytes(precise: true);
                    var stopwatch = Stopwatch.StartNew();
                    var job = DataJob.Define("managed-stream-scale")
                        .WithOptions(new(BatchRows: (uint)options.BatchSize));
                    var input = job.FromSql<StreamScaleRow>("source",
                        $"SELECT id,label,amount FROM {sourceTable} ORDER BY id", stream: true);
                    var copy = sqlite
                        ? input.Select(row => new StreamScaleTextRow
                        {
                            Id = row.Id, Label = row.Label.Trim(),
                            Amount = (row.Amount + 0.25m).ToString(CultureInfo.InvariantCulture)
                        }).ToTable("target", targetTable, durable: false).Append()
                        : input.Select(row => new StreamScaleRow
                        {
                            Id = row.Id, Label = row.Label.Trim(), Amount = row.Amount + 0.25m
                        }).ToTable("target", targetTable, durable: false).Append();
                    await using var run = await data.StartAsync(copy);
                    using var workerProcess = Process.GetProcessById(run.WorkerProcessId);
                    ExecutionMetrics metrics;
                    try { metrics = await run.WaitAsync(); }
                    catch (DataExecutionException error)
                    {
                        Console.Error.WriteLine(JsonSerializer.Serialize(new { error.Code, error.Detail, error.Metrics }));
                        try { Console.Error.WriteLine((await run.DiagnosticsAsync()).GetRawText()); }
                        catch (UniversalDataException) { }
                        throw;
                    }
                    stopwatch.Stop();
                    var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
                    process.Refresh();
                    workerProcess.Refresh();

                    var read = 0;
                    ulong signature = 14695981039346656037UL;
                    foreach (var batch in ReadBatches<StreamScaleRow>(target, $"SELECT id,label,amount FROM {targetTable} ORDER BY id",
                                 options.BatchSize))
                        foreach (var row in batch)
                        {
                            if (row.Id != ++read ||
                                row.Label != FormattableString.Invariant($"订单-{read:D8}") ||
                                row.Amount != (read + 1) * 0.25m)
                                throw new InvalidOperationException($"Unexpected streaming result at row {read}.");
                            signature = unchecked((signature ^ (ulong)row.Id ^ (ulong)(row.Amount * 100)) * 1099511628211UL);
                        }
                    if (metrics.InputRows != (ulong)options.RowCount || metrics.CommittedRows != metrics.InputRows ||
                        (ulong)read != metrics.CommittedRows)
                        throw new InvalidOperationException($"Stream counts differ: source {metrics.InputRows}, written {metrics.CommittedRows}, read back {read}.");

                    await File.WriteAllTextAsync(options.BenchmarkPath, JsonSerializer.Serialize(new
                    {
                        provider = options.Provider.ToString(),
                        rows = options.RowCount,
                        batchRows = options.BatchSize,
                        inputRows = metrics.InputRows,
                        written = metrics.CommittedRows,
                        read,
                        checksum = signature.ToString("x16", CultureInfo.InvariantCulture),
                        elapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
                        rowsPerSecond = read / stopwatch.Elapsed.TotalSeconds,
                        clientAllocatedBytes = allocated,
                        clientPeakRssBytes = process.PeakWorkingSet64,
                        workerPeakRssBytes = workerProcess.PeakWorkingSet64,
                        nativeMemoryBytes = metrics.MemoryUsed,
                        batches = metrics.Batches,
                        queueHighWater = metrics.QueueHighWater
                    }));
                }
                finally { directory.Delete(recursive: true); }
            }
            finally { Execute(target, $"DROP TABLE {targetTable}"); }
        }
        finally { Execute(source, $"DROP TABLE {sourceTable}"); }
    }

    private static string SqlitePath(string connection)
    {
        var config = new System.Data.Common.DbConnectionStringBuilder();
        if (connection.Contains('='))
        {
            config.ConnectionString = connection;
            foreach (var key in new[] { "Data Source", "DataSource", "Filename" })
                if (config.TryGetValue(key, out var value) && value is string path)
                    return Path.GetFullPath(path);
            throw new ArgumentException("SQLite needs a database path");
        }
        return Path.GetFullPath(connection);
    }
}
