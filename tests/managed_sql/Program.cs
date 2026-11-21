using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using ManagedSqlAcceptance;
using SqlSugar;
using UniversalData;
using static ManagedSqlAcceptance.SqlAcceptance;

if (args is ["--worker-failure", var connectionFile, var plugin])
{
    await SdkWorkerFailure.Run(connectionFile, plugin);
    return 0;
}

if (args.Length == 0 || args.Contains("--help", StringComparer.OrdinalIgnoreCase))
{
    Console.Error.WriteLine("managed_sql --provider sqlite|postgresql|mysql|mariadb|sqlserver|oracle --connection-file <local-file> --plugin <absolute-plugin> [--fixture json] [--rows count] [--batch size] [--benchmark output.json] [--setup-sql file] [--stream-copy] [--target-connection-file <local-file>]");
    return args.Length == 0 ? 2 : 0;
}

var options = ManagedSqlOptions.Parse(args);
if (args.Contains("--stream-copy", StringComparer.Ordinal))
{
    await ManagedSqlStreamScale.RunAsync(options);
    return 0;
}
using var fixture = OpenFixture(options.Provider, Path.GetFullPath(options.PluginPath), options.ConnectionString);
var client = fixture.Session;
if (options.SetupSqlPath is not null)
    Execute(client, File.ReadAllText(options.SetupSqlPath));

Execute(client, options.CreateTableSql());
try
{
    using var process = Process.GetCurrentProcess();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var cpuBefore = process.TotalProcessorTime;
    var stopwatch = Stopwatch.StartNew();
    var inserted = 0;
    Transaction(client, SqlNative.TransactionBegin);
    try
    {
        foreach (var rows in StreamRows(options.RowCount, options.BatchSize))
        {
            inserted += WriteRows(client, SqlNative.WriteAppend, rows, options.BatchSize);
        }
        Transaction(client, SqlNative.TransactionCommit);
    }
    catch
    {
        Transaction(client, SqlNative.TransactionRollback);
        throw;
    }
    var streamed = 0;
    var firstBatchObserved = false;
    var firstBatchLatency = TimeSpan.Zero;
    var fetchTimings = new List<double>();
    var readAllocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var streamStarted = Stopwatch.StartNew();
    var previousBatch = TimeSpan.Zero;
    ulong readSignature = 0;
    var table = options.QuotedTable();
    var id = options.QuotedColumn("Id");
    var columns = options.QuotedColumns();
    var prefix = $"SELECT {columns} FROM {table}";
    var ordered = $"{prefix} WHERE {id} > 0 ORDER BY {id}";
    foreach (var batch in ReadBatches<ManagedSqlOrder>(client, ordered, options.BatchSize))
    {
        var elapsed = streamStarted.Elapsed;
        if (streamed == 0) { firstBatchObserved = true; firstBatchLatency = elapsed; }
        if (batch.Length > options.BatchSize)
            throw new InvalidOperationException("The returned batch exceeds the configured batch size.");
        fetchTimings.Add((elapsed - previousBatch).TotalMilliseconds);
        streamed += batch.Length;
        foreach (var row in batch)
            readSignature = unchecked(readSignature * 1099511628211UL + (ulong)row.Id);
        previousBatch = streamStarted.Elapsed;
    }
    streamStarted.Stop();
    var streamAllocated = GC.GetAllocatedBytesForCurrentThread() - readAllocatedBefore;
    var pageSize = Math.Min(options.BatchSize, options.RowCount);
    var page = ReadBatches<ManagedSqlOrder>(client, ordered, pageSize).First();
    var afterPage = (options.BenchmarkPath is null
        ? ReadBatches<ManagedSqlOrder>(client, ordered, options.BatchSize).Sum(rows => rows.Length)
        : options.RowCount);
    var descending = ReadBatches<ManagedSqlOrder>(client, $"{prefix} ORDER BY {id} DESC", 1).First();
    Require(page.Length == pageSize && afterPage == options.RowCount && descending is [{ Id: var maximum }] &&
        maximum == options.RowCount, "An ordered cursor returned unexpected rows");
    using (var early = ReadBatches<ManagedSqlOrder>(client, ordered, 1).GetEnumerator())
        Require(early.MoveNext() && early.Current.Single().Id == 1, "The streaming cursor did not return the first row");
    Require(Scalar<long>(client, $"SELECT {id} FROM {table} WHERE {id}=@id", new SugarParameter("@id", 1)) == 1,
        "Native bound scalar returned an unexpected row");
    var updated = WriteRows(client, SqlNative.WriteUpdate,
        new[] { new ManagedSqlOrder { Id = 1, Name = "updated", Amount = 99.50m } }, options.BatchSize);
    var scalar = Scalar<long>(client, $"SELECT COUNT(*) FROM {table}");
    var deleted = WriteRows(client, SqlNative.WriteDelete,
        new[] { new ManagedSqlOrder { Id = options.RowCount } }, options.BatchSize);
    var tail = new ManagedSqlOrder { Id = options.RowCount, Name = "native-copy", Amount = 7m };
    var fastestCopied = WriteRows(client, SqlNative.WriteAppend, new[] { tail }, options.BatchSize, bulk: true);
    tail.Name = "native-update";
    tail.Amount = 8m;
    var fastestUpdated = WriteRows(client, SqlNative.WriteUpdate, new[] { tail }, options.BatchSize, bulk: true);
    var fastestDeleted = WriteRows(client, SqlNative.WriteDelete, new[] { tail }, options.BatchSize, bulk: true);
    Transaction(client, SqlNative.TransactionBegin);
    _ = WriteRows(client, SqlNative.WriteAppend,
        new[] { new ManagedSqlOrder { Id = options.RowCount + 100, Name = "rollback", Amount = 1m } }, options.BatchSize);
    Transaction(client, SqlNative.TransactionRollback);
    var afterRollback = Scalar<long>(client, $"SELECT COUNT(*) FROM {table}");
    try
    {
        _ = ReadBatches<ManagedSqlOrder>(client, ordered, options.BatchSize, new CancellationToken(true)).First();
        throw new InvalidOperationException("A cancelled stream did not throw.");
    }
    catch (OperationCanceledException) { }
    stopwatch.Stop();
    process.Refresh();
    var cpuElapsed = process.TotalProcessorTime - cpuBefore;
    var managedAllocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
    if (inserted != options.RowCount || streamed != options.RowCount || !firstBatchObserved || scalar != options.RowCount || updated != 1 || deleted != 1 || fastestCopied != 1 || fastestUpdated != 1 || fastestDeleted != 1 || afterRollback != options.RowCount - 1)
        throw new InvalidOperationException($"Managed SQL result mismatch: inserted={inserted}, streamed={streamed}, scalar={scalar}, updated={updated}, deleted={deleted}, fastest={fastestCopied}/{fastestUpdated}/{fastestDeleted}, rollbackCount={afterRollback}.");
    if (options.BenchmarkPath is null && options.RowCount > 1)
        ManagedSqlWriteCases.Run(client, options, options.RowCount - 1);
    if (options.BenchmarkPath is not null)
    {
        fetchTimings.Sort();
        var report = new
        {
            provider = options.Provider.ToString(),
            rows = options.RowCount,
            batchRows = options.BatchSize,
            elapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
            throughputRowsPerSecond = stopwatch.Elapsed.TotalSeconds == 0 ? 0 : options.RowCount / stopwatch.Elapsed.TotalSeconds,
            firstBatchLatencyMilliseconds = firstBatchLatency.TotalMilliseconds,
            readMilliseconds = streamStarted.Elapsed.TotalMilliseconds,
            fetchP95Milliseconds = fetchTimings[(int)Math.Ceiling(fetchTimings.Count * 0.95) - 1],
            readAllocatedBytes = streamAllocated,
            readChecksum = readSignature.ToString("x16", CultureInfo.InvariantCulture),
            cpuMilliseconds = cpuElapsed.TotalMilliseconds,
            managedAllocatedBytes = managedAllocated,
            peakWorkingSetBytes = process.PeakWorkingSet64,
            inserted,
            streamed,
            updated,
            deleted,
            fastestCopied,
            fastestUpdated,
            fastestDeleted,
            resultSignature = $"{inserted}:{streamed}:{updated}:{deleted}:{scalar}"
        };
        File.WriteAllText(options.BenchmarkPath, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
    }
    Console.WriteLine($"PASS provider={options.Provider} rows={options.RowCount} batch={options.BatchSize} elapsedMs={stopwatch.Elapsed.TotalMilliseconds:F1}");
}
finally
{
    Execute(client, "DROP TABLE " + options.QuotedTable());
}
if (options.Provider == DataSourceKind.Sqlite && options.BenchmarkPath is null)
    ManagedSqlEtl.Run(options.PluginPath);
return 0;

static IEnumerable<List<ManagedSqlOrder>> StreamRows(int count, int batchSize)
{
    for (var first = 1; first <= count; first += batchSize)
    {
        var rows = new List<ManagedSqlOrder>(Math.Min(batchSize, count - first + 1));
        for (var id = first; id < first + rows.Capacity; id++) rows.Add(new ManagedSqlOrder
        {
            Id = id,
            Name = "order-" + id.ToString(CultureInfo.InvariantCulture),
            Amount = id * 1.25m
        });
        yield return rows;
    }
}
