using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using ManagedSqlAcceptance;
using SqlSugar;

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
    ManagedSqlStreamScale.Run(options);
    return 0;
}
var config = new ConnectionConfig
{
    DbType = options.Provider,
    ConnectionString = options.ConnectionString,
    PluginPath = options.PluginPath,
    ReadBatchSize = checked((uint)options.BatchSize),
    WriteChunkRows = checked((uint)options.BatchSize)
};
using var client = SqlAcceptance.OpenClient(config);
if (options.SetupSqlPath is not null)
    client.Ado.ExecuteCommand(File.ReadAllText(options.SetupSqlPath)).Require();

client.Ado.ExecuteCommand(options.CreateTableSql()).Require();
try
{
    using var process = Process.GetCurrentProcess();
    var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
    var cpuBefore = process.TotalProcessorTime;
    var stopwatch = Stopwatch.StartNew();
    var inserted = 0;
    client.Ado.BeginTran().Require();
    try
    {
        foreach (var rows in StreamRows(options.RowCount, options.BatchSize))
        {
            inserted += client.Insertable(rows).ExecuteCommand().Require();
        }
        client.Ado.CommitTran().Require();
    }
    catch
    {
        client.Ado.RollbackTran().Require();
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
    var ordered = client.Queryable<ManagedSqlOrder>()
        .Where(order => order.Id > 0)
        .OrderBy(order => (object)order.Id, OrderByType.Asc);
    foreach (var batch in ordered.ToBatches(options.BatchSize))
    {
        var elapsed = streamStarted.Elapsed;
        if (streamed == 0) { firstBatchObserved = true; firstBatchLatency = elapsed; }
        if (batch.Count > options.BatchSize)
            throw new InvalidOperationException("The returned batch exceeds the configured batch size.");
        fetchTimings.Add((elapsed - previousBatch).TotalMilliseconds);
        streamed += batch.Count;
        foreach (var row in batch)
            readSignature = unchecked(readSignature * 1099511628211UL + (ulong)row.Id);
        previousBatch = streamStarted.Elapsed;
    }
    streamStarted.Stop();
    var streamAllocated = GC.GetAllocatedBytesForCurrentThread() - readAllocatedBefore;
    var page = ordered.ToPageList(1, Math.Min(options.BatchSize, options.RowCount)).Require();
    var afterPage = options.BenchmarkPath is null ? ordered.ToList().Require().Count : options.RowCount;
    var descending = client.Queryable<ManagedSqlOrder>().OrderByDescending(order => (object)order.Id).ToPageList(1, 1).Require();
    if (page.Count == 0 || afterPage != options.RowCount || descending.Count != 1 || descending[0].Id != options.RowCount)
        throw new InvalidOperationException("Stable pagination or order state returned unexpected rows.");
    using (var early = ordered.ToBatches(1).GetEnumerator())
    {
        if (!early.MoveNext() || early.Current.Single().Id != 1)
            throw new InvalidOperationException("The streaming cursor did not return the first row.");
    }
    await using (var raw = (await client.Ado.OpenCursorAsync<SqlRow>("SELECT " + options.QuotedColumns() +
        " FROM " + options.QuotedTable() + " WHERE " + options.QuotedColumn("Id") + "=@id",
        [new SugarParameter("@id", 1)])).Require())
    {
        var row = (await raw.ReadAsync()).Require();
        if (Convert.ToInt64(row["Id"], CultureInfo.InvariantCulture) != 1)
            throw new InvalidOperationException("Raw SQL cursor returned an unexpected row.");
    }
    var updated = client.Updateable(new[] { new ManagedSqlOrder { Id = 1, Name = "updated", Amount = 99.50m } }).ExecuteCommand().Require();
    var scalar = Convert.ToInt64(client.Ado.GetScalar("SELECT COUNT(*) FROM " + options.QuotedTable()).Require(), CultureInfo.InvariantCulture);
    var deleted = client.Deleteable(new List<ManagedSqlOrder> { new() { Id = options.RowCount } }).ExecuteCommand().Require();
    var fastest = client.Fastest<ManagedSqlOrder>();
    var fastestCopied = fastest.BulkCopy(new List<ManagedSqlOrder> { new() { Id = options.RowCount, Name = "fast-copy", Amount = 7m } }).Require();
    var fastestUpdated = fastest.BulkUpdate(new[] { new ManagedSqlOrder { Id = options.RowCount, Name = "fast-update", Amount = 8m } }).Require();
    var fastestDeleted = fastest.BulkDelete(new[] { new ManagedSqlOrder { Id = options.RowCount } }).Require();
    client.Ado.BeginTran().Require();
    client.Insertable(new ManagedSqlOrder { Id = options.RowCount + 100, Name = "rollback", Amount = 1m }).ExecuteCommand().Require();
    client.Ado.RollbackTran().Require();
    var afterRollback = Convert.ToInt64(client.Ado.GetScalar("SELECT COUNT(*) FROM " + options.QuotedTable()).Require(), CultureInfo.InvariantCulture);
    try
    {
        foreach (var _ in client.Queryable<ManagedSqlOrder>().ToStream(new CancellationToken(true))) { }
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
    client.Ado.ExecuteCommand("DROP TABLE " + options.QuotedTable()).Require();
}
if (options.Provider == DbType.Sqlite && options.BenchmarkPath is null)
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
