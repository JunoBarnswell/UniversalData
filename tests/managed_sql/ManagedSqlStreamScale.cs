using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using SqlSugar;

namespace ManagedSqlAcceptance;

internal static class ManagedSqlStreamScale
{
    internal static void Run(ManagedSqlOptions options)
    {
        if (options.Provider is not (DbType.Sqlite or DbType.PostgreSQL) || options.BenchmarkPath is null)
            throw new ArgumentException("Stream-copy benchmarking requires SQLite or PostgreSQL and --benchmark.");
        var sqlite = options.Provider == DbType.Sqlite;
        if (sqlite && (string.IsNullOrWhiteSpace(options.TargetConnectionString) ||
                       options.ConnectionString == options.TargetConnectionString))
            throw new ArgumentException("Provide separate SQLite files for the streaming source and target.");

        const string sourceTable = "ud_stream_scale_source";
        const string targetTable = "ud_stream_scale_target";
        var amountType = sqlite ? "TEXT" : "NUMERIC(18,2)";
        using var source = Open(options.ConnectionString);
        using var target = Open(options.TargetConnectionString ?? options.ConnectionString);
        source.Ado.ExecuteCommand($"CREATE TABLE {sourceTable}(id BIGINT PRIMARY KEY, label TEXT NOT NULL, amount {amountType} NOT NULL)");
        try
        {
            target.Ado.ExecuteCommand($"CREATE TABLE {targetTable}(id BIGINT PRIMARY KEY, label TEXT NOT NULL, amount {amountType} NOT NULL)");
            try
            {
                var count = options.RowCount.ToString(CultureInfo.InvariantCulture);
                var generate = sqlite
                    ? $"WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<{count}) " +
                      "SELECT n,printf('  订单-%08d  ',n),printf('%d.%02d',n/4,n%4*25) FROM ids"
                    : $"SELECT n,'  订单-'||lpad(n::text,8,'0')||'  ',n*0.25 FROM generate_series(1,{count}) n";
                source.Ado.ExecuteCommand($"INSERT INTO {sourceTable} {generate}");

                using var process = Process.GetCurrentProcess();
                var batches = source.Queryable<SourceRow>().OrderBy(row => row.Id).ToBatches(options.BatchSize);
                var writer = target.Fastest<TargetRow>();
                writer.PageSize = options.BatchSize;
                var inputRows = 0;
                var firstBatchMilliseconds = 0.0;
                var before = GC.GetAllocatedBytesForCurrentThread();
                var stopwatch = Stopwatch.StartNew();
                var written = writer.BulkCopy(Transform());
                stopwatch.Stop();
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                process.Refresh();
                var peakRss = process.PeakWorkingSet64;

                var read = 0;
                ulong signature = 14695981039346656037UL;
                foreach (var row in target.Queryable<TargetRow>().OrderBy(row => row.Id).ToStream())
                {
                    if (row.Id != ++read ||
                        row.Label != FormattableString.Invariant($"订单-{read:D8}") ||
                        row.Amount != (read + 1) * 0.25m)
                        throw new InvalidOperationException($"Unexpected streaming result at row {read}.");
                    signature = unchecked((signature ^ (ulong)row.Id ^ (ulong)(row.Amount * 100)) * 1099511628211UL);
                }
                if (inputRows != options.RowCount || written != inputRows || read != written)
                    throw new InvalidOperationException($"Stream counts differ: source {inputRows}, written {written}, read back {read}.");

                File.WriteAllText(options.BenchmarkPath, JsonSerializer.Serialize(new
                {
                    provider = options.Provider.ToString(),
                    rows = options.RowCount,
                    batchRows = options.BatchSize,
                    inputRows,
                    written,
                    read,
                    checksum = signature.ToString("x16", CultureInfo.InvariantCulture),
                    firstBatchMilliseconds,
                    elapsedMilliseconds = stopwatch.Elapsed.TotalMilliseconds,
                    rowsPerSecond = written / stopwatch.Elapsed.TotalSeconds,
                    managedAllocatedBytes = allocated,
                    peakRssBytes = peakRss
                }));

                IEnumerable<TargetRow> Transform()
                {
                    foreach (var batch in batches)
                    {
                        if (inputRows == 0) firstBatchMilliseconds = stopwatch.Elapsed.TotalMilliseconds;
                        inputRows += batch.Count;
                        foreach (var row in batch)
                            yield return new TargetRow { Id = row.Id, Label = row.Label.Trim(), Amount = row.Amount + 0.25m };
                    }
                }
            }
            finally { target.Ado.ExecuteCommand($"DROP TABLE {targetTable}"); }
        }
        finally { source.Ado.ExecuteCommand($"DROP TABLE {sourceTable}"); }

        SqlSugarClient Open(string connection) => new(new ConnectionConfig
        {
            DbType = options.Provider,
            ConnectionString = connection,
            PluginPath = options.PluginPath,
            ReadBatchSize = (uint)options.BatchSize,
            WriteChunkRows = (uint)options.BatchSize
        });
    }

    internal class ScaleRow
    {
        [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
        public long Id { get; set; }

        [SugarColumn(ColumnName = "label")]
        public string Label { get; set; } = "";

        [SugarColumn(ColumnName = "amount", Length = 18, DecimalDigits = 2)]
        public decimal Amount { get; set; }
    }

    [SugarTable("ud_stream_scale_source")]
    internal sealed class SourceRow : ScaleRow { }

    [SugarTable("ud_stream_scale_target")]
    internal sealed class TargetRow : ScaleRow { }
}
