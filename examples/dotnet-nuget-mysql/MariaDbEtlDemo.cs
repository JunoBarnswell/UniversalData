using System.Data;
using System.Threading.Channels;
using SqlSugar;
using UniversalData;
using DbType = SqlSugar.DbType;

internal static class MariaDbEtlDemo
{
    private const string Database = "ud_nuget_demo";
    private const int RowCount = 1_003;
    private const int PartitionBoundary = 500;
    private const int BatchSize = 128;

    internal static async Task RunAsync(string connectionString)
    {
        using var target = await OpenAsync(connectionString);
        var database = GetValue(target.Ado.GetScalar("SELECT DATABASE()"), "Read the current database");
        if (!string.Equals(database as string, Database, StringComparison.Ordinal))
            throw new InvalidOperationException($"The ETL demo requires the dedicated database '{Database}'.");

        var runId = Guid.NewGuid().ToString("N");
        Console.WriteLine($"ETL run {runId}");
        PrepareSource(target, runId);

        using var first = await OpenAsync(connectionString);
        using var second = await OpenAsync(connectionString);
        var partitions = await CopyAsync(first, second, target, runId);

        VerifyTarget(target, runId);
        Console.WriteLine($"Read Worker {first.WorkerProcessId}: IDs 1–{PartitionBoundary}, {partitions[0]} rows.");
        Console.WriteLine($"Read Worker {second.WorkerProcessId}: IDs {PartitionBoundary + 1}–{RowCount}, {partitions[1]} rows.");
        Console.WriteLine($"Write Worker {target.WorkerProcessId}: {partitions.Sum()} rows committed and verified.");
    }

    private static async Task<SqlSugarClient> OpenAsync(string connectionString) =>
        GetValue(await SqlSugarClient.OpenAsync(new ConnectionConfig
        {
            DbType = DbType.MariaDb,
            ConnectionString = connectionString,
            ReadBatchSize = BatchSize,
            WriteChunkRows = BatchSize
        }), "Open a MariaDB Worker");

    private static void PrepareSource(SqlSugarClient client, string runId)
    {
        foreach (var table in new[] { "ud_demo_etl_source", "ud_demo_etl_target" })
        {
            Check(client.Ado.ExecuteCommand($"""
                CREATE TABLE IF NOT EXISTS {table} (
                    RunId VARCHAR(32) NOT NULL,
                    Id INT NOT NULL,
                    Name VARCHAR(80) NOT NULL,
                    Amount DECIMAL(38,28) NOT NULL,
                    PRIMARY KEY (RunId, Id)
                ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
                """), $"Prepare {table}");
        }

        var source = client.Fastest<EtlSourceRow>();
        source.PageSize = BatchSize;
        var inserted = GetValue(source.BulkCopy(Enumerable.Range(1, RowCount).Select(id => new EtlSourceRow
        {
            RunId = runId,
            Id = id,
            Name = $"  订单-{id:D4}-🚚  ",
            Amount = id / 100m
        })), "Seed the source table");
        if (inserted != RowCount)
            throw new InvalidOperationException($"Seeded {inserted} rows. Expected {RowCount}.");
    }

    private static async Task<int[]> CopyAsync(
        SqlSugarClient first, SqlSugarClient second, SqlSugarClient target, string runId)
    {
        using var stop = new CancellationTokenSource();
        var queue = Channel.CreateBounded<EtlTargetRow[]>(2);
        var extraction = Task.WhenAll(
            ExtractAsync(first, runId, 1, PartitionBoundary, queue.Writer, stop.Token),
            ExtractAsync(second, runId, PartitionBoundary + 1, RowCount, queue.Writer, stop.Token));
        var completion = CompleteExtractionAsync(extraction, queue.Writer);
        try
        {
            await WriteTargetAsync(target, runId, queue.Reader);
            return await extraction;
        }
        finally
        {
            stop.Cancel();
            await completion;
        }
    }

    private static async Task<int> ExtractAsync(SqlSugarClient source, string runId,
        int firstId, int lastId, ChannelWriter<EtlTargetRow[]> output, CancellationToken cancellation)
    {
        var query = source.Queryable<EtlSourceRow>()
            .Where(row => row.RunId == runId && row.Id >= firstId && row.Id <= lastId)
            .OrderBy(row => row.Id);
        await using var cursor = GetValue(await query.OpenCursorAsync(BatchSize, cancellation), "Open a partition cursor");
        var read = 0;
        while (true)
        {
            var result = await cursor.ReadBatchAsync(cancellation);
            if (result.Code == StatusCode.End)
                return read;

            var batch = GetValue(result, "Read a partition batch");
            await output.WriteAsync(batch.Select(Transform).ToArray(), cancellation);
            read += batch.Count;
        }
    }

    private static EtlTargetRow Transform(EtlSourceRow source) => new()
    {
        RunId = source.RunId,
        Id = source.Id,
        Name = source.Name.Trim(),
        Amount = source.Amount + 1.25m
    };

    private static async Task CompleteExtractionAsync(Task extraction, ChannelWriter<EtlTargetRow[]> output)
    {
        try
        {
            await extraction;
            output.Complete();
        }
        catch (Exception error)
        {
            output.TryComplete(error);
        }
    }

    private static async Task WriteTargetAsync(SqlSugarClient target, string runId, ChannelReader<EtlTargetRow[]> input)
    {
        var writer = target.Fastest<EtlTargetRow>();
        writer.PageSize = BatchSize;
        Check(target.Ado.BeginTran(IsolationLevel.ReadCommitted), "Start the target transaction");
        try
        {
            await foreach (var batch in input.ReadAllAsync())
            {
                var written = GetValue(await writer.BulkCopyAsync(batch), "Load a target batch");
                if (written != batch.Length)
                    throw new InvalidOperationException($"Loaded {written} rows. Expected {batch.Length}.");
            }

            VerifyTarget(target, runId);
            Check(target.Ado.CommitTran(), "Commit the target transaction");
        }
        catch
        {
            var rollback = target.Ado.RollbackTran();
            if (!rollback.IsSuccess)
                Console.Error.WriteLine($"Could not confirm target rollback ({rollback.Code}). Check run {runId} before retrying.");
            throw;
        }
    }

    private static void VerifyTarget(SqlSugarClient target, string runId)
    {
        // The fixture is fixed at 1,003 rows. The transfer itself holds only bounded batches.
        var rows = GetValue(target.Queryable<EtlTargetRow>().Where(row => row.RunId == runId)
            .OrderBy(row => row.Id).ToList(), "Read back the target");
        if (rows.Count != RowCount)
            throw new InvalidOperationException($"Read back {rows.Count} target rows. Expected {RowCount}.");

        for (var index = 0; index < rows.Count; index++)
        {
            var id = index + 1;
            var row = rows[index];
            if (row.Id != id || row.Name != $"订单-{id:D4}-🚚" || row.Amount != (id + 125) / 100m)
                throw new InvalidOperationException($"Target result differs from the fixture at ID {id}.");
        }
    }

    private static T GetValue<T>(Result<T> result, string operation)
    {
        Check(new Result(result.Code), operation);
        return result.Value!;
    }

    private static void Check<T>(Result<T> result, string operation) => Check(new Result(result.Code), operation);

    private static void Check(Result result, string operation)
    {
        if (!result.IsSuccess)
            throw new InvalidOperationException($"{operation} failed ({result.Code}).");
    }
}
