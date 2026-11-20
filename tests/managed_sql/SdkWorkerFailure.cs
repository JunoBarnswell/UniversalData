using System.Diagnostics;
using SqlSugar;
using UniversalData;

namespace ManagedSqlAcceptance;

internal static class SdkWorkerFailure
{
    internal static async Task Run(string connectionFile, string plugin)
    {
        var config = new ConnectionConfig
        {
            DbType = DbType.Sqlite, ConnectionString = File.ReadAllText(connectionFile),
            PluginPath = plugin, ReadBatchSize = 1, WriteChunkRows = 1
        };
        using (var reader = (await SqlSugarClient.OpenAsync(config)).Require())
        {
            reader.Ado.ExecuteCommand("CREATE TABLE managed_sql_orders(\"Id\" INTEGER NOT NULL PRIMARY KEY, \"Name\" TEXT NOT NULL, \"Amount\" TEXT NOT NULL)").Require();
            reader.Ado.ExecuteCommand("INSERT INTO managed_sql_orders VALUES(1,'durable','1.00')").Require();
            await using var cursor = (await reader.Queryable<ManagedSqlOrder>().OpenCursorAsync(1)).Require();
            if ((await cursor.ReadBatchAsync()).Require().Single().Id != 1)
                throw new InvalidOperationException("SQL Worker returned the wrong first batch");
            Kill(reader);
            if ((await cursor.ReadAsync()).Code != StatusCode.WorkerLost)
                throw new InvalidOperationException("A terminated read Worker must report WorkerLost");
        }
        using (var writer = (await SqlSugarClient.OpenAsync(config)).Require())
        {
            var fastest = writer.Fastest<ManagedSqlOrder>();
            fastest.PageSize = 1;
            var result = await fastest.BulkCopyAsync(Rows(writer));
            if (result.Code != StatusCode.CommitUnknown || result.Error?.Cause != StatusCode.WorkerLost)
                throw new InvalidOperationException($"Interrupted write returned {result.Code} ({result.Error?.Cause})");
        }
        using var verifier = (await SqlSugarClient.OpenAsync(config)).Require();
        if (Convert.ToInt64(verifier.Ado.GetScalar("SELECT count(*) FROM managed_sql_orders").Require()) != 1)
            throw new InvalidOperationException("An uncommitted batch survived Worker termination");
        Console.WriteLine("WorkerLost read; CommitUnknown interrupted write; reopened client readback passed");
    }

    private static IEnumerable<ManagedSqlOrder> Rows(SqlSugarClient client)
    {
        yield return new() { Id = 2, Name = "first", Amount = 2m };
        yield return new() { Id = 3, Name = "second", Amount = 3m };
        Kill(client);
        yield return new() { Id = 4, Name = "third", Amount = 4m };
    }

    private static void Kill(SqlSugarClient client)
    {
        using var process = Process.GetProcessById(client.WorkerProcessId);
        process.Kill();
        if (!process.WaitForExit(5000))
            throw new InvalidOperationException("Worker did not terminate");
    }
}
