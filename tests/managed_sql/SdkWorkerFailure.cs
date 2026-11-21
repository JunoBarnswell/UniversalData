using System.Diagnostics;
using System.Text.Json;
using UniversalData;

namespace ManagedSqlAcceptance;

public static class SdkWorkerFailure
{
    internal static async Task Run(string connectionFile, string plugin)
    {
        var directory = Directory.CreateTempSubdirectory("ud-sdk-loss-");
        try
        {
            string connection = (await File.ReadAllTextAsync(connectionFile)).Trim();
            var parsed = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = connection };
            string dataFile = Path.GetFullPath((string)parsed["Data Source"]);
            string credentialFile = Path.Combine(directory.FullName, "orders.credential");
            await File.WriteAllTextAsync(credentialFile, connection);
            string policy = Path.Combine(directory.FullName, "policy.json");
            await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
            {
                dataDirectory = Path.Combine(directory.FullName, "state"),
                fileRoots = new[] { Path.GetDirectoryName(dataFile)! },
                networkOrigins = Array.Empty<string>(),
                pluginRoots = new[] { Path.GetDirectoryName(Path.GetFullPath(plugin))!, AppContext.BaseDirectory },
                credentialFiles = new Dictionary<string, string> { ["orders"] = credentialFile }
            }));
            await using var data = DataClient.CreateBuilder(policy, "local")
                .AddSqlite("orders", connection, options => options.CredentialReference = "orders")
                .Build();
            await data.RunAsync(DataJob.Define("worker-failure-prepare")
                .ExecuteSql("orders", "CREATE TABLE managed_sql_orders(id INTEGER NOT NULL PRIMARY KEY)")
                .ExecuteSql("orders", "INSERT INTO managed_sql_orders VALUES(1)"));

            var interrupted = DataJob.Define("worker-failure-interrupt")
                .Transaction("orders")
                .ExecuteSql("orders", """
                    INSERT INTO managed_sql_orders(id)
                    SELECT 2 WHERE length(hex(randomblob(16777216))) > 0
                    """);
            var package = data.Compile(interrupted, publish: true);
            await using (var doomed = await data.StartAsync(package))
            {
                if (doomed.WorkerProcessId == Environment.ProcessId)
                    throw new InvalidOperationException("The request did not leave the application process");
                using var worker = Process.GetProcessById(doomed.WorkerProcessId);
                try { worker.Kill(entireProcessTree: true); }
                catch (InvalidOperationException) { }
                try
                {
                    await doomed.WaitAsync();
                    throw new InvalidOperationException("An interrupted write was reported as committed");
                }
                catch (DataExecutionException failure) when (failure.Code is StatusCode.CommitUnknown or StatusCode.WorkerLost) { }
            }
            var count = DataJob.Define("worker-failure-count")
                .FromSql<WorkerFailureCount>("orders", "SELECT COUNT(*) AS row_count FROM managed_sql_orders", stream: true)
                .Return();
            var rows = new List<WorkerFailureCount>();
            await foreach (var row in data.RunAsync(count)) rows.Add(row);
            if (rows.Count != 1 || rows[0].Count != 1L)
                throw new InvalidOperationException("A database transaction survived a physical Worker exit");
            Console.WriteLine("Independent process, CommitUnknown on interruption and reopened readback passed");
        }
        finally { directory.Delete(recursive: true); }
    }
}
