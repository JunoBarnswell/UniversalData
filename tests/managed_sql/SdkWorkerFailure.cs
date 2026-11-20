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
            string policy = Path.Combine(directory.FullName, "policy.json");
            await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
            {
                dataDirectory = Path.Combine(directory.FullName, "state"),
                fileRoots = new[] { Path.GetDirectoryName(dataFile)! },
                networkOrigins = Array.Empty<string>(),
                pluginRoots = new[] { Path.GetDirectoryName(Path.GetFullPath(plugin))!, AppContext.BaseDirectory },
                credentialFiles = new Dictionary<string, string>()
            }));
            await using var data = DataClient.CreateBuilder(policy, "local").AddSqlite("orders", connection)
                .AddModule(typeof(SdkWorkerFailure).Assembly).Build();
            await data.RunAsync(Prepare);
            await using (var doomed = await data.OpenSessionAsync())
            {
                if (doomed.ProcessId == Environment.ProcessId)
                    throw new InvalidOperationException("The request did not leave the application process");
                try
                {
                    await doomed.RunAsync(InterruptWrite);
                    throw new InvalidOperationException("An interrupted write was reported as committed");
                }
                catch (UniversalDataException failure) when (failure.Code == StatusCode.CommitUnknown) { }
            }
            if (await data.RunAsync(Count) != 1L)
                throw new InvalidOperationException("A database transaction survived a physical Worker exit");
            Console.WriteLine("Independent process, CommitUnknown on interruption and reopened readback passed");
        }
        finally { directory.Delete(recursive: true); }
    }

    public static Task Prepare(DataContext context, CancellationToken cancellation) =>
        context.Database("orders").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            await db.Ado.ExecuteCommandAsync("CREATE TABLE managed_sql_orders(id INTEGER NOT NULL PRIMARY KEY)");
            return await db.Ado.ExecuteCommandAsync("INSERT INTO managed_sql_orders VALUES(1)");
        }, cancellation);

    public static Task InterruptWrite(DataContext context, CancellationToken cancellation) =>
        context.Database("orders").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            db.Ado.BeginTran();
            await db.Ado.ExecuteCommandAsync("INSERT INTO managed_sql_orders VALUES(2)");
            Environment.Exit(87);
            return false;
        }, cancellation);

    public static Task<long> Count(DataContext context, CancellationToken cancellation) =>
        context.Database("orders").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            return Convert.ToInt64(await db.Ado.GetScalarAsync("SELECT COUNT(*) FROM managed_sql_orders"));
        }, cancellation);
}
