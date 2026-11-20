using SqlSugar;
using UniversalData;

public static class MariaDbEtlDemo
{
    private const string Database = "ud_nuget_demo";
    private const string Source = "demo";
    private const int RowCount = 1_003;

    public sealed record Fixture(Guid Run)
    {
        internal string RunId => Run.ToString("N");
        internal string SourceTable => "ud_etl_source_" + RunId;
        internal string TargetTable => "ud_etl_target_" + RunId;
    }
    public sealed record Health(long Ping, string Version);

    internal static async Task RunAsync(DataClient client, CancellationToken cancellationToken = default)
    {
        var fixture = new Fixture(Guid.NewGuid());
        await client.RunAsync(PrepareAsync, fixture, cancellationToken);

        var flow = client.Etl("mariadb-orders-" + fixture.RunId)
            .FromTable<EtlSourceRow>(Source, fixture.SourceTable)
            .Map(Transform)
            .ToTable(Source, fixture.TargetTable)
            .UpsertBy(row => row.Id);

        var preview = await flow.PreviewAsync(sourceRows: 10, cancellationToken);
        if (preview.GetProperty("result").GetInt32() != (int)StatusCode.Ok ||
            preview.GetProperty("snapshot").GetProperty("preview").GetArrayLength() == 0)
            throw new InvalidOperationException("Native ETL source preview returned no sample.");
        await client.RunAsync(VerifyEmptyAsync, fixture, cancellationToken);

        await using var run = await flow.StartAsync(cancellationToken);
        var metrics = await run.WaitAsync(cancellationToken);
        if (metrics.CommittedRows != RowCount || metrics.RejectedRows != 0)
            throw new InvalidOperationException($"Native ETL committed {metrics.CommittedRows} rows and rejected {metrics.RejectedRows}.");

        var count = await client.RunAsync(VerifyAsync, fixture, cancellationToken);
        Console.WriteLine($"Worker {run.WorkerProcessId}: {count} rows committed and verified for run {fixture.RunId}.");
        Console.WriteLine($"Target table {fixture.TargetTable}");
    }

    public static Task<Health> CheckAsync(DataContext context, CancellationToken cancellationToken) =>
        context.Database(Source).UseAsync(async db =>
        {
            var ping = Convert.ToInt64(await db.Ado.GetScalarAsync("SELECT 1"));
            var version = Convert.ToString(await db.Ado.GetScalarAsync("SELECT VERSION()")) ?? "";
            return new Health(ping, version);
        }, cancellationToken);

    public static Task PrepareAsync(DataContext context, Fixture fixture, CancellationToken cancellationToken) =>
        context.Database(Source).UseAsync(async db =>
        {
            var database = Convert.ToString(await db.Ado.GetScalarAsync("SELECT DATABASE()"));
            if (!string.Equals(database, Database, StringComparison.Ordinal))
                throw new InvalidOperationException($"The ETL example requires the dedicated database '{Database}'.");

            foreach (var table in new[] { fixture.SourceTable, fixture.TargetTable })
            {
                await db.Ado.ExecuteCommandAsync($"""
                    CREATE TABLE `{table}` (
                        RunId VARCHAR(32) NOT NULL,
                        Id INT NOT NULL,
                        Name VARCHAR(80) NOT NULL,
                        Amount DECIMAL(38,28) NOT NULL,
                        PRIMARY KEY (Id)
                    ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
                    """);
            }
            var source = Enumerable.Range(1, RowCount).Select(id => new EtlSourceRow
            {
                RunId = fixture.RunId,
                Id = id,
                Name = $"  订单-{id:D4}-🚚  ",
                Amount = id / 100m
            }).ToArray();
            int written = await db.Insertable(source).AS(fixture.SourceTable).ExecuteCommandAsync();
            if (written != RowCount) throw new InvalidOperationException($"Seeded {written} rows. Expected {RowCount}.");
        }, cancellationToken);

    public static Task VerifyEmptyAsync(DataContext context, Fixture fixture, CancellationToken cancellationToken) =>
        context.Database(Source).UseAsync(async db =>
        {
            if (await db.Queryable<EtlTargetRow>().AS(fixture.TargetTable).CountAsync() != 0)
                throw new InvalidOperationException("Preview changed the target table.");
        }, cancellationToken);

    public static EtlTargetRow Transform(EtlSourceRow source) => new()
    {
        RunId = source.RunId,
        Id = source.Id,
        Name = source.Name.Trim(),
        Amount = source.Amount + 1.25m
    };

    public static Task<int> VerifyAsync(DataContext context, Fixture fixture, CancellationToken cancellationToken) =>
        context.Database(Source).UseAsync(async db =>
        {
            var rows = await db.Queryable<EtlTargetRow>().AS(fixture.TargetTable).OrderBy(row => row.Id)
                .ToListAsync(cancellationToken);
            if (rows.Count != RowCount)
                throw new InvalidOperationException($"Read back {rows.Count} rows. Expected {RowCount}.");

            for (var index = 0; index < rows.Count; index++)
            {
                var id = index + 1;
                var row = rows[index];
                if (row.RunId != fixture.RunId || row.Id != id || row.Name != $"订单-{id:D4}-🚚" ||
                    row.Amount != (id + 125) / 100m)
                    throw new InvalidOperationException($"Target row {id} differs from the fixture.");
            }
            return rows.Count;
        }, cancellationToken);
}
