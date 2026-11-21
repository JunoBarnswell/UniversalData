using System.Text;
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
        await PrepareAsync(client, fixture, cancellationToken);

        var flow = DataJob.Define("mariadb-orders-" + fixture.RunId)
            .FromTable<EtlSourceRow>(Source, fixture.SourceTable)
            .Select(source => new EtlTargetRow
            {
                RunId = source.RunId, Id = source.Id, Name = source.Name.Trim(), Amount = source.Amount + 1.25m
            })
            .ToTable(Source, fixture.TargetTable)
            .UpsertBy(row => row.Id);

        var preview = DataJob.Define("mariadb-preview-" + fixture.RunId)
            .FromTable<EtlSourceRow>(Source, fixture.SourceTable, stream: true)
            .Return();
        var previewRows = 0;
        await foreach (var _ in client.RunAsync(preview, cancellationToken))
        {
            if (++previewRows == 10) break;
        }
        if (previewRows == 0)
            throw new InvalidOperationException("Native ETL source preview returned no sample.");
        await VerifyEmptyAsync(client, fixture, cancellationToken);

        var result = await client.RunAsync(flow, cancellationToken);
        var metrics = result.Metrics;
        if (metrics.CommittedRows != RowCount || metrics.RejectedRows != 0)
            throw new InvalidOperationException($"Native ETL committed {metrics.CommittedRows} rows and rejected {metrics.RejectedRows}.");

        var count = await VerifyAsync(client, fixture, cancellationToken);
        Console.WriteLine($"Execution {result.ExecutionId}: {count} rows committed and verified for run {fixture.RunId}.");
        Console.WriteLine($"Target table {fixture.TargetTable}");
    }

    internal static async Task<Health> CheckAsync(DataClient client, CancellationToken cancellationToken = default)
    {
        var query = DataJob.Define("mariadb-health")
            .FromSql<Health>(Source, "SELECT 1 AS Ping, VERSION() AS Version", stream: true)
            .Return();
        var rows = new List<Health>();
        await foreach (var row in client.RunAsync(query, cancellationToken))
            rows.Add(row);
        return rows.Count == 1 ? rows[0] : throw new InvalidOperationException("MariaDB health query returned an unexpected row count.");
    }

    private static async Task PrepareAsync(DataClient client, Fixture fixture, CancellationToken cancellationToken)
    {
        var identity = DataJob.Define("mariadb-database-" + fixture.RunId)
            .FromSql<Health>(Source, "SELECT 1 AS Ping, DATABASE() AS Version", stream: true)
            .Return();
        var databaseRows = new List<Health>();
        await foreach (var row in client.RunAsync(identity, cancellationToken))
            databaseRows.Add(row);
        if (databaseRows.Count != 1 || !string.Equals(databaseRows[0].Version, Database, StringComparison.Ordinal))
            throw new InvalidOperationException($"The ETL example requires the dedicated database '{Database}'.");

        var prepare = DataJob.Define("mariadb-prepare-" + fixture.RunId)
            .ExecuteSql(Source, $"""
                CREATE TABLE `{fixture.SourceTable}` (
                    RunId VARCHAR(32) NOT NULL,
                    Id INT NOT NULL,
                    Name VARCHAR(80) NOT NULL,
                    Amount DECIMAL(38,28) NOT NULL,
                    PRIMARY KEY (Id)
                ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
                """)
            .ExecuteSql(Source, $"""
                CREATE TABLE `{fixture.TargetTable}` (
                    RunId VARCHAR(32) NOT NULL,
                    Id INT NOT NULL,
                    Name VARCHAR(80) NOT NULL,
                    Amount DECIMAL(38,28) NOT NULL,
                    PRIMARY KEY (Id)
                ) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4
                """);
        foreach (string insert in InsertStatements(fixture))
            prepare = prepare.ExecuteSql(Source, insert);
        await client.RunAsync(prepare, cancellationToken);
    }

    private static IEnumerable<string> InsertStatements(Fixture fixture)
    {
        const int chunk = 128;
        for (int first = 1; first <= RowCount; first += chunk)
        {
            int last = Math.Min(RowCount, first + chunk - 1);
            var sql = new StringBuilder($"INSERT INTO `{fixture.SourceTable}` (RunId,Id,Name,Amount) VALUES ");
            for (int id = first; id <= last; id++)
            {
                if (id != first) sql.Append(',');
                string name = $"  订单-{id:D4}-🚚  ";
                sql.Append("('").Append(fixture.RunId).Append("',")
                    .Append(id).Append(',')
                    .Append(SqlLiteral(name)).Append(',')
                    .Append((id / 100m).ToString(System.Globalization.CultureInfo.InvariantCulture)).Append(')');
            }
            yield return sql.ToString();
        }
    }

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static async Task VerifyEmptyAsync(DataClient client, Fixture fixture, CancellationToken cancellationToken)
    {
        var query = DataJob.Define("mariadb-verify-empty-" + fixture.RunId)
            .FromSql<CountRow>(Source, $"SELECT COUNT(*) AS row_count FROM `{fixture.TargetTable}`", stream: true)
            .Return();
        CountRow? value = null;
        await foreach (var row in client.RunAsync(query, cancellationToken))
        {
            if (value is not null) throw new InvalidOperationException("Target count query returned multiple rows.");
            value = row;
        }
        if (value?.Count != 0) throw new InvalidOperationException("Preview changed the target table.");
    }

    private static async Task<int> VerifyAsync(DataClient client, Fixture fixture, CancellationToken cancellationToken)
    {
        var query = DataJob.Define("mariadb-verify-" + fixture.RunId)
            .FromSql<EtlTargetRow>(Source,
                $"SELECT RunId,Id,Name,Amount FROM `{fixture.TargetTable}` ORDER BY Id", stream: true)
            .Return();
        var rows = new List<EtlTargetRow>();
        await foreach (var row in client.RunAsync(query, cancellationToken))
            rows.Add(row);
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
    }

}
