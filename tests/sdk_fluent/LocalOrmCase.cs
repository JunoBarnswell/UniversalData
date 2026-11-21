using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

[SugarTable("sdk_transaction_entries")]
public sealed class LocalOrmRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "owner_id")]
    public long Owner { get; set; }

    [SugarColumn(IsPrimaryKey = true, ColumnName = "entry_id")]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label", IsNullable = true)]
    public string? Label { get; set; }
}

public static class LocalOrmCase
{
    private const string Source = "orders";
    private const string Table = "sdk_transaction_entries";

    public static async Task Run(DataClient client, CancellationToken cancellation)
    {
        await client.RunAsync(SqlJob("create", $"CREATE TABLE {Table} (owner_id INTEGER NOT NULL, " +
            $"entry_id INTEGER NOT NULL, label TEXT, PRIMARY KEY(owner_id, entry_id))"), cancellation);
        try
        {
            var visible = DataJob.Define<object>("local-orm-commit").Transaction(Source)
                .ExecuteSql(Source, $"INSERT INTO {Table} (owner_id, entry_id, label) VALUES " +
                    "(1, 1, 'other owner'), (2, 1, 'before'), (2, 2, NULL)")
                .ExecuteSql(Source, $"UPDATE {Table} SET label = 'updated' WHERE owner_id = 2 AND entry_id = 1")
                .FromSql<LocalOrmRow>(Source,
                    $"SELECT owner_id, entry_id, label FROM {Table} WHERE owner_id = 2 ORDER BY entry_id")
                .Return();
            var inside = await Read(client, visible, cancellation);
            Require(inside.Count == 2 && inside[0].Label == "updated" && inside[1].Label is null,
                "A Native transaction query did not see its own SQL writes or preserve NULL");

            var committed = await Read(client, Query("local-orm-independent", "SELECT owner_id, entry_id, label " +
                $"FROM {Table} ORDER BY owner_id, entry_id"), cancellation);
            Require(committed.Count == 3 && committed[0].Label == "other owner" &&
                committed[1].Label == "updated" && committed[2].Label is null,
                "The independent Native SQL oracle observed incorrect committed values");

            var rollback = DataJob.Define<object>("local-orm-rollback").Transaction(Source)
                .ExecuteSql(Source, $"INSERT INTO {Table} (owner_id, entry_id, label) VALUES (3, 1, 'rollback')")
                .ExecuteSql(Source, $"INSERT INTO {Table} (owner_id, entry_id, label) VALUES (2, 1, 'duplicate')");
            try
            {
                await client.RunAsync(rollback, cancellation);
                throw new InvalidOperationException("A failed Native local transaction returned normally");
            }
            catch (UniversalDataException error) when (error.Code is not (StatusCode.CommitUnknown or StatusCode.WorkerLost)) { }

            var afterRollback = await Read(client, Query("local-orm-after-rollback",
                $"SELECT owner_id, entry_id, label FROM {Table} ORDER BY owner_id, entry_id"), cancellation);
            Require(afterRollback.Count == 3 && afterRollback.All(row => row.Owner != 3),
                "The Native journal transaction did not roll back the failed SQL batch");
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(8));
            await client.RunAsync(SqlJob("drop", $"DROP TABLE {Table}"), cleanup.Token);
        }
    }

    private static DataJobQuery<object, LocalOrmRow> Query(string id, string statement) =>
        DataJob.Define<object>(id).FromSql<LocalOrmRow>(Source, statement, stream: true).Return();

    private static DataJob<object> SqlJob(string action, string statement) =>
        DataJob.Define<object>($"local-orm-{action}").ExecuteSql(Source, statement);

    private static async Task<List<LocalOrmRow>> Read(DataClient client,
        DataJobQuery<object, LocalOrmRow> query, CancellationToken cancellation)
    {
        var rows = new List<LocalOrmRow>();
        await foreach (var row in client.RunAsync(query, cancellation)) rows.Add(row);
        return rows;
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
