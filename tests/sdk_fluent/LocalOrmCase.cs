using System.Data;
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
    private const string Table = "sdk_transaction_entries";

    public static async Task Run(DataContext context, CancellationToken cancellation)
    {
        var database = context.Database("orders");
        await database.UseAsync(db => db.Ado.ExecuteCommandAsync(
            $"CREATE TABLE {Table} (owner_id INTEGER NOT NULL, entry_id INTEGER NOT NULL, label TEXT, PRIMARY KEY(owner_id, entry_id))"),
            cancellation);
        try
        {
            await database.TransactionAsync(async (transaction, token) =>
            {
                IDataReader? escapedReader = null;
                ISugarQueryable<LocalOrmRow>? escapedQuery = null;
                var branch = transaction.Database("orders");
                await branch.UseAsync(async db =>
                {
                    var fastest = db.Fastest<LocalOrmRow>().BatchSize(2);
                    int copied = await fastest.BulkCopyAsync(
                    [
                        new() { Owner = 1, Id = 1, Label = "other owner" },
                        new() { Owner = 2, Id = 1, Label = "before" },
                        new() { Owner = 2, Id = 2 }
                    ]);
                    int updated = await fastest.BulkUpdateAsync([new() { Owner = 2, Id = 1, Label = "updated" }]);
                    Require(copied == 3 && updated == 1, "Native Fastest did not preserve compound-key write counts");
                    await Reject(db.Ado.CommitTranAsync, StatusCode.Unsupported);
                    await Reject(() => db.Ado.ExecuteCommandAsync($"DROP TABLE {Table}"), StatusCode.Unsupported);
                    escapedQuery = db.Queryable<LocalOrmRow>();
                    escapedReader = await db.Ado.GetDataReaderAsync(
                        $"SELECT entry_id, label FROM {Table} WHERE owner_id = @owner ORDER BY entry_id",
                        new { owner = 2 });
                    Require(escapedReader.Read() && escapedReader.GetInt64(0) == 1 && escapedReader.GetString(1) == "updated",
                        "Parameterized ADO reader did not see the local transaction write");
                    await Reject(() => db.Ado.GetIntAsync($"SELECT COUNT(*) FROM {Table}"), StatusCode.Busy);
                }, token);
                Require(escapedReader?.IsClosed == true, "The callback left an enlisted reader open");
                try
                {
                    _ = await escapedQuery!.ToListAsync();
                    throw new InvalidOperationException("An enlisted query escaped its database callback");
                }
                catch (ObjectDisposedException) { }

                var page = await branch.UseAsync(db => db.Queryable<LocalOrmRow>().Where(row => row.Owner == 2)
                    .OrderBy(row => row.Id).ToPageListAsync(1, 2), token);
                Require(page.Count == 2 && page[0].Label == "updated" && page[1].Label is null,
                    "Entity materialization did not preserve the committed column values or NULL");
            }, cancellation);

            try
            {
                await database.TransactionAsync(async (transaction, token) =>
                {
                    await transaction.Database("orders").UseAsync(db => db.Fastest<LocalOrmRow>().BulkCopyAsync(
                        [new() { Owner = 3, Id = 1 }]), token);
                    throw new InvalidOperationException("rollback local ORM fixture");
                }, cancellation);
                throw new InvalidOperationException("A failed business callback returned normally");
            }
            catch (InvalidOperationException error) when (error.Message == "rollback local ORM fixture") { }
            var rows = await database.UseAsync(db => db.Queryable<LocalOrmRow>().OrderBy(row => new { row.Owner, row.Id })
                .ToListAsync(), cancellation);
            Require(rows.Count == 3 && rows[0].Label == "other owner" && rows[1].Label == "updated" && rows[2].Label is null,
                "The ordinary provider observed incorrect local commit or rollback");
        }
        finally { await database.UseAsync(db => db.Ado.ExecuteCommandAsync($"DROP TABLE {Table}"), cancellation); }
    }

    private static async Task Reject(Func<Task> action, StatusCode code)
    {
        try { await action(); }
        catch (UniversalDataException failure) when (failure.Code == code) { return; }
        throw new InvalidOperationException($"Expected {code} from a guarded transaction operation");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
