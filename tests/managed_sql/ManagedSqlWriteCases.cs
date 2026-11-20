using System.Globalization;
using SqlSugar;
using UniversalData;

namespace ManagedSqlAcceptance;

internal static class ManagedSqlWriteCases
{
    internal static void Run(SqlSugarClient client, ManagedSqlOptions options, int remaining)
    {
        VerifyRollback(client, options, remaining);
        VerifyCompositeKeys(client, options);
        VerifyExactDecimal(client, options.RowCount + 100);
        VerifyWholeDecimal(client, options);
        var numeric = options.Provider == DbType.Oracle
            ? "SELECT CAST(@value AS NUMBER(10,2)) FROM dual"
            : "SELECT CAST(@value AS DECIMAL(10,2))";
        var answer = client.Ado.GetScalar(numeric, new SugarParameter("@value", 19.25m, System.Data.DbType.Decimal)
        {
            Precision = 10,
            Scale = 2
        }).Require();
        Require(Convert.ToDecimal(answer, CultureInfo.InvariantCulture) == 19.25m,
            "Parameterized decimal retained its scale");
    }

    private static void VerifyExactDecimal(SqlSugarClient client, int id)
    {
        const decimal amount = -1.2345678901234567890123456789m;
        var row = new ManagedSqlOrder { Id = id, Name = "exact-decimal", Amount = amount };
        Require(client.Insertable(row).ExecuteCommand().Require() == 1, "Insert an exact signed decimal");
        try
        {
            var read = client.Queryable<ManagedSqlOrder>().Where(order => order.Id == id).ToList().Require().Single();
            Require(read.Amount == amount, "Read all 28 digits of a negative decimal");
            row.Amount = decimal.MaxValue;
            Require(client.Updateable(row).ExecuteCommand().Code == StatusCode.Overflow,
                "A decimal larger than the native coefficient returned Overflow");
            read = client.Queryable<ManagedSqlOrder>().Where(order => order.Id == id).ToList().Require().Single();
            Require(read.Amount == amount, "A decimal overflow did not change the stored value");
        }
        finally { client.Deleteable(row).ExecuteCommand().Require(); }
    }

    private static void VerifyWholeDecimal(SqlSugarClient client, ManagedSqlOptions options)
    {
        var table = options.QuotedColumn("managed_sql_whole_decimal");
        var id = options.QuotedColumn("Id");
        var amount = options.QuotedColumn("Amount");
        var dataType = options.Provider switch
        {
            DbType.Oracle => "NUMBER(38,0)",
            DbType.Sqlite => "TEXT",
            _ => "DECIMAL(38,0)"
        };
        client.Ado.ExecuteCommand($"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {amount} {dataType} NOT NULL)").Require();
        try
        {
            var row = new ManagedSqlWholeDecimal { Id = 1, Amount = decimal.MaxValue };
            Require(client.Insertable(row).ExecuteCommand().Require() == 1, "Insert the largest whole System.Decimal");
            Require(client.Queryable<ManagedSqlWholeDecimal>().ToList().Require().Single().Amount == decimal.MaxValue,
                "Declared zero decimal digits allowed the full whole-number range");
        }
        finally { client.Ado.ExecuteCommand("DROP TABLE " + table).Require(); }
    }

    private static void VerifyRollback(SqlSugarClient client, ManagedSqlOptions options, int expected)
    {
        var rows = Enumerable.Range(options.RowCount + 150, options.BatchSize + 1)
            .Select(id => new ManagedSqlOrder { Id = id, Name = "first-chunk", Amount = 1m }).ToList();
        rows.Add(new ManagedSqlOrder { Id = 1, Name = "duplicate", Amount = 1m });
        var countSql = "SELECT COUNT(*) FROM " + options.QuotedTable();
        Require(client.Insertable(new List<ManagedSqlOrder>()).ExecuteCommand().Require() == 0, "Empty batch");
        ExpectWriteFailure(() => client.Insertable(rows).ExecuteCommand().Require());
        Require(Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
            "An error in a later chunk must roll back earlier chunks");
        client.Ado.BeginTran().Require();
        try
        {
            ExpectWriteFailure(() => client.Insertable(rows).ExecuteCommand().Require());
            try
            {
                client.Ado.CommitTran().Require();
                throw new InvalidOperationException("The failed transaction unexpectedly committed.");
            }
            catch (UniversalDataException error) when (error.Code == StatusCode.Busy) { }
        }
        finally { client.Ado.RollbackTran().Require(); }
        Require(Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
            "The explicit transaction was rolled back");
        var enumerations = 0;
        var sequence = client.Insertable<ManagedSqlOrder>(FaultingRows());
        Require(enumerations == 0, "Building a command leaves its input lazy");
        var interrupted = sequence.ExecuteCommand();
        Require(interrupted.Code == StatusCode.ManagedComponent && interrupted.Error?.Source == ErrorSource.Component,
            "A faulting sequence returned a managed component error");
        Require(enumerations == 1 &&
            Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
            "A failed input ended once and rolled back successful earlier chunks");
        using var cancel = new CancellationTokenSource();
        var cancelled = client.Insertable<ManagedSqlOrder>(CancelDuringInput()).ExecuteCommandAsync(cancel.Token)
            .GetAwaiter().GetResult();
        Require(cancelled.Code == StatusCode.Cancelled, "The Worker confirmed rollback after input cancellation");
        Require(Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
            "Cancellation during enumeration rolled back successful earlier chunks");
        var fastest = client.Fastest<ManagedSqlOrder>();
        fastest.PageSize = 2;
        var streamed = rows.Take(5);
        var updated = streamed.Select(row => new ManagedSqlOrder { Id = row.Id, Name = "changed", Amount = 2m });
        Require(fastest.BulkCopy(streamed).Require() == 5 && fastest.BulkUpdate(updated).Require() == 5 &&
                fastest.BulkDelete(streamed).Require() == 5,
            "Lazy Fastest insert, update and delete reported all rows across three batches");
        Require(Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
            "Fastest deleted every added row");
        if (options.Provider == DbType.Oracle)
        {
            client.Ado.BeginTran(System.Data.IsolationLevel.Serializable).Require();
            try
            {
                Require(fastest.BulkCopy(rows.Take(2)).Require() == 2,
                    "BulkCopy used array DML inside a serializable Oracle transaction");
            }
            finally { client.Ado.RollbackTran().Require(); }
            Require(Convert.ToInt32(client.Ado.GetScalar(countSql).Require(), CultureInfo.InvariantCulture) == expected,
                "The serializable transaction was rolled back");
        }

        IEnumerable<ManagedSqlOrder> FaultingRows()
        {
            enumerations++;
            foreach (var row in rows.Take(options.BatchSize + 1)) yield return row;
            throw new IOException("Input interrupted");
        }

        IEnumerable<ManagedSqlOrder> CancelDuringInput()
        {
            foreach (var row in rows.Take(options.BatchSize + 1)) yield return row;
            cancel.Cancel();
        }
    }

    private static void VerifyCompositeKeys(SqlSugarClient client, ManagedSqlOptions options)
    {
        var table = options.QuotedColumn("managed_sql_composite");
        var group = options.QuotedColumn("GroupId");
        var id = options.QuotedColumn("Id");
        var label = options.QuotedColumn("Label");
        var textType = options.Provider switch
        {
            DbType.SqlServer => "NVARCHAR(120)",
            DbType.Oracle => "NVARCHAR2(120)",
            _ => "VARCHAR(120)"
        };
        client.Ado.ExecuteCommand($"CREATE TABLE {table} ({group} INTEGER NOT NULL, {id} INTEGER NOT NULL, {label} {textType}, PRIMARY KEY ({group},{id}))").Require();
        try
        {
            var rows = new[]
            {
                new ManagedSqlCompositeRow { GroupId = 1, Id = 1, Label = "初始" },
                new ManagedSqlCompositeRow { GroupId = 2, Id = 1, Label = null }
            };
            Require(client.Insertable(rows).ExecuteCommand().Require() == 2, "Insert distinct compound keys");
            rows[0].Label = "修改";
            Require(client.Updateable(rows[0]).ExecuteCommand().Require() == 1, "Update one compound key");
            var read = client.Queryable<ManagedSqlCompositeRow>()
                .Where(row => row.Id == 1 && (row.Label == null || row.GroupId == 1))
                .ToPageList(1, 2).Require();
            Require(read.Count == 2 && read[0].Label == "修改" && read[1].Label is null,
                "Stable composite-key ordering, Unicode and null predicate");
            Require(client.Deleteable(rows[0]).ExecuteCommand().Require() == 1, "Delete one compound key");
            var after = client.Queryable<ManagedSqlCompositeRow>().ToList().Require();
            Require(after.Count == 1 && after[0].GroupId == 2, "The other key remained");
        }
        finally { client.Ado.ExecuteCommand("DROP TABLE " + table).Require(); }
    }

    private static void ExpectWriteFailure(Action execute)
    {
        try { execute(); }
        catch (UniversalDataException) { return; }
        throw new InvalidOperationException("Writing a duplicate key did not fail.");
    }

    private static void Require(bool answer, string name)
    {
        if (!answer)
            throw new InvalidOperationException($"Managed SQL write validation failed. {name}");
    }
}
