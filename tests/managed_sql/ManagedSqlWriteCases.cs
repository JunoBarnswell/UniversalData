using SqlSugar;
using UniversalData;
using static ManagedSqlAcceptance.SqlAcceptance;

namespace ManagedSqlAcceptance;

internal static class ManagedSqlWriteCases
{
    internal static void Run(SqlSessionHandle client, ManagedSqlOptions options, int remaining)
    {
        VerifyRollback(client, options, remaining);
        VerifyCompositeKeys(client, options);
        VerifyExactDecimal(client, options);
        VerifyWholeDecimal(client, options);
        var numeric = options.Provider == DataSourceKind.Oracle
            ? "SELECT CAST(@value AS NUMBER(10,2)) FROM dual"
            : "SELECT CAST(@value AS DECIMAL(10,2))";
        var value = new SugarParameter("@value", 19.25m, System.Data.DbType.Decimal) { Precision = 10, Scale = 2 };
        Require(Scalar<decimal>(client, numeric, value) == 19.25m, "Parameterized decimal retained its scale");
    }

    private static void VerifyExactDecimal(SqlSessionHandle client, ManagedSqlOptions options)
    {
        const decimal amount = -1.2345678901234567890123456789m;
        var id = options.RowCount + 100;
        var sql = $"SELECT {options.QuotedColumns()} FROM {options.QuotedTable()} WHERE {options.QuotedColumn("Id")}={id}";
        var rows = new[] { new ManagedSqlOrder { Id = id, Name = "exact-decimal", Amount = amount } };
        Require(WriteRows(client, SqlNative.WriteAppend, rows, 1) == 1, "Insert a signed decimal");
        try
        {
            Require(ReadBatches<ManagedSqlOrder>(client, sql, 1).First().Single().Amount == amount,
                "Read all 28 digits of a negative decimal");
            rows[0].Amount = decimal.MaxValue;
            ExpectWriteFailure(() => WriteRows(client, SqlNative.WriteUpdate, rows, 1), StatusCode.Overflow);
            Require(ReadBatches<ManagedSqlOrder>(client, sql, 1).First().Single().Amount == amount,
                "A decimal overflow preserved the previous value");
        }
        finally { WriteRows(client, SqlNative.WriteDelete, rows, 1); }
    }

    private static void VerifyWholeDecimal(SqlSessionHandle client, ManagedSqlOptions options)
    {
        var table = options.QuotedColumn("managed_sql_whole_decimal");
        var id = options.QuotedColumn("Id");
        var amount = options.QuotedColumn("Amount");
        var dataType = options.Provider switch
        {
            DataSourceKind.Oracle => "NUMBER(38,0)",
            DataSourceKind.Sqlite => "TEXT",
            _ => "DECIMAL(38,0)"
        };
        Execute(client, $"CREATE TABLE {table} ({id} INTEGER NOT NULL PRIMARY KEY, {amount} {dataType} NOT NULL)");
        try
        {
            var rows = new[] { new ManagedSqlWholeDecimal { Id = 1, Amount = decimal.MaxValue } };
            Require(WriteRows(client, SqlNative.WriteAppend, rows, 1) == 1, "Insert the largest whole System.Decimal");
            Require(ReadBatches<ManagedSqlWholeDecimal>(client, $"SELECT {id},{amount} FROM {table}", 1)
                .First().Single().Amount == decimal.MaxValue, "Declared zero scale preserves the whole-number range");
        }
        finally { Execute(client, "DROP TABLE " + table); }
    }

    private static void VerifyRollback(SqlSessionHandle client, ManagedSqlOptions options, int expected)
    {
        var size = options.BatchSize;
        var rows = Enumerable.Range(options.RowCount + 150, Math.Max(5, size + 1))
            .Select(id => new ManagedSqlOrder { Id = id, Name = "first-chunk", Amount = 1m })
            .Append(new ManagedSqlOrder { Id = 1, Name = "duplicate", Amount = 1m }).ToArray();
        var countSql = "SELECT COUNT(*) FROM " + options.QuotedTable();
        Require(WriteRows<ManagedSqlOrder>(client, SqlNative.WriteAppend, [], size) == 0, "Empty batch");
        ExpectWriteFailure(() => WriteRows(client, SqlNative.WriteAppend, rows, size));
        Count("A later chunk failure must roll back earlier chunks");
        Transaction(client, SqlNative.TransactionBegin);
        try
        {
            ExpectWriteFailure(() => WriteRows(client, SqlNative.WriteAppend, rows, size));
            ExpectWriteFailure(() => Transaction(client, SqlNative.TransactionCommit), StatusCode.Busy);
        }
        finally { Transaction(client, SqlNative.TransactionRollback); }
        Count("Explicit rollback preserved the original rows");
        var enumerations = 0;
        ExpectWriteFailure(() => WriteRows(client, SqlNative.WriteAppend, FaultingRows(), size), StatusCode.ManagedComponent);
        Require(enumerations == 1, "Failed input enumerated once");
        Count("A failed input rolled back successful earlier chunks");
        using var cancel = new CancellationTokenSource();
        ExpectWriteFailure(() => WriteRows(client, SqlNative.WriteAppend, CancelDuringInput(), size, cancel.Token),
            StatusCode.Cancelled);
        Count("Cancellation during enumeration rolled back successful earlier chunks");
        var streamed = rows.Take(5);
        var updated = streamed.Select(row => new ManagedSqlOrder { Id = row.Id, Name = "changed", Amount = 2m });
        Require(WriteRows(client, SqlNative.WriteAppend, streamed, 2, bulk: true) == 5 &&
                WriteRows(client, SqlNative.WriteUpdate, updated, 2, bulk: true) == 5 &&
                WriteRows(client, SqlNative.WriteDelete, streamed, 2, bulk: true) == 5,
            "Native batch insert, update and delete reported all rows across three batches");
        Count("Native batch delete removed all added rows");
        if (options.Provider == DataSourceKind.Oracle)
        {
            Transaction(client, SqlNative.TransactionBegin, isolation: 3);
            try
            {
                Require(WriteRows(client, SqlNative.WriteAppend, rows.Take(2), 2, bulk: true) == 2,
                    "Bulk insert used array DML in a serializable Oracle transaction");
            }
            finally { Transaction(client, SqlNative.TransactionRollback); }
            Count("Serializable transaction rolled back");
        }

        void Count(string message) => Require(Scalar<int>(client, countSql) == expected, message);

        IEnumerable<ManagedSqlOrder> FaultingRows()
        {
            enumerations++;
            foreach (var row in rows.Take(size + 1)) yield return row;
            throw new IOException("Input interrupted");
        }

        IEnumerable<ManagedSqlOrder> CancelDuringInput()
        {
            foreach (var row in rows.Take(size + 1)) yield return row;
            cancel.Cancel();
        }
    }

    private static void VerifyCompositeKeys(SqlSessionHandle client, ManagedSqlOptions options)
    {
        var table = options.QuotedColumn("managed_sql_composite");
        var group = options.QuotedColumn("GroupId");
        var id = options.QuotedColumn("Id");
        var label = options.QuotedColumn("Label");
        var textType = options.Provider switch
        {
            DataSourceKind.SqlServer => "NVARCHAR(120)",
            DataSourceKind.Oracle => "NVARCHAR2(120)",
            _ => "VARCHAR(120)"
        };
        var select = $"SELECT {group},{id},{label} FROM {table}";
        Execute(client, $"CREATE TABLE {table} ({group} INTEGER NOT NULL, {id} INTEGER NOT NULL, {label} {textType}, PRIMARY KEY ({group},{id}))");
        try
        {
            var rows = new[]
            {
                new ManagedSqlCompositeRow { GroupId = 1, Id = 1, Label = "初始" },
                new ManagedSqlCompositeRow { GroupId = 2, Id = 1, Label = null }
            };
            Require(WriteRows(client, SqlNative.WriteAppend, rows, 2) == 2, "Insert distinct compound keys");
            rows[0].Label = "修改";
            Require(WriteRows(client, SqlNative.WriteUpdate, rows.Take(1), 2) == 1, "Update one compound key");
            var read = ReadBatches<ManagedSqlCompositeRow>(client,
                $"{select} WHERE {id}=1 AND ({label} IS NULL OR {group}=1) ORDER BY {group},{id}", 2).First();
            Require(read is [{ Label: "修改" }, { Label: null }], "Stable composite ordering, Unicode and null predicate");
            Require(WriteRows(client, SqlNative.WriteDelete, rows.Take(1), 2) == 1, "Delete one compound key");
            Require(ReadBatches<ManagedSqlCompositeRow>(client, select, 2).First() is [{ GroupId: 2 }],
                "The other key remained");
        }
        finally { Execute(client, "DROP TABLE " + table); }
    }

    private static void ExpectWriteFailure(Action execute, StatusCode? code = null)
    {
        try { execute(); }
        catch (UniversalDataException error) when (code is null || error.Code == code) { return; }
        throw new InvalidOperationException("The expected Native write failure was not reported");
    }
}
