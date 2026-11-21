using System.Globalization;
using System.Linq.Expressions;
using UniversalData;

namespace FluentAcceptance;

internal static class LinqTableCase
{
    private static readonly (long Id, long Shop, long? Score, string? Payload)[] Orders =
    [
        (19, 7, null, "tie"), (7, 7, null, "tie"), (9, 9, 3, "other shop"),
        (10, 7, 3, "tie"), (6, 7, 3, "tie"), (3, 7, 5, "first"),
        (2, 7, null, "tie"), (11, 7, -1, "filtered"), (14, 4, 99, "filtered"),
    ];

    internal static async Task RunAsync(DataClient client, AtomicSource source, string table, CancellationToken cancellation)
    {
        bool oracle = source.Kind == DataSourceKind.Oracle;
        string number = oracle ? "NUMBER(18,0)" : "BIGINT";
        string text = oracle ? "VARCHAR2(25)" : "VARCHAR(25)";
        string id = oracle ? "\"entry_id\"" : "entry_id";
        string shop = oracle ? "\"shop_id\"" : "shop_id";
        string score = oracle ? "\"score\"" : "score";
        string payload = oracle ? "\"payload\"" : "payload";
        await Execute(client, source, $"CREATE TABLE {table} ({id} {number} NOT NULL PRIMARY KEY, " +
            $"{shop} {number} NOT NULL, {score} {number}, {payload} {text})", cancellation);
        try
        {
            string from = oracle ? " FROM DUAL" : "";
            string nullScore = source.Kind == DataSourceKind.PostgreSql ? "CAST(NULL AS BIGINT)" : "NULL";
            var rows = Orders.Select(row => $"SELECT {row.Id}, {row.Shop}, " +
                $"{row.Score?.ToString(CultureInfo.InvariantCulture) ?? nullScore}, '{row.Payload}'{from}");
            await Execute(client, source, $"INSERT INTO {table} " + string.Join(" UNION ALL ", rows), cancellation);
            if (oracle)
                await Check<decimal>(client, source, table,
                    (row, input) => row.Shop >= input.Shop && !(row.Score < input.MinimumScore),
                    (row, input) => !(row.Score == input.MinimumScore),
                    (offset, size) => new(7m, 2m, offset, size), cancellation);
            else
                await Check<long>(client, source, table,
                    (row, input) => row.Shop >= input.Shop && !(row.Score < input.MinimumScore),
                    (row, input) => !(row.Score == input.MinimumScore),
                    (offset, size) => new(7L, 2L, offset, size), cancellation);

            if (source.Kind == DataSourceKind.Sqlite)
                await FinitePage(client, source, table, cancellation);
        }
        finally
        {
            await Execute(client, source, $"DROP TABLE {table}", cancellation);
        }
    }

    private static async Task Check<T>(DataClient client, AtomicSource source, string table,
        Expression<Func<LinqOrder<T>, LinqPageRequest<T>, bool>> predicate,
        Expression<Func<LinqOrder<T>, LinqPageRequest<T>, bool>> unequal,
        Func<int, int, LinqPageRequest<T>> request, CancellationToken cancellation) where T : struct
    {
        const string step = "read-page";
        var tableQuery = DataJob.Define<LinqPageRequest<T>>("linq-table")
            .QueryTable<LinqOrder<T>>(source.Name, table, step);
        var query = tableQuery.Where(predicate)
            .OrderByDescending(row => row.Score)
            .ThenBy(row => row.Shop)
            .Skip(input => input.Offset)
            .Take(input => input.Size)
            .Select(row => new LinqPage<T>(row.Id, row.Score)).Return();

        long[][] expectedPages = [[3, 6], [10, 9], [2, 7], [19], []];
        for (int index = 0; index < expectedPages.Length; index++)
        {
            await using var run = await client.StartAsync(query, request(index * 2, 2), cancellation);
            var rows = await run.ReadRowsAsync<LinqPage<T>>(cancellation).ToListAsync(cancellation);
            var metrics = await run.WaitAsync(cancellation);
            var expected = expectedPages[index];
            Require(rows.Select(row => Convert.ToInt64(row.Id, CultureInfo.InvariantCulture)).SequenceEqual(expected),
                source, "physical primary key must break score and shop ties across adjacent pages");
            Require(rows.Select(row => row.Score is T score ? Convert.ToInt64(score, CultureInfo.InvariantCulture) : (long?)null)
                .SequenceEqual(expected.Select(id => Orders.Single(row => row.Id == id).Score)),
                source, "projected aliases must preserve values and nulls");
            Require(metrics.InputRows == (ulong)expected.Length, source, "database must return only the current page");
            if (index == 0)
            {
                var diagnostics = await run.DiagnosticsAsync(cancellation);
                var node = diagnostics.Document.GetProperty("pipelines").EnumerateArray()
                    .SelectMany(pipeline => pipeline.GetProperty("trees").EnumerateArray())
                    .SelectMany(tree => tree.GetProperty("nodes").EnumerateArray())
                    .Single(node => node.GetProperty("id").GetString() == step);
                var facts = node.GetProperty("facts");
                var sql = facts.GetProperty("readSql").GetString()!;
                Require(facts.GetProperty("predicatePushed").GetBoolean() &&
                        facts.GetProperty("readColumns").GetInt32() == 2 &&
                        !sql.Contains("payload", StringComparison.OrdinalIgnoreCase), source,
                    "bound SQL must return only the two projected fields");
                Console.WriteLine($"{source.Kind} SQL page source rows={metrics.InputRows}, " +
                    $"typed projection=entry_alias,score_alias, statement={sql}");
            }
        }
        var zero = await client.RunAsync(query, request(0, 0), cancellation).ToListAsync(cancellation);
        Require(zero.Count == 0, source, "Take(0) must return no rows");

        var nonmatching = DataJob.Define<LinqPageRequest<T>>("linq-inequality")
            .QueryTable<LinqOrder<T>>(source.Name, table).Where(unequal).Take(20)
            .Select(row => new LinqPage<T>(row.Id, row.Score)).Return();
        var filterScore = (T)Convert.ChangeType(3, typeof(T), CultureInfo.InvariantCulture);
        var unequalRows = await client.RunAsync(nonmatching, request(0, 20) with { MinimumScore = filterScore },
            cancellation).ToListAsync(cancellation);
        Require(unequalRows.Select(row => Convert.ToInt64(row.Id, CultureInfo.InvariantCulture))
            .SequenceEqual(new long[] { 2, 3, 7, 11, 14, 19 }), source, "negated equality must include NULL rows");

        foreach (var invalid in new[] { request(-1, 2), request(0, -1), request(0, 65537) })
        {
            try
            {
                await client.RunAsync(query, invalid, cancellation).ToListAsync(cancellation);
                throw new InvalidOperationException("Invalid table page was read");
            }
            catch (UniversalDataException error)
            {
                Require(error.Code == StatusCode.Invalid &&
                    (error.Message.Contains(step, StringComparison.Ordinal) ||
                     error.Detail?.Message.Contains(step, StringComparison.Ordinal) == true), source,
                    $"Offset {invalid.Offset}, size {invalid.Size}: expected a source-step parameter error, " +
                    $"received {error.Code}: {error.Detail?.Message ?? error.Message}");
            }
        }

        try
        {
            tableQuery.Where(row => row.Payload!.StartsWith("tie")).Take(1).Return();
            throw new InvalidOperationException("Unsupported SQL expression was translated");
        }
        catch (ArgumentException error) when (error.Message.Contains("linq-table", StringComparison.Ordinal) &&
                                               error.Message.Contains(step, StringComparison.Ordinal))
        {
        }
    }

    private static async Task FinitePage(DataClient client, AtomicSource source, string table, CancellationToken cancellation)
    {
        var query = DataJob.Define<LinqPageRequest<long>>("linq-finite")
            .WithOptions(new(BatchRows: 2))
            .FromTable<LinqOrder<long>>(source.Name, table)
            .Where((row, input) => row.Shop >= input.Shop && !(row.Score < input.MinimumScore))
            .OrderByDescending(row => row.Score)
            .ThenBy(row => row.Shop)
            .ThenBy(row => row.Id)
            .Skip(1)
            .Take(5)
            .Skip(input => input.Offset)
            .Take(input => input.Size)
            .Select(row => new LinqPage<long>(row.Id, row.Score))
            .Return();
        var page = await client.RunAsync(query, new(7, 2, 1, 3), cancellation).ToListAsync(cancellation);
        Require(page.Select(row => row.Id).SequenceEqual(new long[] { 10, 9, 2 }), source,
            "Native batch ranges must compose in declaration order after mixed-direction sorting");
    }

    private static Task<DataJobResult> Execute(DataClient client, AtomicSource source, string sql, CancellationToken cancellation) =>
        client.RunAsync(DataJob.Define("linq-fixture").ExecuteSql(source.Name, sql), cancellation);

    private static void Require(bool condition, AtomicSource source, string message)
    {
        if (!condition) throw new InvalidOperationException($"{source.Kind}: {message}");
    }
}
