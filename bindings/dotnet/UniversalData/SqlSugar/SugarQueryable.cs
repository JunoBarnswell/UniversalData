using System.Linq.Expressions;
using UniversalData;

namespace SqlSugar;

internal sealed class SugarQueryable<T> : ISugarQueryable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly List<Expression<Func<T, bool>>> filters = [];
    private readonly List<(LambdaExpression Expression, bool Descending)> orderings = [];

    internal SugarQueryable(SqlSugarClient client, EntityMapping mapping) { this.client = client; this.mapping = mapping; }

    public ISugarQueryable<T> Where(Expression<Func<T, bool>> expression)
    { ArgumentNullException.ThrowIfNull(expression); filters.Add(expression); return this; }

    public ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression)
        => OrderBy(expression, OrderByType.Asc);

    public ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression, OrderByType orderByType)
    {
        ArgumentNullException.ThrowIfNull(expression);
        orderings.Clear();
        orderings.Add((expression, orderByType switch
        {
            OrderByType.Asc => false,
            OrderByType.Desc => true,
            _ => throw new ArgumentOutOfRangeException(nameof(orderByType))
        }));
        return this;
    }

    public ISugarQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> expression)
        => OrderBy(expression, OrderByType.Desc);

    public Result<List<T>> ToList() => SqlResult.Run(() => ReadList(null, null));

    private List<T> ReadList(int? pageNumber, int? pageSize, CancellationToken cancellation = default)
    {
        using var reader = OpenReader(cancellation, null, pageNumber, pageSize);
        var result = new List<T>();
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        while (reader.Read()) result.Add(materializer.Read(reader));
        return result;
    }

    public Task<Result<List<T>>> ToListAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() => SqlResult.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ReadList(null, null, cancellationToken);
        }));

    public Result<List<T>> ToPageList(int pageNumber, int pageSize) => SqlResult.Run(() =>
    {
        if (pageNumber <= 0 || pageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(pageNumber));
        return ReadList(pageNumber, pageSize);
    });

    public ValueTask<Result<SqlCursor<T>>> OpenCursorAsync(int batchSize = 0, CancellationToken cancellation = default) =>
        ValueTask.FromResult(SqlResult.Run(() =>
        {
            if (batchSize < 0)
                throw new ArgumentOutOfRangeException(nameof(batchSize));
            return new SqlCursor<T>(OpenReader(cancellation, batchSize == 0 ? null : (uint)batchSize));
        }));

    private NativeSqlReader OpenReader(CancellationToken cancellationToken = default, uint? batchRows = null,
        int? pageNumber = null, int? pageSize = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var plan = SqlExpression.Build(mapping, filters, orderings, pageNumber, pageSize);
        return client.OpenQuery(plan, batchRows, cancellationToken);
    }
}
