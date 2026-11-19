using System.Linq.Expressions;

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

    public List<T> ToList()
        => ReadList(null, null);

    private List<T> ReadList(int? pageNumber, int? pageSize)
    {
        using var reader = OpenReader(default, null, pageNumber, pageSize);
        var result = new List<T>();
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        while (reader.Read()) result.Add(materializer.Read(reader));
        return result;
    }

    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(ToList, cancellationToken); }

    public List<T> ToPageList(int pageNumber, int pageSize)
    {
        if (pageNumber <= 0) throw new ArgumentOutOfRangeException(nameof(pageNumber));
        if (pageSize <= 0) throw new ArgumentOutOfRangeException(nameof(pageSize));
        return ReadList(pageNumber, pageSize);
    }

    public IEnumerable<T> ToStream(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var reader = OpenReader(cancellationToken);
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return materializer.Read(reader);
        }
    }

    public IEnumerable<IReadOnlyList<T>> ToBatches(int batchSize, CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
        cancellationToken.ThrowIfCancellationRequested();
        using var reader = OpenReader(cancellationToken, checked((uint)batchSize));
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        var batch = new List<T>(batchSize);
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            batch.Add(materializer.Read(reader));
            if (batch.Count != batchSize) continue;
            yield return batch;
            batch = new List<T>(batchSize);
        }
        if (batch.Count != 0) yield return batch;
    }

    private NativeSqlReader OpenReader(CancellationToken cancellationToken = default, uint? batchRows = null,
        int? pageNumber = null, int? pageSize = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var plan = SqlExpression.Build(mapping, filters, orderings, pageNumber, pageSize);
        return client.OpenQuery(plan, batchRows, cancellationToken);
    }
}
