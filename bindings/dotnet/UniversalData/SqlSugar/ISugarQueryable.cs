using System.Linq.Expressions;

namespace SqlSugar;

public interface ISugarQueryable<T>
{
    ISugarQueryable<T> Where(Expression<Func<T, bool>> expression);
    ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression);
    ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression, OrderByType orderByType);
    ISugarQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> expression);
    List<T> ToList();
    Task<List<T>> ToListAsync(CancellationToken cancellationToken = default);
    List<T> ToPageList(int pageNumber, int pageSize);
    IEnumerable<T> ToStream(CancellationToken cancellationToken = default);
    IEnumerable<IReadOnlyList<T>> ToBatches(int batchSize, CancellationToken cancellationToken = default);
}
