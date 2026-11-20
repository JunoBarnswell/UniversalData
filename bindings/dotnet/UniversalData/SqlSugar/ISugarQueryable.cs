using System.Linq.Expressions;
using UniversalData;

namespace SqlSugar;

public interface ISugarQueryable<T>
{
    ISugarQueryable<T> Where(Expression<Func<T, bool>> expression);
    ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression);
    ISugarQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> expression, OrderByType orderByType);
    ISugarQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> expression);
    Result<List<T>> ToList();
    Task<Result<List<T>>> ToListAsync(CancellationToken cancellationToken = default);
    Result<List<T>> ToPageList(int pageNumber, int pageSize);
    ValueTask<Result<SqlCursor<T>>> OpenCursorAsync(int batchSize = 0, CancellationToken cancellation = default);
}
