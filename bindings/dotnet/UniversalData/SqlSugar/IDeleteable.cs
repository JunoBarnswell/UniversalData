using UniversalData;
using System.Linq.Expressions;

namespace SqlSugar;

public interface IDeleteable<T>
{
    IDeleteable<T> WhereColumns(params Expression<Func<T, object>>[] columns);
    Result<int> ExecuteCommand();
    Task<Result<int>> ExecuteCommandAsync(CancellationToken cancellationToken = default);
}
