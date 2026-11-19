using System.Linq.Expressions;

namespace SqlSugar;

public interface IDeleteable<T>
{
    IDeleteable<T> WhereColumns(params Expression<Func<T, object>>[] columns);
    int ExecuteCommand();
    Task<int> ExecuteCommandAsync(CancellationToken cancellationToken = default);
}
