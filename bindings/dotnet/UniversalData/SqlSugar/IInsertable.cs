using UniversalData;
namespace SqlSugar;

public interface IInsertable<T>
{
    Result<int> ExecuteCommand();
    Task<Result<int>> ExecuteCommandAsync(CancellationToken cancellationToken = default);
}
