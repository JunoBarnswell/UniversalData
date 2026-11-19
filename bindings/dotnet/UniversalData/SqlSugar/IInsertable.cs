namespace SqlSugar;

public interface IInsertable<T>
{
    int ExecuteCommand();
    Task<int> ExecuteCommandAsync(CancellationToken cancellationToken = default);
}
