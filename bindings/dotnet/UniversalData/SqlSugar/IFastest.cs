using UniversalData;
namespace SqlSugar;

public interface IFastest<T>
{
    int PageSize { get; set; }
    Result<int> BulkCopy(IEnumerable<T> entities);
    Result<int> BulkUpdate(IEnumerable<T> entities);
    Result<int> BulkDelete(IEnumerable<T> entities);
    Task<Result<int>> BulkCopyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
    Task<Result<int>> BulkUpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
    Task<Result<int>> BulkDeleteAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
}
