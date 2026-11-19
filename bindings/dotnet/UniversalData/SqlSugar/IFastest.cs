namespace SqlSugar;

public interface IFastest<T>
{
    int PageSize { get; set; }
    int BulkCopy(IEnumerable<T> entities);
    int BulkUpdate(IEnumerable<T> entities);
    int BulkDelete(IEnumerable<T> entities);
    Task<int> BulkCopyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
    Task<int> BulkUpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
    Task<int> BulkDeleteAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);
}
