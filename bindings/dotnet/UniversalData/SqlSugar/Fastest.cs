using UniversalData;

namespace SqlSugar;

internal sealed class Fastest<T> : IFastest<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    internal Fastest(SqlSugarClient client, EntityMapping mapping) { this.client = client; this.mapping = mapping; }

    public int PageSize { get; set; } = 512;
    public Result<int> BulkCopy(IEnumerable<T> entities) => SqlResult.Run(() => Copy(entities, default));
    public Result<int> BulkUpdate(IEnumerable<T> entities) => SqlResult.Run(() => Update(entities, default));
    public Result<int> BulkDelete(IEnumerable<T> entities) => SqlResult.Run(() => Delete(entities, default));

    private int Copy(IEnumerable<T> entities, CancellationToken cancellationToken)
    {
        var members = mapping.InsertMembers;
        if (members.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no insertable members.");
        return client.Write(SqlNative.WriteAppend, SqlNative.LoadAuto, mapping, members, entities, [],
            checked((uint)Math.Max(PageSize, 1)), cancellationToken);
    }

    private int Update(IEnumerable<T> entities, CancellationToken cancellationToken)
    {
        var keys = mapping.Keys;
        if (keys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no update key.");
        var members = mapping.Members.Where(member => (!member.IsIdentity && !member.IgnoreUpdate) || keys.Contains(member)).ToArray();
        return client.Write(SqlNative.WriteUpdate, SqlNative.LoadAuto, mapping, members, entities, keys,
            checked((uint)Math.Max(PageSize, 1)), cancellationToken);
    }

    private int Delete(IEnumerable<T> entities, CancellationToken cancellationToken)
    {
        var keys = mapping.Keys;
        if (keys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no delete key.");
        return client.Write(SqlNative.WriteDelete, SqlNative.LoadAuto, mapping, keys, entities, keys,
            checked((uint)Math.Max(PageSize, 1)), cancellationToken);
    }
    public Task<Result<int>> BulkCopyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => Copy(entities, cancellationToken));
    public Task<Result<int>> BulkUpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => Update(entities, cancellationToken));
    public Task<Result<int>> BulkDeleteAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => Delete(entities, cancellationToken));

    private static Task<Result<int>> RunAsync(Func<int> operation) =>
        Task.Run(() => SqlResult.Run(operation));
}
