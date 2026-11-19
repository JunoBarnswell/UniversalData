namespace SqlSugar;

internal sealed class Fastest<T> : IFastest<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    internal Fastest(SqlSugarClient client, EntityMapping mapping) { this.client = client; this.mapping = mapping; }

    public int PageSize { get; set; } = 512;
    public int BulkCopy(IEnumerable<T> entities)
    {
        var rows = Materialize(entities);
        var members = mapping.Members.Where(member => !member.IsIdentity && !member.IgnoreInsert).ToArray();
        if (members.Length == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no insertable members.");
        return client.Write(SqlNative.WriteAppend, SqlNative.LoadAuto, mapping, members, rows, [], checked((uint)Math.Max(PageSize, 1)));
    }
    public int BulkUpdate(IEnumerable<T> entities)
    {
        var rows = Materialize(entities); var keys = mapping.Keys;
        if (keys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no update key.");
        var members = mapping.Members.Where(member => (!member.IsIdentity && !member.IgnoreUpdate) || keys.Contains(member)).ToArray();
        return client.Write(SqlNative.WriteUpdate, SqlNative.LoadAuto, mapping, members, rows, keys, checked((uint)Math.Max(PageSize, 1)));
    }
    public int BulkDelete(IEnumerable<T> entities)
    {
        var rows = Materialize(entities); var keys = mapping.Keys;
        if (keys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no delete key.");
        return client.Write(SqlNative.WriteDelete, SqlNative.LoadAuto, mapping, keys, rows, keys, checked((uint)Math.Max(PageSize, 1)));
    }
    public Task<int> BulkCopyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => BulkCopy(entities), cancellationToken);
    public Task<int> BulkUpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => BulkUpdate(entities), cancellationToken);
    public Task<int> BulkDeleteAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default) => RunAsync(() => BulkDelete(entities), cancellationToken);

    private static List<T> Materialize(IEnumerable<T> entities)
    { ArgumentNullException.ThrowIfNull(entities); return entities is List<T> list ? list : entities.ToList(); }
    private static Task<int> RunAsync(Func<int> operation, CancellationToken cancellationToken)
    { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(operation, cancellationToken); }
}
