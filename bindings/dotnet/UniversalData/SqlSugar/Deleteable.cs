using System.Linq.Expressions;

namespace SqlSugar;

internal sealed class Deleteable<T> : IDeleteable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly IReadOnlyList<T> entities;
    private IReadOnlyList<EntityMember>? keys;

    internal Deleteable(SqlSugarClient client, IReadOnlyList<T> entities)
    { this.client = client; mapping = EntityMapping.For<T>(); this.entities = entities; }

    public IDeleteable<T> WhereColumns(params Expression<Func<T, object>>[] columns)
    { keys = WriteKeyResolver.Resolve(mapping, columns ?? []); return this; }

    public int ExecuteCommand()
    {
        var selectedKeys = keys ?? mapping.Keys;
        if (selectedKeys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no delete key.");
        return client.Write(SqlNative.WriteDelete, SqlNative.LoadAuto, mapping, selectedKeys, entities, selectedKeys);
    }

    public Task<int> ExecuteCommandAsync(CancellationToken cancellationToken = default)
    { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(ExecuteCommand, cancellationToken); }
}
