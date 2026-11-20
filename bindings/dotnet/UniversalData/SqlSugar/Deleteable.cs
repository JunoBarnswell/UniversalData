using UniversalData;
using System.Linq.Expressions;

namespace SqlSugar;

internal sealed class Deleteable<T> : IDeleteable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly IEnumerable<T> entities;
    private IReadOnlyList<EntityMember>? keys;

    internal Deleteable(SqlSugarClient client, IEnumerable<T> entities)
    { this.client = client; mapping = EntityMapping.For<T>(); this.entities = entities ?? throw new ArgumentNullException(nameof(entities)); }

    public IDeleteable<T> WhereColumns(params Expression<Func<T, object>>[] columns)
    { keys = WriteKeyResolver.Resolve(mapping, columns ?? []); return this; }

    public Result<int> ExecuteCommand() => SqlResult.Run(() => Execute(default));

    private int Execute(CancellationToken cancellationToken)
    {
        var selectedKeys = keys ?? mapping.Keys;
        if (selectedKeys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no delete key.");
        return client.Write(SqlNative.WriteDelete, SqlNative.LoadAuto, mapping, selectedKeys, entities, selectedKeys,
            cancellationToken: cancellationToken);
    }

    public Task<Result<int>> ExecuteCommandAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() => SqlResult.Run(() => Execute(cancellationToken)));
}
