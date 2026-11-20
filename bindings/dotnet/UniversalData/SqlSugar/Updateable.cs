using UniversalData;
using System.Linq.Expressions;

namespace SqlSugar;

internal sealed class Updateable<T> : IUpdateable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly IEnumerable<T> entities;
    private IReadOnlyList<EntityMember>? keys;

    internal Updateable(SqlSugarClient client, IEnumerable<T> entities)
    { this.client = client; mapping = EntityMapping.For<T>(); this.entities = entities ?? throw new ArgumentNullException(nameof(entities)); }

    public IUpdateable<T> WhereColumns(params Expression<Func<T, object>>[] columns)
    { keys = WriteKeyResolver.Resolve(mapping, columns ?? []); return this; }

    public Result<int> ExecuteCommand() => SqlResult.Run(() => Execute(default));

    private int Execute(CancellationToken cancellationToken)
    {
        var selectedKeys = keys ?? mapping.Keys;
        if (selectedKeys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no update key. Mark a member with SugarColumn(IsPrimaryKey=true) or call WhereColumns.");
        var members = mapping.Members.Where(member => (!member.IsIdentity && !member.IgnoreUpdate) || selectedKeys.Contains(member)).ToArray();
        return client.Write(SqlNative.WriteUpdate, SqlNative.LoadAuto, mapping, members, entities, selectedKeys,
            cancellationToken: cancellationToken);
    }

    public Task<Result<int>> ExecuteCommandAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() => SqlResult.Run(() => Execute(cancellationToken)));
}
