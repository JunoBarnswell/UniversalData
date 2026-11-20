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

    public int ExecuteCommand() => Execute(default);

    private int Execute(CancellationToken cancellationToken)
    {
        var selectedKeys = keys ?? mapping.Keys;
        if (selectedKeys.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no update key. Mark a member with SugarColumn(IsPrimaryKey=true) or call WhereColumns.");
        var members = mapping.Members.Where(member => (!member.IsIdentity && !member.IgnoreUpdate) || selectedKeys.Contains(member)).ToArray();
        return client.Write(SqlNative.WriteUpdate, SqlNative.LoadAuto, mapping, members, entities, selectedKeys,
            cancellationToken: cancellationToken);
    }

    public Task<int> ExecuteCommandAsync(CancellationToken cancellationToken = default)
    { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(() => Execute(cancellationToken), cancellationToken); }
}
