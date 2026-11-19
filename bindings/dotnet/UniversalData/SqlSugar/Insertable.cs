namespace SqlSugar;

internal sealed class Insertable<T> : IInsertable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly IReadOnlyList<T> entities;

    internal Insertable(SqlSugarClient client, IReadOnlyList<T> entities)
    { this.client = client; mapping = EntityMapping.For<T>(); this.entities = entities; }

    public int ExecuteCommand()
    {
        var members = mapping.Members.Where(member => !member.IsIdentity && !member.IgnoreInsert).ToArray();
        if (members.Length == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no insertable members.");
        return client.Write(SqlNative.WriteAppend, SqlNative.LoadAuto, mapping, members, entities, []);
    }

    public Task<int> ExecuteCommandAsync(CancellationToken cancellationToken = default)
    { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(ExecuteCommand, cancellationToken); }
}
