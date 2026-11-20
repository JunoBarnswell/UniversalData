using UniversalData;
namespace SqlSugar;

internal sealed class Insertable<T> : IInsertable<T>
{
    private readonly SqlSugarClient client;
    private readonly EntityMapping mapping;
    private readonly IEnumerable<T> entities;

    internal Insertable(SqlSugarClient client, IEnumerable<T> entities)
    { this.client = client; mapping = EntityMapping.For<T>(); this.entities = entities ?? throw new ArgumentNullException(nameof(entities)); }

    public Result<int> ExecuteCommand() => SqlResult.Run(() => Execute(default));

    private int Execute(CancellationToken cancellationToken)
    {
        var members = mapping.InsertMembers;
        if (members.Count == 0) throw new InvalidOperationException($"Entity '{mapping.EntityType.FullName}' has no insertable members.");
        return client.Write(SqlNative.WriteAppend, SqlNative.LoadParameters, mapping, members, entities, [],
            cancellationToken: cancellationToken);
    }

    public Task<Result<int>> ExecuteCommandAsync(CancellationToken cancellationToken = default) =>
        Task.Run(() => SqlResult.Run(() => Execute(cancellationToken)));
}
