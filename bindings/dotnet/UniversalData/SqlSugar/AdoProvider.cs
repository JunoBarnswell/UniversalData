using System.Data;

namespace SqlSugar;

internal sealed class AdoProvider : IAdo
{
    private readonly SqlSugarClient client;
    internal AdoProvider(SqlSugarClient client) => this.client = client;

    public IDataReader GetDataReader(string sql, params SugarParameter[] parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return client.OpenRawReader(sql, parameters ?? [], SqlNative.ReadDiscover);
    }

    public object? GetScalar(string sql, params SugarParameter[] parameters)
    {
        using var reader = client.OpenRawReader(sql, parameters ?? [], SqlNative.ReadScalar);
        if (!reader.Read() || reader.FieldCount == 0) return null;
        var value = reader.GetValue(0);
        return value is DBNull ? null : value;
    }

    public int ExecuteCommand(string sql, params SugarParameter[] parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return client.Execute(sql, parameters ?? []);
    }

    public List<T> SqlQuery<T>(string sql, params SugarParameter[] parameters)
    {
        using var reader = client.OpenRawReader(sql, parameters ?? [], SqlNative.ReadDiscover);
        var result = new List<T>();
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        while (reader.Read()) result.Add(materializer.Read(reader));
        return result;
    }

    public void BeginTran() => client.BeginTransaction(0);
    public void BeginTran(IsolationLevel isolationLevel) => client.BeginTransaction(ToNativeIsolation(isolationLevel));
    public void CommitTran() => client.CommitTransaction();
    public void RollbackTran() => client.RollbackTransaction();

    private static int ToNativeIsolation(IsolationLevel isolationLevel) => isolationLevel switch
    {
        IsolationLevel.RepeatableRead => 2,
        IsolationLevel.Serializable => 3,
        IsolationLevel.Snapshot => 4,
        _ => 1
    };
}
