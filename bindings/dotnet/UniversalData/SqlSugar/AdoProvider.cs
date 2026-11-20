using System.Data;
using UniversalData;

namespace SqlSugar;

internal sealed class AdoProvider(SqlSugarClient client) : IAdo
{
    public ValueTask<Result<SqlCursor<T>>> OpenCursorAsync<T>(string sql, IReadOnlyList<SugarParameter>? parameters = null,
        CancellationToken cancellation = default) => ValueTask.FromResult(SqlResult.Run(() =>
            new SqlCursor<T>(client.OpenRawReader(sql, parameters ?? [], cancellation: cancellation))));

    public Result<object?> GetScalar(string sql, params SugarParameter[] parameters) => SqlResult.Run<object?>(() =>
    {
        using var reader = client.OpenRawReader(sql, parameters ?? [], SqlNative.ReadScalar);
        if (!reader.Read() || reader.FieldCount == 0)
            return null;
        var value = reader.GetValue(0);
        return value is DBNull ? null : value;
    });

    public Result<int> ExecuteCommand(string sql, params SugarParameter[] parameters) =>
        SqlResult.Run(() => client.Execute(sql, parameters ?? []));

    public Result<List<T>> SqlQuery<T>(string sql, params SugarParameter[] parameters) => SqlResult.Run(() =>
    {
        using var reader = client.OpenRawReader(sql, parameters ?? []);
        var rows = new List<T>();
        var materializer = EntityMaterializerPlan<T>.Create(reader);
        while (reader.Read())
            rows.Add(materializer.Read(reader));
        return rows;
    });

    public Result BeginTran() => SqlResult.Run(() => client.BeginTransaction(0));
    public Result BeginTran(IsolationLevel isolationLevel) => SqlResult.Run(() => client.BeginTransaction(isolationLevel switch
    {
        IsolationLevel.RepeatableRead => 2,
        IsolationLevel.Serializable => 3,
        IsolationLevel.Snapshot => 4,
        _ => 1
    }));
    public Result CommitTran() => SqlResult.Run(client.CommitTransaction);
    public Result RollbackTran() => SqlResult.Run(client.RollbackTransaction);
}
