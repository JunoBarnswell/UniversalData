using System.Data;
using UniversalData;

namespace SqlSugar;

public interface IAdo
{
    ValueTask<Result<SqlCursor<T>>> OpenCursorAsync<T>(string sql, IReadOnlyList<SugarParameter>? parameters = null,
        CancellationToken cancellation = default);
    Result<object?> GetScalar(string sql, params SugarParameter[] parameters);
    Result<int> ExecuteCommand(string sql, params SugarParameter[] parameters);
    Result<List<T>> SqlQuery<T>(string sql, params SugarParameter[] parameters);
    Result BeginTran();
    Result BeginTran(IsolationLevel isolationLevel);
    Result CommitTran();
    Result RollbackTran();
}
