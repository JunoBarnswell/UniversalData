using System.Data;

namespace SqlSugar;

public interface IAdo
{
    IDataReader GetDataReader(string sql, params SugarParameter[] parameters);
    object? GetScalar(string sql, params SugarParameter[] parameters);
    int ExecuteCommand(string sql, params SugarParameter[] parameters);
    List<T> SqlQuery<T>(string sql, params SugarParameter[] parameters);
    void BeginTran();
    void BeginTran(IsolationLevel isolationLevel);
    void CommitTran();
    void RollbackTran();
}
