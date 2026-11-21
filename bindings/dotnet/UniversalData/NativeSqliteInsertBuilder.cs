using SqlSugar;
using System.Text;

namespace UniversalData;

internal sealed class NativeSqliteInsertBuilder : SqliteInsertBuilder
{
    internal NativeSqliteInsertBuilder(InsertBuilder source)
    {
        Context = source.Context;
        LambdaExpressions = source.LambdaExpressions;
        Builder = source.Builder;
        Parameters = source.Parameters;
        TableWithString = source.TableWithString;
        DbColumnInfoList = source.DbColumnInfoList;
        IsNoInsertNull = source.IsNoInsertNull;
        IsReturnIdentity = source.IsReturnIdentity;
        EntityInfo = source.EntityInfo;
        OracleSeqInfoList = source.OracleSeqInfoList;
        IsBlukCopy = source.IsBlukCopy;
        IsOleDb = source.IsOleDb;
        ConvertInsertReturnIdFunc = source.ConvertInsertReturnIdFunc;
        IsNoPage = source.IsNoPage;
        IsReturnPkList = source.IsReturnPkList;
        AsName = source.AsName;
        IsOffIdentity = source.IsOffIdentity;
        SerializeObjectFunc = source.SerializeObjectFunc;
        DeserializeObjectFunc = source.DeserializeObjectFunc;
        ConflictNothing = source.ConflictNothing;
    }

    public override string ToSqlString()
    {
        if (IsNoInsertNull)
            DbColumnInfoList.RemoveAll(column => column.Value is null);

        var rows = DbColumnInfoList.GroupBy(column => column.TableId).ToArray();
        if (rows.Length == 1)
            return base.ToSqlString();
        if (IsReturnIdentity)
            throw NativeAdoProvider.Unsupported("Native SQLite batch identity return is unsupported.");

        var columns = string.Join(",", rows[0].Select(column => Builder.GetTranslationColumnName(column.DbColumnName)));

        var sql = new StringBuilder();
        sql.AppendFormat(SqlTemplateBatch, GetTableNameString, columns).Append(" VALUES");
        for (var row = 0; row < rows.Length; row++)
        {
            sql.Append("(");
            sql.Append(string.Join(",", rows[row].Select((column, columnIndex) =>
                FormatBatchValue(row, columnIndex, column))));
            sql.Append(row == rows.Length - 1 ? ") " : "),  ");
        }
        sql.Append("\r\n;\r\n");

        var result = sql.ToString();
        if (MySqlIgnore)
        {
            result = result.Remove(0, "INSERT INTO `".Length)
                .Insert(0, "INSERT OR IGNORE  INTO  `");
        }
        return result;
    }

    private string FormatBatchValue(int row, int columnIndex, DbColumnInfo column)
    {
        if (column.Value is string && column.InsertServerTime == false &&
            string.IsNullOrEmpty(column.InsertSql) && column.SqlParameterDbType is null &&
            !string.Equals(column.DataType, "nvarchar2", StringComparison.Ordinal))
        {
            var name = $"{Builder.SqlParameterKeyWord}__ud_native_{row}_{columnIndex}";
            Parameters.Add(new SugarParameter(name, column.Value));
            return GetDbColumn(column, name);
        }
        return GetDbColumn(column, FormatValue(row, column.DbColumnName, column.Value));
    }
}
