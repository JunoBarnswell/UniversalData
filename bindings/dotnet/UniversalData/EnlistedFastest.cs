using System.Data;
using System.Linq.Expressions;
using SqlSugar;

namespace UniversalData;

internal sealed class EnlistedFastest<T>(EnlistedAdoProvider participant) : IFastest<T> where T : class, new()
{
    private string? tableName;
    private int batchRows = 256;

    public IFastest<T> AS(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        this.tableName = tableName;
        return this;
    }

    public IFastest<T> PageSize(int Size) => BatchSize(Size);
    public IFastest<T> BatchSize(int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        batchRows = Math.Min(4096, batchSize);
        return this;
    }

    public int BulkCopy(List<T> datas)
    {
        ArgumentNullException.ThrowIfNull(datas);
        var mapping = GetMapping(insert: true);
        var columns = mapping.Members.Where(member => !member.IsIdentity && !member.IgnoreInsert).ToArray();
        if (columns.Length == 0)
            throw new ArgumentException("The entity has no writable columns.", nameof(datas));
        return participant.Write(mapping.Table, columns, [], datas, batchRows, SqlNative.WriteAppend);
    }

    public Task<int> BulkCopyAsync(List<T> datas) => Task.Run(() => BulkCopy(datas));

    public int BulkUpdate(List<T> datas) => Update(datas, null, null);
    public int BulkUpdate(List<T> datas, string[] whereColumns) => Update(datas, whereColumns, null);
    public int BulkUpdate(List<T> datas, string[] whereColumns, string[] updateColumns) => Update(datas, whereColumns, updateColumns);
    public Task<int> BulkUpdateAsync(List<T> datas) => Task.Run(() => BulkUpdate(datas));
    public Task<int> BulkUpdateAsync(List<T> datas, string[] whereColumns) => Task.Run(() => BulkUpdate(datas, whereColumns));
    public Task<int> BulkUpdateAsync(List<T> datas, string[] whereColumns, string[] updateColumns) =>
        Task.Run(() => BulkUpdate(datas, whereColumns, updateColumns));

    private int Update(List<T> rows, string[]? whereColumns, string[]? updateColumns)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var mapping = GetMapping(insert: false);
        var keys = whereColumns is null ? mapping.Members.Where(member => member.IsPrimaryKey).ToArray() :
            Resolve(mapping.Members, whereColumns);
        if (keys.Length == 0) throw new ArgumentException("A native batch update requires keys.", nameof(whereColumns));
        var writable = mapping.Members.Where(member => !member.IsIdentity && !member.IsPrimaryKey &&
            !member.IgnoreUpdate && !keys.Contains(member)).ToArray();
        var changes = updateColumns is null ? writable : Resolve(writable, updateColumns);
        if (changes.Length == 0) throw new ArgumentException("A native batch update requires writable columns.", nameof(updateColumns));
        var columns = keys.Concat(changes).ToArray();
        var keyOrdinals = Enumerable.Range(0, keys.Length).Select(index => (uint)index).ToArray();
        return participant.Write(mapping.Table, columns, keyOrdinals, rows, batchRows, SqlNative.WriteUpdate);
    }

    private (string Table, EntityMember[] Members) GetMapping(bool insert)
    {
        var context = participant.Context;
        context.InitMappingInfo<T>();
        var entity = context.EntityMaintenance.GetEntityInfo<T>();
        var shape = EntityMapping.For<T>().Members.ToDictionary(member => member.Property.Name, StringComparer.Ordinal);
        var members = new List<EntityMember>(shape.Count);
        foreach (var column in entity.Columns)
        {
            if (column.IsIgnore || context.IgnoreColumns?.Any(value => value.EntityName == entity.EntityName &&
                value.PropertyName == column.PropertyName) == true) continue;
            if (!shape.TryGetValue(column.PropertyName, out var member) || column.IsJson || column.IsArray ||
                column.IsTranscoding || column.SqlParameterDbType is not null ||
                (insert && (column.InsertServerTime || !string.IsNullOrEmpty(column.InsertSql))) ||
                (!insert && (column.UpdateServerTime || !string.IsNullOrEmpty(column.UpdateSql) ||
                    column.IsEnableUpdateVersionValidation)))
                throw EnlistedAdoProvider.Unsupported($"Bulk mapping for '{column.PropertyName}' requires provider-specific behavior.");
            members.Add(member with
            {
                ColumnName = context.EntityMaintenance.GetDbColumnName<T>(column.PropertyName),
                IsIdentity = column.IsIdentity,
                IsPrimaryKey = column.IsPrimarykey,
                IgnoreInsert = column.IsOnlyIgnoreInsert || context.IgnoreInsertColumns?.Any(value =>
                    value.EntityName == entity.EntityName && value.PropertyName == column.PropertyName) == true,
                IgnoreUpdate = column.IsOnlyIgnoreUpdate,
                Nullable = member.Nullable || column.IsNullable
            });
        }
        return (tableName ?? context.EntityMaintenance.GetTableName<T>(), members.ToArray());
    }

    private static EntityMember[] Resolve(IReadOnlyList<EntityMember> members, string[] names)
    {
        var found = new HashSet<EntityMember>();
        var columns = new EntityMember[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            var name = names[i];
            var column = members.SingleOrDefault(member => string.Equals(member.Property.Name, name, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(member.ColumnName, name, StringComparison.OrdinalIgnoreCase));
            if (column is null || !found.Add(column))
                throw new ArgumentException($"Duplicate or unavailable batch column '{name}'.", nameof(names));
            columns[i] = column;
        }
        return columns;
    }

    // Driver-specific copy, merge and split-table operations may create a new connection.
    // The transaction surface admits only typed writes that own a native participant.
    private static UniversalDataException Unsupported() => EnlistedAdoProvider.Unsupported(
        "This bulk operation cannot be applied to a native transaction participant. Use typed BulkCopy or BulkUpdate.");
    private static Task<int> UnsupportedAsync() => Task.FromException<int>(Unsupported());

    public IFastest<T> IgnoreInsertError() => throw Unsupported();
    public IFastest<T> RemoveDataCache() => throw Unsupported();
    public IFastest<T> RemoveDataCache(string cacheKey) => throw Unsupported();
    public IFastest<T> OffIdentity() => throw Unsupported();
    public IFastest<T> SetCharacterSet(string CharacterSet) => throw Unsupported();
    public IFastest<T> EnableDataAop() => throw Unsupported();
    public SplitFastest<T> SplitTable() => throw Unsupported();
    public int BulkCopy(string tableName, DataTable dataTable) => throw Unsupported();
    public int BulkCopy(DataTable dataTable) => throw Unsupported();
    public Task<int> BulkCopyAsync(string tableName, DataTable dataTable) => UnsupportedAsync();
    public Task<int> BulkCopyAsync(DataTable dataTable) => UnsupportedAsync();
    public int BulkUpdate(string tableName, DataTable dataTable, string[] whereColumns, string[] updateColumns) => throw Unsupported();
    public int BulkUpdate(DataTable dataTable, string[] whereColumns, string[] updateColumns) => throw Unsupported();
    public int BulkUpdate(DataTable dataTable, string[] whereColumns) => throw Unsupported();
    public Task<int> BulkUpdateAsync(string tableName, DataTable dataTable, string[] whereColumns, string[] updateColumns) => UnsupportedAsync();
    public Task<int> BulkUpdateAsync(DataTable dataTable, string[] whereColumns) => UnsupportedAsync();
    public Task<int> BulkMergeAsync(List<T> datas) => UnsupportedAsync();
    public int BulkMerge(List<T> datas) => throw Unsupported();
    public int BulkMerge(DataTable dataTable, string[] whereColumns, bool isIdentity) => throw Unsupported();
    public Task<int> BulkMergeAsync(DataTable dataTable, string[] whereColumns, bool isIdentity) => UnsupportedAsync();
    public int BulkMerge(DataTable dataTable, string[] whereColumns, string[] updateColumns, bool isIdentity) => throw Unsupported();
    public Task<int> BulkMergeAsync(DataTable dataTable, string[] whereColumns, string[] updateColumns, bool isIdentity) => UnsupportedAsync();
    public Task<int> BulkMergeAsync(List<T> datas, string[] whereColumns) => UnsupportedAsync();
    public int BulkMerge(List<T> datas, string[] whereColumns) => throw Unsupported();
    public Task<int> BulkMergeAsync(List<T> datas, string[] whereColumns, string[] updateColumns) => UnsupportedAsync();
    public int BulkMerge(List<T> datas, string[] whereColumns, string[] updateColumns) => throw Unsupported();
    public Task<int> BulkMergeAsync(List<T> datas, Expression<Func<T, object>> whereColumnsExp, Expression<Func<T, object>> updateColumnsExp) => UnsupportedAsync();
    public int BulkMerge(List<T> datas, Expression<Func<T, object>> whereColumnsExp, Expression<Func<T, object>> updateColumnsExp) => throw Unsupported();
}
