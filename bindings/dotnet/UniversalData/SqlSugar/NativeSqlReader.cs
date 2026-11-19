using System.Collections;
using System.Data;
using System.Text.Json;
using UniversalData;

namespace SqlSugar;

internal sealed class NativeSqlReader : IDataReader
{
    private readonly SqlSessionHandle session;
    private readonly SqlBatchHandle? schema;
    private readonly SqlBatchHandle? parameters;
    private readonly CancellationToken cancellationToken;
    private readonly CancellationTokenRegistration cancellation;
    private readonly ColumnInfo[] columns;
    private SqlBatchHandle? batch;
    private int row = -1;
    private bool closed;

    internal NativeSqlReader(SqlSessionHandle session, SqlBatchHandle? schema, SqlBatchHandle? parameters = null,
        CancellationToken cancellationToken = default)
    {
        this.session = session;
        this.schema = schema;
        this.parameters = parameters;
        this.cancellationToken = cancellationToken;
        cancellation = cancellationToken.CanBeCanceled ? cancellationToken.Register(static state => SqlNative.Cancel((SqlSessionHandle)state!), session) : default;
        columns = schema is null ? [] : ReadColumns(SqlNative.SchemaJson(schema));
    }

    public bool Read()
    {
        EnsureOpen();
        if (batch is not null && row + 1 < SqlNative.BatchRows(batch)) { row++; return true; }
        batch?.Dispose();
        batch = null;
        row = -1;
        var status = SqlNative.ReadNext(session, out batch);
        if (status == 1) return false;
        if (status == (int)StatusCode.Cancelled) throw new OperationCanceledException(cancellationToken);
        SqlNative.Check(status);
        if (batch is null) return false;
        row = 0;
        return true;
    }

    public int FieldCount => columns.Length;
    public object GetValue(int i)
    {
        EnsureRow();
        CheckOrdinal(i);
        SqlNative.Check(SqlNative.BatchGet(batch!, checked((uint)row), checked((uint)i), out var value));
        return SqlValueCodec.ReadValue(value, typeof(object), columns[i].Name) ?? DBNull.Value;
    }

    public string GetName(int i) { CheckOrdinal(i); return columns[i].Name; }
    public int GetOrdinal(string name)
    {
        for (var i = 0; i < columns.Length; i++) if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
        throw new IndexOutOfRangeException($"Column '{name}' was not returned by the Native SQL query.");
    }
    public bool IsDBNull(int i) => GetValue(i) is DBNull;
    public Type GetFieldType(int i) { CheckOrdinal(i); return columns[i].ClrType; }
    public string GetDataTypeName(int i) { CheckOrdinal(i); return columns[i].NativeType; }
    public int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++) values[i] = GetValue(i);
        return count;
    }
    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));
    public bool NextResult() => false;
    public int Depth => 0;
    public int RecordsAffected => -1;
    public bool IsClosed => closed;
    public DataTable? GetSchemaTable()
    {
        var table = new DataTable("SchemaTable");
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        for (var i = 0; i < columns.Length; i++) table.Rows.Add(columns[i].Name, i, columns[i].ClrType, columns[i].Nullable);
        return table;
    }
    public bool GetBoolean(int i) => (bool)GetValue(i);
    public byte GetByte(int i) => Convert.ToByte(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        var bytes = (byte[])GetValue(i);
        if (buffer is null) return bytes.Length;
        var count = Math.Min(length, bytes.Length - checked((int)fieldOffset));
        if (count > 0) Array.Copy(bytes, fieldOffset, buffer, bufferoffset, count);
        return count;
    }
    public char GetChar(int i) => Convert.ToChar(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        var chars = Convert.ToString(GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!.ToCharArray();
        if (buffer is null) return chars.Length;
        var count = Math.Min(length, chars.Length - checked((int)fieldoffset));
        if (count > 0) Array.Copy(chars, fieldoffset, buffer, bufferoffset, count);
        return count;
    }
    public Guid GetGuid(int i) => (Guid)SqlValueCodec.ConvertTo(GetValue(i), typeof(Guid), GetName(i))!;
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public string GetString(int i) => Convert.ToString(GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!;
    public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
    public DateTime GetDateTime(int i) => SqlValueCodec.ConvertTo(GetValue(i), typeof(DateTime), GetName(i)) is DateTime value ? value : throw new InvalidCastException();
    public IDataReader GetData(int i) => throw new NotSupportedException("Nested Native SQL values are not IDataReader instances.");
    public IEnumerator GetEnumerator()
    {
        while (Read()) yield return this;
    }

    public void Close() => Dispose();
    public void Dispose()
    {
        if (closed) return;
        closed = true;
        batch?.Dispose();
        batch = null;
        try { SqlNative.Check(SqlNative.ReadClose(session)); }
        finally { cancellation.Dispose(); schema?.Dispose(); parameters?.Dispose(); }
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(closed, this);
    private void EnsureRow() { EnsureOpen(); if (row < 0 || batch is null) throw new InvalidOperationException("The reader is not positioned on a row."); }
    private void CheckOrdinal(int ordinal) { EnsureOpen(); if ((uint)ordinal >= (uint)columns.Length) throw new IndexOutOfRangeException($"Column ordinal {ordinal} is outside the result."); }

    private static ColumnInfo[] ReadColumns(string schemaJson)
    {
        using var document = JsonDocument.Parse(schemaJson);
        if (!document.RootElement.TryGetProperty("fields", out var fields)) return [];
        return fields.EnumerateArray().Select(field =>
        {
            var name = field.GetProperty("name").GetString() ?? string.Empty;
            var native = field.GetProperty("type").GetString() ?? string.Empty;
            return new ColumnInfo(name, native, field.TryGetProperty("nullable", out var nullable) && nullable.GetBoolean(), ClrType(native));
        }).ToArray();
    }

    private static Type ClrType(string nativeType) => nativeType switch
    {
        "bool" => typeof(bool), "int64" => typeof(long), "uint64" => typeof(ulong), "float64" => typeof(double),
        "utf8" => typeof(string), "binary" => typeof(byte[]), "decimal128" => typeof(decimal), "date32" => typeof(DateOnly),
        "time64" => typeof(TimeOnly), "datetime" => typeof(DateTimeOffset), "timestamp" => typeof(DateTimeOffset),
        "duration" => typeof(Duration), "interval" => typeof(Interval), _ => typeof(object)
    };

    private readonly record struct ColumnInfo(string Name, string NativeType, bool Nullable, Type ClrType);
}
