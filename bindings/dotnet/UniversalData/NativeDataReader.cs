using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.Json;

namespace UniversalData;

internal sealed class NativeDataReader : DbDataReader
{
    private readonly NativeAdoProvider owner;
    private readonly Column[] columns;
    private SqlBatchHandle? batch;
    private int row = -1;
    private int batchRows;
    private bool closed, finished, hasRows;

    internal NativeDataReader(NativeAdoProvider owner, string schema)
    {
        this.owner = owner;
        using var document = JsonDocument.Parse(schema);
        columns = document.RootElement.GetProperty("fields").EnumerateArray().Select(field =>
        {
            var type = field.GetProperty("type").GetString()!;
            return new Column(field.GetProperty("name").GetString()!, type, ClrType(type),
                field.TryGetProperty("nullable", out var nullable) && nullable.GetBoolean());
        }).ToArray();
    }

    public override bool Read() => ReadCore(default);
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<bool>(cancellationToken);
        try
        {
            using var lease = EnterOperation();
            if (finished) return Task.FromResult(false);
            if (batch is not null && row + 1 < batchRows)
            {
                row++;
                return Task.FromResult(true);
            }
        }
        catch (Exception error) { return Task.FromException<bool>(error); }
        return Task.Run(() => ReadCore(cancellationToken), cancellationToken);
    }

    private bool ReadCore(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var lease = EnterOperation();
        if (!HasBatch(cancellationToken)) return false;
        row++;
        return true;
    }

    private bool HasBatch(CancellationToken cancellationToken)
    {
        if (batch is not null && row + 1 < batchRows) return true;
        batch?.Dispose();
        batch = null;
        row = -1;
        if (finished) return false;
        batch = owner.ReadBatch(cancellationToken);
        if (batch is null)
        {
            finished = true;
            return false;
        }
        batchRows = checked((int)SqlNative.BatchRows(batch));
        if (batchRows == 0)
            throw new UniversalDataException((int)StatusCode.Protocol, "The native cursor returned an empty batch.");
        hasRows = true;
        return true;
    }

    public override bool HasRows
    {
        get
        {
            using var lease = EnterOperation();
            return hasRows || HasBatch(default);
        }
    }
    public override int Depth => 0;
    public override int FieldCount => columns.Length;
    public override bool IsClosed => Volatile.Read(ref closed);
    public override int RecordsAffected => -1;
    public override bool NextResult() => false;
    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));
    public override string GetName(int ordinal) => columns[ordinal].Name;
    public override Type GetFieldType(int ordinal) => columns[ordinal].Type;
    public override string GetDataTypeName(int ordinal) => owner.GetDataTypeName(columns[ordinal].NativeType);

    public override object GetValue(int ordinal) => Value(ordinal, typeof(object)) ?? DBNull.Value;
    public override T GetFieldValue<T>(int ordinal) => Value(ordinal, typeof(T)) is T value ? value :
        throw new InvalidCastException($"The transaction column '{GetName(ordinal)}' is null.");

    private object? Value(int ordinal, Type target)
    {
        using var lease = EnterOperation();
        return CurrentValue(ordinal, target);
    }

    private object? CurrentValue(int ordinal, Type target)
    {
        if (row < 0 || batch is null) throw new InvalidOperationException("The reader is not positioned on a row.");
        var name = columns[ordinal].Name;
        SqlNative.Check(SqlNative.BatchGet(batch, checked((uint)row), checked((uint)ordinal), out var value));
        return SqlValueCodec.ReadValue(value, target, name);
    }

    public override int GetOrdinal(string name)
    {
        for (var i = 0; i < columns.Length; i++)
            if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
        throw new IndexOutOfRangeException($"The native query did not return column '{name}'.");
    }

    public override int GetValues(object[] values)
    {
        using var lease = EnterOperation();
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++) values[i] = CurrentValue(i, typeof(object)) ?? DBNull.Value;
        return count;
    }

    public override bool IsDBNull(int ordinal) => Value(ordinal, typeof(object)) is null;
    public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);
    public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal), CultureInfo.InvariantCulture);
    public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);
    public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);
    public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);
    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);
    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);
    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);
    public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);
    public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);
    public override string GetString(int ordinal) => GetFieldValue<string>(ordinal);
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        Copy(GetFieldValue<byte[]>(ordinal), dataOffset, buffer, bufferOffset, length);
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
        Copy(GetString(ordinal).ToCharArray(), dataOffset, buffer, bufferOffset, length);

    private static long Copy<T>(T[] source, long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (buffer is null) return source.Length;
        ArgumentOutOfRangeException.ThrowIfNegative(dataOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var count = Math.Max(0, Math.Min(length, source.LongLength - dataOffset));
        if (count != 0) Array.Copy(source, dataOffset, buffer, bufferOffset, count);
        return count;
    }

    public override DataTable GetSchemaTable()
    {
        var schema = new DataTable("SchemaTable");
        schema.Columns.Add("ColumnName", typeof(string));
        schema.Columns.Add("ColumnOrdinal", typeof(int));
        schema.Columns.Add("DataType", typeof(Type));
        schema.Columns.Add("AllowDBNull", typeof(bool));
        for (var i = 0; i < columns.Length; i++)
            schema.Rows.Add(columns[i].Name, i, columns[i].Type, columns[i].Nullable);
        return schema;
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this);
    public override void Close() => owner.CloseReader(this);

    internal void CloseState()
    {
        if (closed) return;
        Volatile.Write(ref closed, true);
        batch?.Dispose();
        batch = null;
    }
    protected override void Dispose(bool disposing)
    {
        if (disposing) Close();
        base.Dispose(disposing);
    }

    private NativeAdoProvider.OperationLease EnterOperation()
    {
        ObjectDisposedException.ThrowIf(IsClosed, this);
        return owner.EnterOperation(this);
    }

    private static Type ClrType(string name) => name switch
    {
        "bool" => typeof(bool), "int64" => typeof(long), "uint64" => typeof(ulong), "float64" => typeof(double),
        "utf8" => typeof(string), "binary" => typeof(byte[]), "decimal128" => typeof(decimal), "date32" => typeof(DateOnly),
        "time64" => typeof(TimeOnly), "datetime" or "timestamp" => typeof(DateTimeOffset),
        "duration" => typeof(Duration), "interval" => typeof(Interval), _ => typeof(object)
    };

    private sealed record Column(string Name, string NativeType, Type Type, bool Nullable);
}
