using System.Text.Json;
using System.Security.Cryptography;

namespace UniversalData;

/// <summary>Native Batch in the owning activation's memory quota. Borrowed input expires at callback return.</summary>
public sealed class ManagedBatch : IDisposable
{
    private IntPtr pointer; private readonly IntPtr memory; private readonly CallbackLifetime lifetime; private readonly bool owned;
    internal ManagedBatch(IntPtr context, IntPtr value, CallbackLifetime validity, bool ownership)
    { memory = context; pointer = value; lifetime = validity; owned = ownership; if (owned) lifetime.Own(this); }
    internal IntPtr Pointer { get { lifetime.Open(); ObjectDisposedException.ThrowIf(pointer == IntPtr.Zero, this); return pointer; } }
    internal IntPtr Memory => memory;
    public uint Rows => Native.ud_batch_rows_raw(Pointer);
    public uint Columns => Native.ud_batch_columns_raw(Pointer);
    public string SchemaJson { get { Check(Native.ud_schema_to_json_raw(Pointer, out var text, out var length)); return DataOwnership.Text(text, length); } }
    public string DataJson { get { Check(Native.ud_batch_to_json_raw(Pointer, out var text, out var length)); return DataOwnership.Text(text, length); } }
    public string RowIdentity(uint row) { Check(Native.ud_batch_row_info_json_raw(Pointer, row, out var text, out var length)); return DataOwnership.Text(text, length); }
    public void CopyRowIdentity(uint row, ManagedBatch source, uint sourceRow) {
        if (!owned || source.Memory != memory) throw new ArgumentException("Identity needs an owned Batch in the same execution");
        Check(Native.ud_batch_row_info_raw(source.Pointer, sourceRow, out var identity));
        Check(Native.ud_batch_set_row_info_raw(Pointer, row, ref identity));
    }
    public void CopyRowIdentitiesFrom(ManagedBatch source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (!owned || source.Memory != memory || Rows != source.Rows)
            throw new ArgumentException("Identities need equally sized Batches in the same execution");
        Check(Native.ud_batch_copy_row_info_range_raw(Pointer, source.Pointer));
    }
    internal bool HasSchemaFingerprint(byte[] expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        if (expected.Length != 32) throw new ArgumentException("A Native schema fingerprint must be 32 bytes", nameof(expected));
        var actual = new byte[32];
        Check(Native.ud_schema_fingerprint_raw(Pointer, actual));
        return CryptographicOperations.FixedTimeEquals(actual, expected);
    }
    public bool IsPresent(uint row, uint column) { Check(Native.ud_batch_cell_present_raw(Pointer, row, column, out var present)); return present != 0; }
    public ulong? GetUInt64(uint row, uint column) { var value = Scalar(row, column, DataType.UInt64); return value.IsNull != 0 ? null : value.Unsigned; }
    public long? GetInt64(uint row, uint column) { var value = Scalar(row, column, DataType.Int64); return value.IsNull != 0 ? null : unchecked((long)value.Unsigned); }
    public long? GetDuration(uint row, uint column) { var value = Scalar(row, column, DataType.Duration); return value.IsNull != 0 ? null : unchecked((long)value.Unsigned); }
    public Interval? GetInterval(uint row, uint column) { var value = Scalar(row, column, DataType.Interval); return value.IsNull != 0 ? null : new(value.Interval.Months, value.Interval.Days, value.Interval.Microseconds); }
    private Native.ScalarValue Scalar(uint row, uint column, DataType type) {
        Check(Native.ud_batch_get_raw(Pointer, row, column, out var value));
        if (value.Type != (int)type || value.IsAbsent != 0) throw new ArgumentException("Field is absent or has another type"); return value;
    }
    private static void Check(int status) { if (status != 0) throw new UniversalDataException(status, "Native Batch operation failed"); }
    public void Dispose() { if (pointer == IntPtr.Zero) return; if (owned) { lifetime.Open(); Native.ud_batch_destroy(pointer); lifetime.Release(this); } pointer = IntPtr.Zero; }

    /// <summary>Map Native columnar rows to strong entities without a JSON row round-trip.</summary>
    public IReadOnlyList<T> ToEntities<T>() => EnumerateEntities<T>().ToArray();

    internal static byte[] SchemaFingerprint(string schemaJson) => SHA256.HashData(Utf8.Bytes(schemaJson));

    internal static IReadOnlyList<(EntityMember Member, uint Ordinal)> BuildEntityProjection<T>(string schemaJson)
    {
        var mapping = EntityMapping.For<T>();
        using var schema = JsonDocument.Parse(schemaJson);
        var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var index = 0;
        foreach (var field in schema.RootElement.GetProperty("fields").EnumerateArray())
            ordinals.Add(field.GetProperty("name").GetString()!, index++);
        var selected = new List<(EntityMember Member, uint Ordinal)>();
        foreach (var member in mapping.Members)
            if (ordinals.TryGetValue(member.ColumnName, out var ordinal))
                selected.Add((member, checked((uint)ordinal)));
        return selected;
    }

    internal static IReadOnlyList<(EntityMember Member, int? Precision, int? Scale)> BuildOutputProjection<T>(string schemaJson)
    {
        var members = EntityMapping.For<T>().Members.ToDictionary(member => member.ColumnName,
            StringComparer.OrdinalIgnoreCase);
        using var shape = JsonDocument.Parse(schemaJson);
        var projection = new List<(EntityMember Member, int? Precision, int? Scale)>();
        foreach (var field in shape.RootElement.GetProperty("fields").EnumerateArray())
        {
            var name = field.GetProperty("name").GetString()!;
            var type = field.GetProperty("type").GetString();
            if (!members.TryGetValue(name, out var member))
                throw new InvalidOperationException($"Managed output column '{name}' has no member on '{typeof(T).FullName}'.");
            if (type != SqlValueCodec.NativeTypeName(member.NativeType))
                throw new InvalidOperationException($"Managed output column '{name}' expects '{type}', but '{member.Property.Name}' uses '{member.ValueType.FullName}'.");
            var numeric = member.NativeType == DataType.Decimal128;
            projection.Add((member, numeric ? field.GetProperty("precision").GetInt32() : null,
                numeric ? field.GetProperty("scale").GetInt32() : null));
        }
        return projection;
    }

    internal IEnumerable<T> EnumerateEntities<T>() => EnumerateEntities<T>(BuildEntityProjection<T>(SchemaJson));

    internal IEnumerable<T> EnumerateEntities<T>(IReadOnlyList<(EntityMember Member, uint Ordinal)> selected)
    {
        var count = Rows;
        var input = Pointer;
        var factory = new EntityRowFactory<T>(selected);
        for (uint row = 0; row < count; row++)
        {
            for (int index = 0; index < selected.Count; index++)
            {
                var (member, ordinal) = selected[index];
                Check(Native.ud_batch_get_raw(input, row, ordinal, out var value));
                factory.Values[index] = SqlValueCodec.ReadValue(value, member.ValueType, member.ColumnName);
            }
            yield return factory.Materialize();
        }
    }
}
