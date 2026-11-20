using System.Globalization;
using System.Runtime.InteropServices;
using SqlSugar;

namespace UniversalData;

internal static class SqlBatchBuilder
{
    internal static IntPtr BuildManaged<T>(IntPtr context, IntPtr metadata,
        IReadOnlyList<(EntityMember Member, int? Precision, int? Scale)> columns,
        IEnumerable<T> entities, uint maxRows)
    {
        var known = entities is IReadOnlyCollection<T> collection ? collection.Count
            : entities.TryGetNonEnumeratedCount(out var count) ? count : -1;
        if (known >= 0 && (uint)known > maxRows)
            throw new ArgumentException("Managed output exceeds the bound batch size.", nameof(entities));
        var batch = IntPtr.Zero;
        try
        {
            using var rows = entities.GetEnumerator();
            if (!rows.MoveNext())
                throw new ArgumentException("A managed output batch cannot be empty.", nameof(entities));
            using var values = new PinnedValues(columns.Count);
            SqlNative.Check(SqlNative.BatchCreateLikeRaw(context, metadata,
                known > 0 ? (uint)known : maxRows, out batch));
            uint written = 0;
            do
            {
                if (written++ == maxRows)
                    throw new ArgumentException("Managed output exceeds the bound batch size.", nameof(entities));
                var entity = rows.Current;
                for (var i = 0; i < columns.Count; i++)
                {
                    var (member, precision, scale) = columns[i];
                    values.Set(i, member, member.Get(entity!), precision, scale);
                }
                values.Append(batch);
            } while (rows.MoveNext());
            return batch;
        }
        catch { SqlNative.DestroyBatchRaw(batch); throw; }
    }

    internal static SqlBatchHandle BuildChunk<T>(IntPtr context, SqlBatchHandle schema,
        IReadOnlyList<EntityMember> members, IEnumerator<T> rows, PinnedValues values,
        uint capacity, CancellationToken cancellationToken, out bool hasMore)
    {
        var createStatus = SqlNative.BatchCreateLike(context, schema.DangerousGetHandle(), capacity, out var batch);
        try { SqlNative.Check(createStatus); }
        catch { batch?.Dispose(); throw; }
        try
        {
            uint written = 0;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                var entity = rows.Current;
                for (var i = 0; i < members.Count; i++) values.Set(i, members[i], members[i].Get(entity!));
                values.Append(batch.DangerousGetHandle());
                written++;
                hasMore = rows.MoveNext();
            } while (written < capacity && hasMore);
            cancellationToken.ThrowIfCancellationRequested();
            SqlNative.Check(SqlNative.BatchSeal(batch));
            return batch;
        }
        catch
        {
            batch.Dispose();
            throw;
        }
    }

    internal static SqlBatchHandle BuildParameters(IntPtr context, IReadOnlyList<SugarParameter> parameters)
    {
        if (parameters.Count == 0) throw new ArgumentException("No parameters were supplied.", nameof(parameters));
        var members = new List<EntityMember>(parameters.Count);
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < parameters.Count; i++)
        {
            var parameter = parameters[i];
            var name = parameter.ParameterName.TrimStart('@', ':', '$');
            if (name.Length == 0 || !names.Add(name)) throw new ArgumentException($"Parameter '{parameter.ParameterName}' is empty or duplicated.", nameof(parameters));
            var type = ParameterType(parameter);
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);
            if (!SqlValueCodec.TryGetNativeType(type, out var nativeType))
                throw new InvalidOperationException($"Parameter '{parameter.ParameterName}' has unsupported type '{type.FullName}'.");
            int scale = parameter.Precision != 0 || parameter.Scale != 0 ? parameter.Scale
                        : parameter.Value is decimal number ? (decimal.GetBits(number)[3] >> 16) & 0xff
                                                            : 0;
            members.Add(new EntityMember
            {
                Property = typeof(ParameterValue).GetProperty(nameof(ParameterValue.Value))!,
                ColumnName = name,
                NativeType = nativeType,
                ValueType = type,
                Nullable = true,
                IsPrimaryKey = false,
                IsIdentity = false,
                IgnoreInsert = false,
                IgnoreUpdate = false,
                Precision = parameter.Precision == 0 ? 38 : parameter.Precision,
                Scale = scale
            });
        }
        using var schema = SqlNative.SchemaFromJson(context, EntityMapping.FormatSchema(members));
        SqlNative.Check(SqlNative.BatchCreateLike(context, schema.DangerousGetHandle(), 1, out var batch));
        try
        {
            using var values = new PinnedValues(parameters.Count);
            for (var i = 0; i < parameters.Count; i++)
                values.Set(i, members[i], parameters[i].Value is DBNull ? null : parameters[i].Value);
            values.Append(batch.DangerousGetHandle());
            SqlNative.Check(SqlNative.BatchSeal(batch));
            return batch;
        }
        catch { batch.Dispose(); throw; }
    }

    private static Type ParameterType(SugarParameter parameter) => parameter.DbType switch
    {
        System.Data.DbType.Boolean => typeof(bool),
        System.Data.DbType.SByte or System.Data.DbType.Int16 or
            System.Data.DbType.Int32 or System.Data.DbType.Int64 => typeof(long),
        System.Data.DbType.Byte or System.Data.DbType.UInt16 or
            System.Data.DbType.UInt32 or System.Data.DbType.UInt64 => typeof(ulong),
        System.Data.DbType.Single or System.Data.DbType.Double => typeof(double),
        System.Data.DbType.Decimal or System.Data.DbType.Currency or System.Data.DbType.VarNumeric => typeof(decimal),
        System.Data.DbType.AnsiString or System.Data.DbType.AnsiStringFixedLength or
            System.Data.DbType.String or System.Data.DbType.StringFixedLength or System.Data.DbType.Xml => typeof(string),
        System.Data.DbType.Guid => typeof(Guid),
        System.Data.DbType.Binary => typeof(byte[]),
        System.Data.DbType.Date => typeof(DateOnly),
        System.Data.DbType.Time => typeof(TimeOnly),
        System.Data.DbType.DateTime or System.Data.DbType.DateTime2 => typeof(DateTime),
        System.Data.DbType.DateTimeOffset => typeof(DateTimeOffset),
        System.Data.DbType.Object => parameter.Value is null or DBNull ? typeof(string) : parameter.Value.GetType(),
        _ => throw new NotSupportedException($"Native SQL parameter type '{parameter.DbType}' is unsupported.")
    };

    private static Native.ScalarValue CreateValue(int index, EntityMember member, object? value,
        PinnedValues buffers, int? precision, int? scale)
    {
        var result = new Native.ScalarValue { Type = (int)member.NativeType, IsNull = value is null ? 1 : 0 };
        if (value is null) return result;
        var type = Nullable.GetUnderlyingType(member.ValueType) ?? member.ValueType;
        if (type.IsEnum) value = Convert.ChangeType(value, Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture);
        switch (member.NativeType)
        {
            case DataType.Bool: result.Unsigned = Convert.ToBoolean(value, CultureInfo.InvariantCulture) ? 1UL : 0UL; break;
            case DataType.Int64: result.Unsigned = unchecked((ulong)Convert.ToInt64(value, CultureInfo.InvariantCulture)); break;
            case DataType.UInt64: result.Unsigned = Convert.ToUInt64(value, CultureInfo.InvariantCulture); break;
            case DataType.Float64: result.Real = Convert.ToDouble(value, CultureInfo.InvariantCulture); break;
            case DataType.Decimal128:
                SqlValueCodec.WriteDecimal(ref result, Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                    member.ColumnName, precision ?? member.Precision, scale ?? member.Scale);
                break;
            case DataType.Utf8:
                var text = value is Guid guid ? guid.ToString("D") : Convert.ToString(value, CultureInfo.InvariantCulture)!;
                result.Text = buffers.Encode(index, text, out result.TextLength);
                break;
            case DataType.Binary:
                var binary = (byte[])value;
                result.Text = buffers.PinBinary(index, binary);
                result.TextLength = checked((uint)binary.Length);
                break;
            case DataType.Date32:
                var date = value is DateOnly dateOnly ? dateOnly : DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture));
                result.Unsigned = unchecked((ulong)date.DayNumber - (ulong)DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber); break;
            case DataType.Time64:
                var time = value is TimeOnly timeOnly ? timeOnly.ToTimeSpan() : (TimeSpan)value;
                result.Unsigned = checked((ulong)(time.Ticks / 10)); break;
            case DataType.DateTime:
                if (value is DateTimeOffset offset) result.DateTime = new Native.DateTimeValue { Ticks = offset.DateTime.Ticks, HasOffset = 1, OffsetMinutes = (int)offset.Offset.TotalMinutes };
                else { var dateTime = (DateTime)value; result.DateTime = new Native.DateTimeValue { Ticks = dateTime.Ticks, HasOffset = dateTime.Kind == DateTimeKind.Utc ? 1 : 0, OffsetMinutes = dateTime.Kind == DateTimeKind.Utc ? 0 : 0 }; }
                break;
            case DataType.Duration:
                result.Unsigned = unchecked((ulong)(value is Duration duration ? duration.Microseconds : ((TimeSpan)value).Ticks / 10)); break;
            case DataType.Interval:
                var interval = (Interval)value; result.Interval = new Native.IntervalValue { Months = interval.Months, Days = interval.Days, Microseconds = interval.Microseconds }; break;
            default: throw new NotSupportedException($"Native SQL write type {member.NativeType} is not supported.");
        }
        return result;
    }

    private sealed class ParameterValue
    {
        public object? Value { get; set; }
    }

    internal sealed class PinnedValues : IDisposable
    {
        private static readonly byte[] EmptyPayload = new byte[1];
        private readonly Native.ScalarValue[] values;
        private readonly byte[]?[] textBuffers;
        private readonly GCHandle[] payloadPins;
        private GCHandle valuePin;

        internal PinnedValues(int count)
        {
            values = new Native.ScalarValue[count];
            textBuffers = new byte[count][];
            payloadPins = new GCHandle[count];
            valuePin = GCHandle.Alloc(values, GCHandleType.Pinned);
        }

        internal void Set(int index, EntityMember member, object? value, int? precision = null, int? scale = null)
        {
            if (member.NativeType == DataType.Binary && payloadPins[index].IsAllocated)
                payloadPins[index].Free();
            values[index] = CreateValue(index, member, value, this, precision, scale);
        }

        internal IntPtr Encode(int index, string text, out uint length)
        {
            int bytes = Utf8.ByteCount(text);
            var buffer = textBuffers[index];
            if (buffer is null || buffer.Length < bytes)
            {
                buffer = new byte[Math.Max(bytes, 64)];
                if (payloadPins[index].IsAllocated)
                    payloadPins[index].Free();
                payloadPins[index] = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                textBuffers[index] = buffer;
            }
            Utf8.Encode(text, buffer);
            length = checked((uint)bytes);
            return payloadPins[index].AddrOfPinnedObject();
        }

        internal IntPtr PinBinary(int index, byte[] bytes)
        {
            payloadPins[index] = GCHandle.Alloc(bytes.Length == 0 ? EmptyPayload : bytes, GCHandleType.Pinned);
            return payloadPins[index].AddrOfPinnedObject();
        }

        internal void Append(IntPtr batch) =>
            SqlNative.Check(SqlNative.BatchAppendRaw(batch, valuePin.AddrOfPinnedObject(), checked((uint)values.Length)));

        public void Dispose()
        {
            foreach (ref var pin in payloadPins.AsSpan())
                if (pin.IsAllocated) pin.Free();
            if (valuePin.IsAllocated) valuePin.Free();
        }
    }
}
