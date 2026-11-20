using System.Globalization;
using System.Runtime.InteropServices;

namespace UniversalData;

internal static class SqlValueCodec
{
    internal static bool TryGetNativeType(Type type, out DataType nativeType)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(bool)) nativeType = DataType.Bool;
        else if (type == typeof(byte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong)) nativeType = DataType.UInt64;
        else if (type == typeof(sbyte) || type == typeof(short) || type == typeof(int) || type == typeof(long)) nativeType = DataType.Int64;
        else if (type == typeof(float) || type == typeof(double)) nativeType = DataType.Float64;
        else if (type == typeof(decimal)) nativeType = DataType.Decimal128;
        else if (type == typeof(string) || type == typeof(char) || type == typeof(Guid) || type.IsEnum) nativeType = type.IsEnum && Enum.GetUnderlyingType(type) != typeof(string) ? DataType.Int64 : DataType.Utf8;
        else if (type == typeof(byte[])) nativeType = DataType.Binary;
        else if (type == typeof(DateOnly)) nativeType = DataType.Date32;
        else if (type == typeof(TimeOnly)) nativeType = DataType.Time64;
        else if (type == typeof(DateTime) || type == typeof(DateTimeOffset)) nativeType = DataType.DateTime;
        else if (type == typeof(TimeSpan)) nativeType = DataType.Duration;
        else if (type == typeof(Duration)) nativeType = DataType.Duration;
        else if (type == typeof(Interval)) nativeType = DataType.Interval;
        else { nativeType = default; return false; }
        return true;
    }

    internal static string NativeTypeName(DataType type) => type switch
    {
        DataType.Bool => "bool",
        DataType.Int64 => "int64",
        DataType.Float64 => "float64",
        DataType.Utf8 => "utf8",
        DataType.Decimal128 => "decimal128",
        DataType.Date32 => "date32",
        DataType.Time64 => "time64",
        DataType.Timestamp => "timestamp",
        DataType.Binary => "binary",
        DataType.UInt64 => "uint64",
        DataType.Duration => "duration",
        DataType.Interval => "interval",
        DataType.DateTime => "datetime",
        _ => throw new NotSupportedException($"Native SQL mapping does not support {type}.")
    };

    internal static object? ConvertTo(object value, Type targetType, string column)
    {
        var nullable = Nullable.GetUnderlyingType(targetType);
        targetType = nullable ?? targetType;
        try
        {
            if (targetType.IsAssignableFrom(value.GetType())) return value;
            if (targetType.IsEnum) return Enum.ToObject(targetType, value);
            if (targetType == typeof(Guid)) return value is string text ? Guid.Parse(text) : new Guid((byte[])value);
            if (targetType == typeof(DateTime)) return value switch { DateTimeOffset offset => offset.DateTime, _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture) };
            if (targetType == typeof(DateTimeOffset)) return value switch { DateTime date => new DateTimeOffset(date), _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture) };
            if (targetType == typeof(DateOnly)) return value is DateTime date ? DateOnly.FromDateTime(date) : DateOnly.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            if (targetType == typeof(TimeOnly)) return value is TimeSpan span ? TimeOnly.FromTimeSpan(span) : TimeOnly.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            if (targetType == typeof(TimeSpan)) return value is Duration duration ? TimeSpan.FromTicks(duration.Microseconds * 10) : value is long microseconds ? TimeSpan.FromTicks(microseconds * 10) : (TimeSpan)value;
            if (targetType == typeof(Duration)) return value is long microseconds ? new Duration(microseconds) : (Duration)value;
            return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
        }
        catch (Exception error) when (error is InvalidCastException or FormatException or OverflowException or ArgumentException)
        { throw new InvalidOperationException($"Native SQL column '{column}' of type '{value.GetType().FullName}' cannot be converted to '{targetType.FullName}'.", error); }
    }

    internal static object? ReadValue(in Native.ScalarValue value, Type targetType, string column)
    {
        if (value.IsNull != 0 || value.IsAbsent != 0) return null;
        object raw = (DataType)value.Type switch
        {
            DataType.Bool => value.Unsigned != 0,
            DataType.Int64 => unchecked((long)value.Unsigned),
            DataType.UInt64 => value.Unsigned,
            DataType.Float64 => value.Real,
            DataType.Utf8 => Marshal.PtrToStringUTF8(value.Text, checked((int)value.TextLength)) ?? string.Empty,
            DataType.Binary => ReadBytes(value.Text, value.TextLength),
            DataType.Decimal128 => ReadDecimal(value, column),
            DataType.Date32 => DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(unchecked((long)value.Unsigned))),
            DataType.Time64 => TimeOnly.FromTimeSpan(TimeSpan.FromTicks(unchecked((long)value.Unsigned) * 10)),
            DataType.Timestamp => DateTimeOffset.UnixEpoch.AddTicks(unchecked((long)value.Unsigned) * 10),
            DataType.DateTime => new DateTimeOffset(value.DateTime.Ticks, TimeSpan.FromMinutes(value.DateTime.HasOffset == 0 ? 0 : value.DateTime.OffsetMinutes)),
            DataType.Duration => new Duration(unchecked((long)value.Unsigned)),
            DataType.Interval => new Interval(value.Interval.Months, value.Interval.Days, value.Interval.Microseconds),
            _ => throw new InvalidOperationException($"Native SQL column '{column}' has unsupported value type {value.Type}.")
        };
        return ConvertTo(raw, targetType, column);
    }

    private static byte[] ReadBytes(IntPtr pointer, uint length)
    {
        var bytes = new byte[checked((int)length)];
        if (bytes.Length != 0) Marshal.Copy(pointer, bytes, 0, bytes.Length);
        return bytes;
    }

    private static decimal ReadDecimal(in Native.ScalarValue value, string column)
    {
        var coefficient = ((UInt128)value.DecimalHigh << 64) | value.DecimalLow;
        var negative = unchecked((long)value.DecimalHigh) < 0;
        var magnitude = negative ? unchecked(UInt128.Zero - coefficient) : coefficient;
        var scale = value.Scale;
        while (scale > 0)
        {
            var (quotient, remainder) = UInt128.DivRem(magnitude, 10);
            if (remainder != 0) break;
            magnitude = quotient;
            scale--;
        }
        if (scale is < 0 or > 28 || (magnitude >> 96) != 0)
            throw new InvalidOperationException($"Native SQL column '{column}' of type Decimal128 cannot be represented exactly by System.Decimal.");
        var low = unchecked((int)magnitude);
        var mid = unchecked((int)(magnitude >> 32));
        var high = unchecked((int)(magnitude >> 64));
        return new decimal(low, mid, high, negative, (byte)scale);
    }

    internal static void WriteDecimal(ref Native.ScalarValue result, decimal value, string column, int precision, int scale)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var coefficient = ((Int128)(uint)bits[2] << 64) | ((Int128)(uint)bits[1] << 32) | (uint)bits[0];
        if (bits[3] < 0) coefficient = -coefficient;
        var currentScale = (bits[3] >> 16) & 0xff;
        for (; currentScale < scale; currentScale++)
        {
            if (coefficient > Int128.MaxValue / 10 || coefficient < Int128.MinValue / 10)
                throw new OverflowException($"Native SQL column '{column}' cannot store this System.Decimal value at scale {scale}.");
            coefficient *= 10;
        }
        for (; currentScale > scale; currentScale--)
        {
            var (quotient, remainder) = Int128.DivRem(coefficient, 10);
            if (remainder != 0)
                throw new OverflowException($"Native SQL column '{column}' cannot store this System.Decimal value exactly at scale {scale}.");
            coefficient = quotient;
        }
        result.DecimalLow = unchecked((ulong)coefficient);
        result.DecimalHigh = unchecked((ulong)(coefficient >> 64));
        result.Precision = precision;
        result.Scale = scale;
    }
}
