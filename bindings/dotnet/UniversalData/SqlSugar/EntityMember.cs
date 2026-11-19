using System.Reflection;
using UniversalData;

namespace SqlSugar;

internal sealed class EntityMember
{
    internal required PropertyInfo Property { get; init; }
    internal required string ColumnName { get; init; }
    internal required DataType NativeType { get; init; }
    internal required Type ValueType { get; init; }
    internal required bool Nullable { get; init; }
    internal required bool IsPrimaryKey { get; init; }
    internal required bool IsIdentity { get; init; }
    internal required bool IgnoreInsert { get; init; }
    internal required bool IgnoreUpdate { get; init; }
    internal required int Precision { get; init; }
    internal required int Scale { get; init; }

    internal object? Get(object instance) => Property.GetValue(instance);

    internal void Set(object instance, object? value)
    {
        if (value is null)
        {
            if (ValueType.IsValueType && System.Nullable.GetUnderlyingType(ValueType) is null)
                throw new InvalidOperationException($"Native SQL column '{ColumnName}' is null and '{ValueType.FullName}' is not nullable.");
            Property.SetValue(instance, null);
            return;
        }
        Property.SetValue(instance, SqlValueCodec.ConvertTo(value, ValueType, ColumnName));
    }
}
