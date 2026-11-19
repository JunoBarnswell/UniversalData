using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using UniversalData;

namespace SqlSugar;

internal sealed class EntityMapping
{
    private static readonly ConcurrentDictionary<Type, EntityMapping> Cache = new();

    private EntityMapping(Type entityType)
    {
        EntityType = entityType;
        var table = entityType.GetCustomAttribute<SugarTable>(true);
        TableName = table?.TableName ?? entityType.Name;
        Members = BuildMembers(entityType);
    }

    internal Type EntityType { get; }
    internal string TableName { get; }
    internal IReadOnlyList<EntityMember> Members { get; }
    internal IReadOnlyList<EntityMember> Keys => Members.Where(member => member.IsPrimaryKey).ToArray();

    internal static EntityMapping For(Type type) => Cache.GetOrAdd(type, static value => new EntityMapping(value));
    internal static EntityMapping For<T>() => For(typeof(T));

    internal string SchemaJson(IEnumerable<EntityMember>? selected = null) => FormatSchema(selected ?? Members);

    internal static string FormatSchema(IEnumerable<EntityMember> members)
    {
        var fields = members.Select(member =>
        {
            var field = new Dictionary<string, object?>
            {
                ["name"] = member.ColumnName,
                ["type"] = SqlValueCodec.NativeTypeName(member.NativeType),
                ["nullable"] = member.Nullable
            };
            if (member.NativeType == DataType.Decimal128) { field["precision"] = member.Precision; field["scale"] = member.Scale; }
            return field;
        }).ToArray();
        return JsonSerializer.Serialize(new Dictionary<string, object?> { ["version"] = ProductInfo.Protocol, ["fields"] = fields });
    }

    private static IReadOnlyList<EntityMember> BuildMembers(Type entityType)
    {
        var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(property => property.CanRead && property.GetIndexParameters().Length == 0 && property.GetMethod is not null)
            .ToArray();
        var result = new List<EntityMember>(properties.Length);
        foreach (var property in properties)
        {
            var attribute = property.GetCustomAttribute<SugarColumn>(true);
            if (attribute?.IsIgnore == true) continue;
            var columnName = string.IsNullOrWhiteSpace(attribute?.ColumnName) ? property.Name : attribute!.ColumnName!;
            var type = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            if (!SqlValueCodec.TryGetNativeType(type, out var nativeType))
                throw new InvalidOperationException($"Entity '{entityType.FullName}' member '{property.Name}' type '{type.FullName}' is not supported by Native SQL mapping.");
            var key = attribute?.IsPrimaryKey == true || string.Equals(property.Name, "Id", StringComparison.OrdinalIgnoreCase)
                || string.Equals(property.Name, entityType.Name + "Id", StringComparison.OrdinalIgnoreCase);
            result.Add(new EntityMember
            {
                Property = property,
                ColumnName = columnName,
                NativeType = nativeType,
                ValueType = property.PropertyType,
                Nullable = !property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) is not null || attribute?.IsNullable == true,
                IsPrimaryKey = key,
                IsIdentity = attribute?.IsIdentity == true,
                IgnoreInsert = attribute?.IsOnlyIgnoreInsert == true,
                IgnoreUpdate = attribute?.IsOnlyIgnoreUpdate == true,
                Precision = attribute?.Length > 0 ? attribute.Length : 38,
                Scale = attribute?.DeclaredDecimalDigits ?? 28
            });
        }
        if (result.Count == 0) throw new InvalidOperationException($"Entity '{entityType.FullName}' has no mapped members.");
        return result;
    }
}
