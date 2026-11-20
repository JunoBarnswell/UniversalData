using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

internal sealed class EntityMapping
{
    private static readonly ConditionalWeakTable<Type, EntityMapping> Cache = new();

    private EntityMapping(Type entityType)
    {
        EntityType = entityType;
        var table = entityType.GetCustomAttribute<SugarTable>(true);
        TableName = table?.TableName ?? entityType.Name;
        Members = BuildMembers(entityType);
        Keys = Members.Where(member => member.IsPrimaryKey).ToArray();
        InsertMembers = Members.Where(member => !member.IsIdentity && !member.IgnoreInsert).ToArray();
    }

    internal Type EntityType { get; }
    internal string TableName { get; }
    internal IReadOnlyList<EntityMember> Members { get; }
    internal IReadOnlyList<EntityMember> Keys { get; }
    internal IReadOnlyList<EntityMember> InsertMembers { get; }

    internal static EntityMapping For(Type type) => Cache.GetValue(type, static value => new EntityMapping(value));
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
        var nullability = new NullabilityInfoContext();
        foreach (var property in properties)
        {
            var attribute = property.GetCustomAttribute<SugarColumn>(true);
            if (attribute?.IsIgnore == true) continue;
            var columnName = string.IsNullOrWhiteSpace(attribute?.ColumnName) ? property.Name : attribute!.ColumnName!;
            var type = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            if (!SqlValueCodec.TryGetNativeType(type, out var nativeType))
                throw new InvalidOperationException($"Entity '{entityType.FullName}' member '{property.Name}' type '{type.FullName}' is not supported by Native SQL mapping.");
            var attributeArguments = property.CustomAttributes.SingleOrDefault(value => value.AttributeType == typeof(SugarColumn))?.NamedArguments;
            int scale = attributeArguments?.Any(value => value.MemberName == nameof(SugarColumn.DecimalDigits)) == true
                ? attribute!.DecimalDigits : 28;
            result.Add(new EntityMember
            {
                Property = property,
                ColumnName = columnName,
                NativeType = nativeType,
                ValueType = property.PropertyType,
                Nullable = nullability.Create(property).ReadState != NullabilityState.NotNull || attribute?.IsNullable == true,
                IsPrimaryKey = attribute?.IsPrimaryKey == true,
                IsIdentity = attribute?.IsIdentity == true,
                IgnoreInsert = attribute?.IsOnlyIgnoreInsert == true,
                IgnoreUpdate = attribute?.IsOnlyIgnoreUpdate == true,
                Precision = attribute?.Length > 0 ? attribute.Length : 38,
                Scale = scale
            });
        }
        if (result.Count == 0) throw new InvalidOperationException($"Entity '{entityType.FullName}' has no mapped members.");
        return result;
    }
}
