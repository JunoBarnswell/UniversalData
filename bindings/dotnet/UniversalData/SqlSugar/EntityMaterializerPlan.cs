using System.Data;

namespace SqlSugar;

internal sealed class EntityMaterializerPlan<T>
{
    private readonly (EntityMember Member, int Ordinal)[] members;

    private EntityMaterializerPlan(IDataRecord reader)
    {
        var mapping = EntityMapping.For<T>();
        var selected = new List<(EntityMember, int)>(mapping.Members.Count);
        foreach (var member in mapping.Members)
        {
            try { selected.Add((member, reader.GetOrdinal(member.ColumnName))); }
            catch (IndexOutOfRangeException) { }
        }
        members = selected.ToArray();
    }

    internal static EntityMaterializerPlan<T> Create(IDataRecord reader) => new(reader);

    internal T Read(IDataRecord reader)
    {
        object entity = Activator.CreateInstance<T>()!;
        foreach (var (member, ordinal) in members)
        {
            var value = reader.GetValue(ordinal);
            member.Set(entity, value is DBNull ? null : value);
        }
        return (T)entity;
    }
}
