using System.Data;

namespace SqlSugar;

internal static class EntityMaterializer
{
    internal static T Read<T>(IDataRecord reader)
        => EntityMaterializerPlan<T>.Create(reader).Read(reader);
}
