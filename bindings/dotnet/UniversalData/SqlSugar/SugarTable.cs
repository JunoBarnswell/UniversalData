namespace SqlSugar;

[AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
public sealed class SugarTable : Attribute
{
    public SugarTable(string tableName) => TableName = tableName;
    public string TableName { get; }
    public string? TableDescription { get; set; }
}
