namespace SqlSugar;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
public sealed class SugarColumn : Attribute
{
    private int? decimalDigits;

    public string? ColumnName { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsIdentity { get; set; }
    public bool IsIgnore { get; set; }
    public bool IsOnlyIgnoreInsert { get; set; }
    public bool IsOnlyIgnoreUpdate { get; set; }
    public int Length { get; set; }
    public int DecimalDigits
    {
        get => decimalDigits.GetValueOrDefault();
        set => decimalDigits = value;
    }
    public bool IsNullable { get; set; } = true;

    internal int? DeclaredDecimalDigits => decimalDigits;
}
