namespace SqlSugar;

/// <summary>One named, parameterized value. The value is never interpolated into SQL.</summary>
public sealed class SugarParameter
{
    public SugarParameter() { }
    public SugarParameter(string parameterName, object? value) { ParameterName = parameterName; Value = value; }
    public SugarParameter(string parameterName, object? value, System.Data.DbType dbType) { ParameterName = parameterName; Value = value; DbType = dbType; }
    public SugarParameter(string parameterName, object? value, System.Data.DbType dbType, int size) { ParameterName = parameterName; Value = value; DbType = dbType; Size = size; }

    public string ParameterName { get; set; } = string.Empty;
    public object? Value { get; set; }
    public System.Data.DbType? DbType { get; set; }
    public int Size { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
}
