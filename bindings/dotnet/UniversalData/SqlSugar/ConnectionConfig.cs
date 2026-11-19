namespace SqlSugar;

/// <summary>Connection and Native plugin settings for one logical database.</summary>
public sealed class ConnectionConfig
{
    public string ConfigId { get; set; } = "default";
    public DbType DbType { get; set; }
    public string ConnectionString { get; set; } = string.Empty;
    public InitKeyType InitKeyType { get; set; } = InitKeyType.Attribute;
    public bool IsAutoCloseConnection { get; set; } = true;
    public string? PluginPath { get; set; }
    public uint ReadBatchSize { get; set; } = 512;
    public uint WriteChunkRows { get; set; } = 512;
}
