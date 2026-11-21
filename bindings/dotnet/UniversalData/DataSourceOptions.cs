namespace UniversalData;

public sealed class DataSourceOptions
{
    public string? Resource { get; set; }
    public string? CredentialReference { get; set; }
    public string? NativeConnectionString { get; set; }
    public ulong InitialRevision { get; set; } = 1;
    public bool ProvisionTransactionReceipts { get; set; }
}
