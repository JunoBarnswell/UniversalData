namespace UniversalData.Hosting;

public sealed record WorkerNodeCapacity(uint MaxCpu, uint MaxProcesses, ulong MaxMemoryBytes, string[] Tenants)
{
    public bool Valid => MaxCpu > 0 && MaxProcesses is > 0 and <= 64 && MaxMemoryBytes > 0 &&
        Tenants is { Length: > 0 and <= 128 } && Tenants.All(HostIdentity.ValidId) &&
        Tenants.Distinct(StringComparer.Ordinal).Count() == Tenants.Length;
}
