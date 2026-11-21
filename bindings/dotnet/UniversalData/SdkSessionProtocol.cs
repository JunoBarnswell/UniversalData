using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;

namespace UniversalData;

internal sealed record SdkHostConfiguration(int Revision, string PolicyFile, string Tenant,
    SdkHostOptions Options, SdkModule[] Modules, string Endpoint, string Capability);

internal sealed record SdkWorkerConfiguration(SdkHostOptions Options, SdkModule[] Modules,
    string Endpoint, string Capability, string HostEndpoint, string SourceStateFile, int Capacity, bool Cooperative)
{
    internal int MaxConnectionsPerSource => Options.MaxConnectionsPerSource - (Options.RequireIsolation ? 1 : 0);
}

internal sealed record SdkPipeAdmission(int Revision, string Capability, string Operation = "open",
    string? SourceName = null, SdkSourceRevision? SourceRevision = null, string? SessionId = null,
    bool Dedicated = false, bool Poison = false);

internal sealed record SdkSessionAssignment(int Revision, string WorkerCapability, string SessionId,
    string Endpoint, string Capability);

internal sealed record SdkSourceRevision(ulong Revision, bool Enabled);

internal sealed record SdkWorkerGrant(int ProcessId, string Endpoint, string Capability, string SessionId, bool Cooperative);

internal sealed record SdkHostStatus(int Revision, StatusCode Code, OperationError? Error = null,
    SdkWorkerGrant? Grant = null);

internal static class SdkSessionProtocol
{
    internal const string DeploymentVariable = "UD_INTERNAL_SDK_DEPLOYMENT";
    internal static string CreateEndpoint() => "ud-sdk-" + Guid.NewGuid().ToString("N");
    internal static string CreateCapability() => Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
    internal static bool ValidEndpoint(string endpoint) => endpoint.StartsWith("ud-sdk-", StringComparison.Ordinal) &&
        Guid.TryParseExact(endpoint.AsSpan(7), "N", out _);

    internal static NamedPipeServerStream Listen(string endpoint, int capacity = 1) =>
        new(endpoint, PipeDirection.InOut, capacity, PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);

    internal static NamedPipeClientStream Connect(string endpoint) =>
        new(".", endpoint, PipeDirection.InOut, PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);

    internal static bool Authenticate(SdkPipeAdmission admission, string expected) =>
        Authenticate(admission.Revision, admission.Capability, expected);

    internal static bool Authenticate(int revision, string capability, string expected) =>
        revision == ProductInfo.WorkerRevision && capability is { Length: 64 } &&
        CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(capability), Encoding.ASCII.GetBytes(expected));
}
