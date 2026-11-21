using System.Text.Json;

namespace UniversalData;

public sealed class DataBusinessException : Exception
{
    public DataBusinessException(string remoteType, string message, DataRunReference reference)
        : base(message) { RemoteType = remoteType; Reference = reference; }
    public string RemoteType { get; }
    public DataRunReference Reference { get; }
}
