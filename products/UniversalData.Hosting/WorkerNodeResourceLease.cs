using System.Text.Json;

namespace UniversalData.Hosting;

internal sealed class WorkerNodeResourceLease(string path, WorkerNodeResourceRecord record) : IDisposable
{
    internal void Bind(int pid)
    {
        if (WorkerProcessIdentity.Read(pid) is { } worker)
            HostFiles.WritePrivate(path, JsonSerializer.SerializeToUtf8Bytes(record with { Worker = worker }), overwrite: true);
    }

    public void Dispose() => File.Delete(path);
}
