using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Worker;

/// <summary>Transports bounded Native result batches. All row transformations run in the package.</summary>
internal static class AnalysisResults
{
    internal static object Run(HostPolicy policy, WorkerRequest request, CancellationToken cancellation)
    {
        const int responseBytes = 16 * 1024 * 1024;
        int limit = request.Payload!.Value.GetProperty("maxRows").GetInt32();
        bool sample = request.Payload.Value.TryGetProperty("sample", out var mode) && mode.GetBoolean();
        if (limit is < 1 or > 5005)
            throw new ArgumentOutOfRangeException(nameof(request));
        string directory = policy.ExecutionDirectory(request.Ticket);
        HostFiles.NoLinks(directory);
        Directory.CreateDirectory(directory);
        try
        {
            using var execution = policy.CreateExecution(request.Package, stateDirectory: sample ? null : directory);
            execution.OpenResults();
            execution.Start();
            var rows = new List<Dictionary<string, string?>>();
            long bytes = 0;
            while (true)
            {
                if (cancellation.IsCancellationRequested)
                    throw new UniversalDataException((int)StatusCode.Cancelled, "Analysis cancelled");
                var status = execution.ReadResult(100, out var wire);
                if (status == StatusCode.End) break;
                if (status == StatusCode.Busy) continue;
                if (status != StatusCode.Ok)
                    throw new UniversalDataException((int)status, "Native analysis failed");
                using (wire!)
                using (var batch = Batch.FromWire(wire!.Bytes, wire.Length, policy.MemoryLimit))
                {
                    if (!sample && (long)rows.Count + batch.Rows > limit)
                        throw new UniversalDataException((int)StatusCode.Resource, "Analysis result exceeds its row limit");
                    var json = batch.ToJson();
                    bytes += json.Length * 3L;
                    if (bytes > responseBytes)
                        throw new UniversalDataException((int)StatusCode.Resource, "Analysis result exceeds its size limit");
                    using var document = JsonDocument.Parse(json);
                    foreach (var row in document.RootElement.EnumerateArray().Take(limit - rows.Count))
                        rows.Add(row.EnumerateObject().ToDictionary(property => property.Name,
                            property => property.Value.ValueKind switch
                            {
                                JsonValueKind.Null => null,
                                JsonValueKind.String => property.Value.GetString(),
                                _ => property.Value.GetRawText()
                            }, StringComparer.Ordinal));
                }
                if (sample && rows.Count == limit)
                {
                    execution.Cancel();
                    break;
                }
            }
            try { execution.Wait(uint.MaxValue); }
            catch (UniversalDataException error) when (sample && rows.Count == limit && error.Code == StatusCode.Cancelled) { }
            var metrics = execution.Metrics;
            return new { executionId = execution.Id, rows, metrics.InputRows, metrics.OutputRows };
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true); }
    }
}
