using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>Verifies the inherited Worker report stream before importing Native facts into the tenant Catalog.</summary>
internal sealed class WorkerReportReader(Catalog catalog, RunState run, Func<CancellationToken, ValueTask<string?>> read)
{
    internal string? NativeId { get; private set; } = run.ExecutionId.Length == 0 ? null : run.ExecutionId;

    internal void BindReady(string nativeId)
    {
        if (nativeId.Length == 0 || NativeId is not null && NativeId != nativeId)
            throw new InvalidDataException("Worker Native history differs from dispatch");
        NativeId = nativeId;
    }

    internal async Task<JsonElement> ReadAsync(string expected, CancellationToken cancellation)
    {
        while (true)
        {
            string text = await read(cancellation) ?? throw new InvalidDataException("Worker outcome is unknown");
            using var message = JsonDocument.Parse(text);
            var report = message.RootElement;
            if (report.GetProperty("version").GetInt32() != ProductInfo.Protocol || report.GetProperty("ticket").GetString() != run.Id)
                throw new InvalidDataException("Worker identity differs from dispatch");
            string? kind = report.GetProperty("kind").GetString();
            if (kind == "failed")
                throw new WorkerFailureException(new RunFailure(report.GetProperty("result").GetInt32(),
                    report.GetProperty("phase").GetString()!, report.GetProperty("errorType").GetString()!));
            if (kind == "catalog")
            {
                Import(report);
                continue;
            }
            if (kind != expected)
                throw new InvalidDataException("Worker phase differs from dispatch");
            return report.Clone();
        }
    }

    private void Import(JsonElement report)
    {
        var kind = (CatalogKind)report.GetProperty("catalogKind").GetInt32();
        var records = report.GetProperty("records");
        if (kind is not (CatalogKind.Execution or CatalogKind.Event) || records.GetArrayLength() > 100)
            throw new InvalidDataException("Invalid Worker Catalog page");
        foreach (var entry in records.EnumerateArray())
        {
            string? recordId = entry.GetProperty("id").GetString();
            var document = entry.GetProperty("document");
            if (string.IsNullOrEmpty(recordId) || document.GetProperty("packageId").GetString() != run.PackageId ||
                document.GetProperty(kind == CatalogKind.Event ? "executionId" : "id").GetString() != recordId ||
                (NativeId is not null && NativeId != recordId))
                throw new InvalidDataException("Worker Catalog identity differs from dispatch");
            NativeId = recordId;
            catalog.Put(kind, recordId, entry.GetProperty("revision").GetUInt64(), document.GetRawText());
        }
    }
}
