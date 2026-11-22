using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>A host-approved single-table output and its exact published snapshot.</summary>
public sealed record AnalysisIcebergSource(string Id, string Name, AnalysisConnection Catalog,
    AnalysisConnection Storage, string Namespace, string Table, string SnapshotId, JsonElement Schema,
    string RunReceipt, DateTimeOffset DataAsOf, string? Warehouse = null, JsonElement? RowFilter = null);

public sealed record AnalysisConnection(string Driver, string Resource, string? CredentialReference = null);
