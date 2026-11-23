using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>
/// One host-registered read-only SQL table or view for Analysis. The Native
/// connector owns schema discovery; Schema is an optional verified cache for
/// hosts that already have an exact schema snapshot.
/// </summary>
public sealed record AnalysisSqlSource(string Id, string Name, AnalysisConnection Connection,
    string Table, bool AllowSharing = false, JsonElement? Schema = null);
