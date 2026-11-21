using System.Text.Json;

namespace UniversalData;

/// <summary>Durable publication receipt returned by the management Catalog.</summary>
public sealed record JobPublicationReceipt(string Id, string Revision, JsonElement Deployment,
    JobOccurrence? Occurrence);
