using System.Text.Json;

namespace UniversalData;

public sealed record DataRunReference(string RunId, string? TransactionId = null, string? JournalTicket = null);
