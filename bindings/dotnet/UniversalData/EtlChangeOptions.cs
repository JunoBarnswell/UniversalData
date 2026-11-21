namespace UniversalData;

/// <summary>Provider settings for a snapshot followed by CDC, or a live stream starting at an explicit position.</summary>
public sealed record EtlChangeOptions(string? Position = null, string? Slot = null, string? Publication = null,
    uint? ServerId = null, uint? MicroBatchRows = null, uint? MicroBatchMilliseconds = null,
    uint? MaxTransactions = null);
