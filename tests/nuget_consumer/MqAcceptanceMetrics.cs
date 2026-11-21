public sealed record MqAcceptanceMetrics(ulong PublishedCommitted, ulong ConsumedCommitted,
    ulong ConsumedAcknowledged);
