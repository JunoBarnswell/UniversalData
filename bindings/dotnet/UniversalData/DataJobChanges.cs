namespace UniversalData;

/// <summary>Native CDC source options. Native always retains operation and before-image facts.</summary>
public sealed record DataJobChanges
{
    public DataJobChanges(string? Position = null, string? Slot = null, string? Publication = null,
        uint? ServerId = null, uint? MicroBatchRows = null, uint? MicroBatchMilliseconds = null,
        uint? MaxTransactions = null, bool AppendOnly = false, string? Mode = null)
    {
        if (Position is not null) ArgumentException.ThrowIfNullOrWhiteSpace(Position);
        if (Slot is not null) ArgumentException.ThrowIfNullOrWhiteSpace(Slot);
        if (Publication is not null) ArgumentException.ThrowIfNullOrWhiteSpace(Publication);
        if (ServerId is 0) throw new ArgumentOutOfRangeException(nameof(ServerId));
        if (MicroBatchRows is 0) throw new ArgumentOutOfRangeException(nameof(MicroBatchRows));
        if (MicroBatchMilliseconds is 0) throw new ArgumentOutOfRangeException(nameof(MicroBatchMilliseconds));
        if (MaxTransactions is 0) throw new ArgumentOutOfRangeException(nameof(MaxTransactions));
        if (Mode is not (null or "stream" or "snapshotAndStream"))
            throw new ArgumentException("CDC mode must be stream or snapshotAndStream", nameof(Mode));
        this.Position = Position;
        this.Slot = Slot;
        this.Publication = Publication;
        this.ServerId = ServerId;
        this.MicroBatchRows = MicroBatchRows;
        this.MicroBatchMilliseconds = MicroBatchMilliseconds;
        this.MaxTransactions = MaxTransactions;
        this.AppendOnly = AppendOnly;
        this.Mode = Mode;
    }

    public string? Position { get; init; }
    public string? Slot { get; init; }
    public string? Publication { get; init; }
    public uint? ServerId { get; init; }
    public uint? MicroBatchRows { get; init; }
    public uint? MicroBatchMilliseconds { get; init; }
    public uint? MaxTransactions { get; init; }
    public bool AppendOnly { get; init; }
    public string? Mode { get; init; }
}
