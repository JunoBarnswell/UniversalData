namespace UniversalData;

public sealed record DataJobResult(string ExecutionId, string JournalTicket, ExecutionMetrics Metrics)
{
    public ulong CommittedRows => Metrics.CommittedRows;
    public ulong RejectedRows => Metrics.RejectedRows;
    public DataRunReference Reference => new(ExecutionId, JournalTicket);
}
