namespace UniversalData;

public sealed class DataJobQuery<TInput, TRow> : DataJob
{
    internal DataJobQuery(DataJobGraph graph) : base(graph) { }
}
