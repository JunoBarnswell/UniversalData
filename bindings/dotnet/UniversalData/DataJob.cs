namespace UniversalData;

/// <summary>A declaration compiled to an immutable Native Package.</summary>
public abstract class DataJob
{
    private protected DataJob(DataJobGraph graph) => Graph = graph;
    internal DataJobGraph Graph { get; }
    public string Id => Graph.Id;
    public static DataJob<TInput> Define<TInput>(string id) => new(new DataJobGraph(id, typeof(TInput)));
    public static DataJob<object> Define(string id) => Define<object>(id);
}
