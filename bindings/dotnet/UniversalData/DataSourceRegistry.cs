namespace UniversalData;

public sealed class DataSourceRegistry
{
    private readonly object gate = new();
    private readonly Dictionary<string, SdkSourceBinding> sources = new(StringComparer.Ordinal);
    private readonly Func<string, CancellationToken, Task<DataSourceProbe>> probe;
    private readonly Action<SdkSourceBinding>? publish;

    internal DataSourceRegistry(IEnumerable<DataSourceDefinition> definitions,
        Func<string, CancellationToken, Task<DataSourceProbe>> probe, Action<SdkSourceBinding> publish)
    {
        this.probe = probe;
        foreach (var definition in definitions) Add(definition);
        this.publish = publish;
    }

    public DataSourceInfo Add(DataSourceDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var binding = new SdkSourceBinding(definition, definition.InitialRevision, true);
        lock (gate)
        {
            if (!sources.TryAdd(definition.Name, binding))
                throw SourceError(StatusCode.Busy, "The data source already exists");
        }
        publish?.Invoke(binding);
        return Describe(binding);
    }

    public DataSourceInfo Get(string name)
    {
        lock (gate) return Describe(Find(name));
    }

    public IReadOnlyList<DataSourceInfo> List()
    {
        lock (gate) return sources.Values.OrderBy(binding => binding.Definition.Name, StringComparer.Ordinal)
            .Select(Describe).ToArray();
    }

    public Task<DataSourceProbe> ProbeAsync(string name, CancellationToken cancellation = default) =>
        probe(name, cancellation);

    public DataSourceInfo Replace(string name, ulong expectedRevision, DataSourceDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (!string.Equals(name, definition.Name, StringComparison.Ordinal))
            throw new ArgumentException("Replacement must keep the registered source name", nameof(definition));
        SdkSourceBinding replacement;
        lock (gate)
        {
            var previous = FindRevision(name, expectedRevision);
            replacement = new SdkSourceBinding(definition, checked(previous.Revision + 1), true);
            sources[name] = replacement;
        }
        publish?.Invoke(replacement);
        return Describe(replacement);
    }

    public DataSourceInfo Disable(string name, ulong expectedRevision)
    {
        SdkSourceBinding disabled;
        lock (gate)
        {
            var previous = FindRevision(name, expectedRevision);
            disabled = previous with { Revision = checked(previous.Revision + 1), Enabled = false };
            sources[name] = disabled;
        }
        publish?.Invoke(disabled);
        return Describe(disabled);
    }

    internal SdkSourceBinding[] Snapshot()
    {
        lock (gate) return sources.Values.ToArray();
    }

    internal static DataSourceInfo Describe(SdkSourceBinding binding) =>
        new(binding.Definition.Name, binding.Definition.Kind, binding.Revision, binding.Enabled);

    internal static UniversalDataException SourceError(StatusCode code, string message) => new((int)code, message);

    private SdkSourceBinding Find(string name) => sources.TryGetValue(name, out var binding) ? binding
        : throw SourceError(StatusCode.Invalid, "The named data source does not exist");

    private SdkSourceBinding FindRevision(string name, ulong expected)
    {
        var binding = Find(name);
        if (binding.Revision != expected)
            throw SourceError(StatusCode.Fenced, "The data source revision changed");
        return binding;
    }
}
