using System.Runtime.CompilerServices;
using System.Text.Json;

namespace UniversalData;

public sealed partial class DataClient
{
    internal DataConfiguration? Configuration { get; set; }

    public static DataClient Create(string[] args) => DataConfiguration.FromArguments(args).CreateClient();

    /// <summary>Export one Native definition. Published definitions refer only to host credentials.</summary>
    public Package Compile(DataJob job, bool publish = false)
    {
        ArgumentNullException.ThrowIfNull(job);
        return CompileJob(job, Sources.Snapshot(), publish);
    }

    public Task<DataJobResult> RunAsync(DataJob<object> job, CancellationToken cancellation = default) =>
        RunAsync(job, new object(), cancellation);

    public async Task<DataJobResult> RunAsync<TInput>(DataJob<TInput> job, TInput input,
        CancellationToken cancellation = default)
    {
        await using var run = await StartAsync(job, input, cancellation).ConfigureAwait(false);
        return new(run.Id, run.JournalTicket, await run.WaitAsync(cancellation).ConfigureAwait(false));
    }

    public Task<EtlRun> StartAsync(DataJob<object> job, CancellationToken cancellation = default) =>
        StartAsync(job, new object(), cancellation);

    public Task<EtlRun> StartAsync<TInput>(DataJob<TInput> job, TInput input,
        CancellationToken cancellation = default) => StartDefinitionAsync(job, input, false, cancellation);

    public Task<EtlRun> StartAsync<TRow>(DataJobQuery<object, TRow> job, CancellationToken cancellation = default) =>
        StartAsync(job, new object(), cancellation);

    public Task<EtlRun> StartAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        CancellationToken cancellation = default) => StartDefinitionAsync(job, input, true, cancellation);

    public Task<EtlRun> ResumeAsync(DataJob<object> job, DataRunReference execution,
        CancellationToken cancellation = default) => ResumeAsync(job, new object(), execution, cancellation);

    public Task<EtlRun> ResumeAsync<TInput>(DataJob<TInput> job, TInput input, DataRunReference execution,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(execution);
        if (execution.JournalTicket is null) throw new ArgumentException("The Native Journal ticket is required", nameof(execution));
        return StartDefinitionAsync(job, input, false, cancellation, execution);
    }

    public Task<DataJobResult> RunAsync(Package package, string parameters = "{}",
        CancellationToken cancellationToken = default) =>
        RunJobAsync(package, parameters, Sources.Snapshot(), cancellationToken);

    private async Task<DataJobResult> RunJobAsync(Package package, string parameters, SdkSourceBinding[] sources,
        CancellationToken cancellation)
    {
        await using var run = await StartJobAsync(package, parameters, sources, false, cancellation).ConfigureAwait(false);
        var metrics = await run.WaitAsync(cancellation).ConfigureAwait(false);
        return new(run.Id, run.JournalTicket, metrics);
    }

    public IAsyncEnumerable<TRow> RunAsync<TRow>(DataJobQuery<object, TRow> job,
        CancellationToken cancellation = default) => RunAsync(job, new object(), cancellation);

    public async IAsyncEnumerable<TRow> RunAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        await using var run = await StartAsync(job, input, cancellation).ConfigureAwait(false);
        await foreach (var row in run.ReadRowsAsync<TRow>(cancellation).ConfigureAwait(false))
            yield return row;
    }

    private Task<EtlRun> StartDefinitionAsync(DataJob job, object? input, bool readResults,
        CancellationToken cancellation, DataRunReference? execution = null)
    {
        ArgumentNullException.ThrowIfNull(job);
        var sources = Sources.Snapshot();
        var package = CompileJob(job, sources, publish: false);
        return StartJobAsync(package, Parameters(job, input), sources, readResults, cancellation, execution);
    }

    private Package CompileJob(DataJob job, SdkSourceBinding[] sources, bool publish)
    {
        if (Configuration?.HasJob(job.Id) == true)
            throw new InvalidDataException($"Job '{job.Id}' is declared in both YAML and C#.");
        return job.Graph.Compile(sources, publish, Configuration?.ExecutionOptions(job.Id));
    }

    private Task<EtlRun> StartJobAsync(Package package, string parameters, SdkSourceBinding[] inventory,
        bool readResults, CancellationToken cancellation, DataRunReference? execution = null)
    {
        ArgumentNullException.ThrowIfNull(package);
        cancellation.ThrowIfCancellationRequested();
        var root = package.Definition;
        var sources = BindJobSources(root, inventory);
        uint slots = root.TryGetProperty("policy", out var policy) &&
            policy.TryGetProperty("maxConcurrency", out var parallelism) ? parallelism.GetUInt32() : 1;
        return new EtlClient(this).StartAsync((_, _) => Task.FromResult(package), sources, parameters,
            execution?.RunId ?? Guid.NewGuid().ToString("N"), execution?.JournalTicket ?? Guid.NewGuid().ToString("N"),
            execution is not null, cancellation, readResults, slots);
    }

    private string Parameters(DataJob job, object? input)
    {
        var explicitValues = job.Graph.Parameters(input);
        if (Configuration is null) return explicitValues;
        var configured = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(Configuration.Parameters(job.Id))!;
        using var values = JsonDocument.Parse(explicitValues);
        foreach (var value in values.RootElement.EnumerateObject())
        {
            foreach (string key in configured.Keys.Where(key =>
                key.Equals(value.Name, StringComparison.OrdinalIgnoreCase)).ToArray())
                configured.Remove(key);
            configured[value.Name] = value.Value.Clone();
        }
        return JsonSerializer.Serialize(configured, WorkerProtocol.Json);
    }

    private static SdkSourceBinding[] BindJobSources(JsonElement definition, SdkSourceBinding[] inventory)
    {
        var byName = inventory.ToDictionary(source => source.Definition.Name, StringComparer.Ordinal);
        var sources = new List<SdkSourceBinding>();
        if (!definition.TryGetProperty("connections", out var connections)) return [];
        foreach (var connection in connections.EnumerateArray())
        {
            string name = connection.GetProperty("id").GetString()!;
            string? credential = connection.TryGetProperty("credentialReference", out var reference) &&
                reference.ValueKind == JsonValueKind.String ? reference.GetString() : null;
            bool sdkCredential = credential?.StartsWith("sdk-source/", StringComparison.Ordinal) == true;
            if (!sdkCredential && (!byName.TryGetValue(name, out var http) ||
                http.Definition.Kind != DataSourceKind.Http))
                continue;
            if (!byName.TryGetValue(name, out var registered) || !registered.Enabled ||
                (sdkCredential && SdkNativeSources.CredentialReference(registered) != credential) ||
                !connection.TryGetProperty("resource", out var resource) ||
                resource.ValueKind != JsonValueKind.String || resource.GetString() != registered.Definition.Resource)
                throw new UniversalDataException((int)StatusCode.Auth, $"Job resource '{name}' changed before execution");
            sources.Add(registered);
        }
        return sources.ToArray();
    }
}
