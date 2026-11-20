using System.Text.Json;

namespace UniversalData;

/// <summary>A completed typed ETL definition compiled by the Native Package builder.</summary>
public sealed class EtlPipeline
{
    private const string SourceId = "source";
    private const string MapId = "map";
    private const string TargetId = "target";
    private readonly DataClient client;
    private readonly EtlInput input;
    private readonly EtlOutput output;

    internal EtlPipeline(DataClient client, EtlInput input, EtlOutput output)
    {
        this.client = client;
        this.input = input;
        this.output = output;
    }

    /// <summary>Sample the table source through the read-only Native preview.</summary>
    public Task<JsonElement> PreviewAsync(uint sourceRows = 1000, CancellationToken cancellationToken = default) =>
        PreviewAsync(sourceRows, Math.Min(sourceRows, 32), cancellationToken);

    public async Task<JsonElement> PreviewAsync(uint sourceRows, uint sampleRows,
        CancellationToken cancellationToken = default)
    {
        if (sourceRows is 0 or > 10000) throw new ArgumentOutOfRangeException(nameof(sourceRows));
        if (sampleRows is 0 or > 32 || sampleRows > sourceRows) throw new ArgumentOutOfRangeException(nameof(sampleRows));
        var sources = Sources(previewSource: true);
        await using var worker = await client.OpenWorkerAsync(cancellationToken).ConfigureAwait(false);
        var (package, preview) = await CompileAsync(worker, sources, cancellationToken, previewSource: true).ConfigureAwait(false);
        return await EtlClient.PreviewAsync(worker, package, preview,
            SourceId, sourceRows, sampleRows, "{}", sources, cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<EtlRun> StartAsync(CancellationToken cancellationToken = default) =>
        StartAsync(Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N"), false, cancellationToken);

    public Task<EtlRun> ResumeAsync(string executionId, string journalTicket, CancellationToken cancellationToken = default)
    {
        if (input.Stream)
            throw new UniversalDataException((int)StatusCode.Unsupported, "A direct database stream has no resumable snapshot");
        return StartAsync(executionId, journalTicket, true, cancellationToken);
    }

    public async Task<ExecutionMetrics> RunAsync(CancellationToken cancellationToken = default)
    {
        await using var run = await StartAsync(cancellationToken).ConfigureAwait(false);
        return await run.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task<EtlRun> StartAsync(string id, string ticket, bool resume, CancellationToken cancellationToken)
    {
        var sources = Sources();
        return new EtlClient(client).StartAsync(async (worker, ct) =>
            (await CompileAsync(worker, sources, ct).ConfigureAwait(false)).Package,
            sources, "{}", id, ticket, resume, cancellationToken);
    }

    private SdkSourceBinding[] Sources(bool previewSource = false)
    {
        var sources = client.Sources.Snapshot();
        SdkSourceBinding Required(string name)
        {
            var binding = sources.SingleOrDefault(item => item.Definition.Name == name);
            return binding is { Enabled: true } && binding.Definition.Kind != DataSourceKind.Http ? binding :
                throw new UniversalDataException((int)StatusCode.Auth, "The named ETL database is unavailable");
        }
        return previewSource || input.Source == output.Target ? [Required(input.Source)] :
            [Required(input.Source), Required(output.Target)];
    }

    private async Task<(Package Package, JsonElement Preview)> CompileAsync(WorkerSession worker, SdkSourceBinding[] sources,
        CancellationToken cancellationToken, bool previewSource = false)
    {
        var tableMap = EntityMapping.For(input.Type);
        var builder = new EtlPackageBuilder(input.Id);
        foreach (var source in sources)
            builder.Connection(source.Definition.Name, SdkNativeSources.PluginPath(source.Definition.Kind, client.WorkerPath),
                source.Definition.Resource, SdkNativeSources.CredentialReference(source));
        var inspection = previewSource || input.Table is not null ? Build(builder) : null;
        JsonElement schema;
        if (input.Table is { } table)
        {
            var physical = await EtlClient.InspectAsync(worker, "discover", inspection!, sources, "{}",
                input.Source, table, cancellationToken).ConfigureAwait(false);
            var fields = physical.GetProperty("fields").EnumerateArray().ToDictionary(Name, StringComparer.OrdinalIgnoreCase);
            schema = JsonSerializer.SerializeToElement(new
            {
                version = ProductInfo.Protocol,
                fields = tableMap.Members.Select(member => fields.TryGetValue(member.ColumnName, out var field) ? field :
                    throw new UniversalDataException((int)StatusCode.Schema,
                        $"Table '{table}' has no mapped column '{member.ColumnName}'")).ToArray()
            }, WorkerProtocol.Json);
        }
        else
        {
            using var typed = JsonDocument.Parse(tableMap.SchemaJson());
            schema = typed.RootElement.Clone();
        }

        builder.SqlSource(SourceId, input.Source, input.Table, input.Sql, schema, input.Stream, input.BatchSize,
            input.Parameters);
        if (previewSource) return (inspection!, builder.DescribeDataFlow());

        var outputFields = input.Map is null
            ? schema.GetProperty("fields").EnumerateArray().Select(Name).ToArray()
            : output.Mapping.Members.Select(member => member.ColumnName).ToArray();
        var mapping = outputFields.Select((name, index) => new SqlColumnMapping(name,
            output.Mapping.Members[index].ColumnName)).ToArray();
        var keys = output.Keys.Select(key => Array.FindIndex(mapping, pair => pair.Target == key)).ToArray();
        if (keys.Any(index => index < 0))
            throw new UniversalDataException((int)StatusCode.Schema, "An ETL key is not in the target projection");

        if (input.Map is { } method)
        {
            var module = client.Modules.Single(entry => entry.Sha256 == method.ModuleSha256);
            builder.RegisterModule(JsonSerializer.SerializeToElement(new ManagedAssembly("managedAssembly",
                    module.Path, module.Sha256), WorkerProtocol.Json))
                .CSharpTransform(MapId, EtlMapRegistration.Component,
                    JsonSerializer.SerializeToElement(new { method }, WorkerProtocol.Json))
                .Connect(SourceId, MapId);
        }
        builder.SqlDestination(TargetId, output.Target, output.Table, output.Mode, mapping, keys,
                input.Stream ? "response" : "receipt", !input.Stream)
            .Connect(input.Map is null ? SourceId : MapId, TargetId)
            .DataFlow("copy");
        return (Build(builder), default);
    }

    private static Package Build(EtlPackageBuilder builder)
    {
        var package = builder.Build();
        return package.Value ?? throw new UniversalDataException((int)package.Code,
            "The typed ETL plan could not be compiled", package.Error);
    }

    private static string Name(JsonElement field) => field.GetProperty("name").GetString()!;
}
