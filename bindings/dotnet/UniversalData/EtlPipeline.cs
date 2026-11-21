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
        if (input.Changes is not null)
            throw new UniversalDataException((int)StatusCode.Unsupported, "Reading a CDC source requires a durable execution");
        if (sourceRows is 0 or > 10000) throw new ArgumentOutOfRangeException(nameof(sourceRows));
        if (sampleRows is 0 or > 32 || sampleRows > sourceRows) throw new ArgumentOutOfRangeException(nameof(sampleRows));
        var sources = Sources(previewSource: true);
        await using var worker = await client.OpenWorkerAsync(cancellationToken, dedicated: true).ConfigureAwait(false);
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

    public Task<JobOccurrence> SubmitAsync(EtlManagementClient management, string requestId,
        CancellationToken cancellationToken = default) =>
        SubmitAsync(management, requestId, 1, cancellationToken);

    public async Task<JobOccurrence> SubmitAsync(EtlManagementClient management, string requestId,
        ulong packageRevision, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(management);
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        if (packageRevision == 0) throw new ArgumentOutOfRangeException(nameof(packageRevision));
        var sources = Sources();
        if (sources.Any(source => source.Definition.IsDatabase &&
            string.IsNullOrWhiteSpace(source.Definition.CredentialReference)))
            throw new UniversalDataException((int)StatusCode.Invalid,
                "Published ETL sources require Management credential references");

        Package package;
        await using (var worker = await client.OpenWorkerAsync(cancellationToken, dedicated: true).ConfigureAwait(false))
            (package, _) = await CompileAsync(worker, sources, cancellationToken, publish: true).ConfigureAwait(false);

        CheckManagement(await management.PublishAsync(input.Id, packageRevision, package, cancellationToken)
            .ConfigureAwait(false));
        string revision = packageRevision.ToString(System.Globalization.CultureInfo.InvariantCulture);
        CheckManagement(await management.SaveJobAsync(new(input.Id, input.Id, revision, []), cancellationToken)
            .ConfigureAwait(false));
        var occurrence = await management.RunJobAsync(input.Id, requestId, cancellation: cancellationToken)
            .ConfigureAwait(false);
        CheckManagement(occurrence);
        return occurrence.Value ?? throw new UniversalDataException((int)StatusCode.Protocol,
            "Management returned no job occurrence");
    }

    private Task<EtlRun> StartAsync(string id, string ticket, bool resume, CancellationToken cancellationToken)
    {
        var sources = Sources();
        if (input.Changes is { MaxTransactions: null or 0 })
            throw new UniversalDataException((int)StatusCode.Unsupported,
                "Submit a continuous CDC job to Management so its lifetime is independent of the client");
        return new EtlClient(client).StartAsync(async (worker, ct) =>
            (await CompileAsync(worker, sources, ct).ConfigureAwait(false)).Package,
            sources, "{}", id, ticket, resume, cancellationToken);
    }

    private SdkSourceBinding[] Sources(bool previewSource = false)
    {
        var sources = client.Sources.Snapshot();
        SdkSourceBinding Required(string name, bool iceberg = false)
        {
            var binding = sources.SingleOrDefault(item => item.Definition.Name == name);
            return binding is { Enabled: true } &&
                (iceberg ? binding.Definition.Kind == DataSourceKind.Iceberg : binding.Definition.IsDatabase) ? binding :
                throw new UniversalDataException((int)StatusCode.Auth, "The named ETL resource is unavailable");
        }
        var source = Required(input.Source, iceberg: input.IcebergNamespace is not null);
        if (previewSource) return [source];
        var target = Required(output.Target, iceberg: output.Namespace is not null);
        return input.Source == output.Target ? [source] : [source, target];
    }

    private async Task<(Package Package, JsonElement Preview)> CompileAsync(WorkerSession worker, SdkSourceBinding[] sources,
        CancellationToken cancellationToken, bool previewSource = false, bool publish = false)
    {
        var tableMap = EntityMapping.For(input.Type);
        var builder = Connections(sources, publish: false);
        var inspection = previewSource || (input.Table is not null && input.IcebergNamespace is null)
            ? Build(builder) : null;
        JsonElement schema;
        if (input.Table is { } table && input.IcebergNamespace is null)
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

        if (publish) builder = Connections(sources, publish: true);
        if (input.Changes is { } changes)
            builder.ChangeSource(SourceId, input.Source, sources[0].Definition.Kind, input.Table!, schema, changes);
        else if (input.IcebergNamespace is { } sourceNamespace)
            builder.IcebergSource(SourceId, input.Source, IcebergStorage(input.Source), sourceNamespace, input.Table!,
                schema, input.SnapshotId, input.BatchSize, sources[0].Definition.CatalogWarehouse);
        else
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
        if (output.Namespace is { } tableNamespace)
            builder.IcebergDestination(TargetId, output.Target, IcebergStorage(output.Target), tableNamespace, output.Table,
                output.Mode, keys, sources[^1].Definition.CatalogWarehouse, input.Changes is null ? null : "before");
        else
            builder.SqlDestination(TargetId, output.Target, output.Table, output.Mode,
                input.Changes is null ? mapping : null, keys, input.Stream ? "response" : "receipt", !input.Stream);
        builder.Connect(input.Map is null ? SourceId : MapId, TargetId).DataFlow("copy");
        return (Build(builder), default);
    }

    private EtlPackageBuilder Connections(SdkSourceBinding[] sources, bool publish)
    {
        var builder = new EtlPackageBuilder(input.Id);
        foreach (var source in sources)
        {
            var definition = source.Definition;
            string? reference = publish ? definition.CredentialReference : SdkNativeSources.CredentialReference(source);
            if (definition.Kind == DataSourceKind.Iceberg)
                builder.Connection(definition.Name, "http", definition.Resource,
                        definition.CredentialReference is null ? null : reference)
                    .Connection(IcebergStorage(definition.Name), "file", definition.StorageRoot!);
            else
                builder.Connection(definition.Name, SdkNativeSources.PluginPath(definition.Kind, client.WorkerPath),
                    definition.Resource, reference);
        }
        return builder;
    }

    private string IcebergStorage(string name) => name == input.Source ? "iceberg-input-files" : "iceberg-output-files";

    private static void CheckManagement<T>(Result<T> result)
    {
        if (!result.IsSuccess) throw new UniversalDataException((int)result.Code,
            result.Error?.Message ?? "Management rejected the ETL submission", result.Error);
    }

    private static Package Build(EtlPackageBuilder builder)
    {
        var package = builder.Build();
        return package.Value ?? throw new UniversalDataException((int)package.Code,
            "The typed ETL plan could not be compiled", package.Error);
    }

    private static string Name(JsonElement field) => field.GetProperty("name").GetString()!;
}
