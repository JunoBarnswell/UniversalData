using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal sealed class SdkEtlSession(HostPolicy policy,
    Func<SdkSourceBinding[], SdkExecutionEnvironment> createEnvironment) : IDisposable
{
    private Execution? execution;
    private Catalog? catalog;
    private Task<Terminal>? terminal;

    internal async ValueTask<(WorkerResponseFrame, WorkerBatchBuffer?)> InvokeAsync(WorkerRequestFrame request,
        CancellationToken cancellationToken)
    {
        var values = request.Values;
        switch (request.Operation)
        {
            case "etl.instantiate":
                using (var package = new NativePackage(Text(values, "package"), policy.MemoryLimit))
                    return JsonReply(request, package.ToJson());
            case "etl.inspect":
            case "etl.catalog":
            case "etl.discover":
                var sources = ReadSources(values);
                var environment = Scope(sources);
                var connection = Text(values, "connection");
                var inspected = sources.Where(source => source.Definition.Name == connection).ToArray();
                await using (await ReserveAsync(environment, inspected, cancellationToken))
                using (var inspection = CreateExecution(values, environment))
                {
                    string json = request.Operation switch
                    {
                        "etl.inspect" => inspection.Explain(),
                        "etl.catalog" => inspection.ComponentCatalog(),
                        _ => inspection.DiscoverSchema(connection, Text(values, "table"))
                    };
                    return JsonReply(request, json);
                }
            case "etl.preview":
                var pipeline = values.GetProperty("pipeline");
                var selected = pipeline.GetProperty("components").EnumerateArray()
                    .Where(node => node.GetProperty("component").GetString() is "sql.source" or "iceberg.source")
                    .Select(node => Text(node.GetProperty("config"), "connection")).ToHashSet(StringComparer.Ordinal);
                var previewSources = ReadSources(values);
                var previewScope = Scope(previewSources);
                await using (await ReserveAsync(previewScope,
                    previewSources.Where(source => selected.Contains(source.Definition.Name)), cancellationToken))
                using (var preview = CreateExecution(values, previewScope))
                    return JsonReply(request, preview.Preview(pipeline.GetRawText(),
                        Text(values, "component"), values.GetProperty("sourceRows").GetUInt32(),
                        values.GetProperty("sampleRows").GetUInt32()));
            case "etl.start":
                return await StartAsync(request, cancellationToken);
            case "etl.progress":
                return JsonReply(request, RequireExecution().Progress());
            case "etl.wait":
                var completion = terminal ?? throw new UniversalDataException((int)StatusCode.Invalid,
                    "No Native ETL execution is active");
                if (!completion.Wait(250)) return Reply(request, StatusCode.Busy);
                var finished = completion.GetAwaiter().GetResult();
                return Reply(request, finished.Result, finished.Metrics, finished.Error);
            case "etl.diagnostics":
                return JsonReply(request, RequireExecution().Diagnostics());
            case "etl.cancel":
                RequireExecution().Cancel();
                return Reply(request, StatusCode.Ok);
            default:
                return Reply(request, StatusCode.Protocol);
        }
    }

    private static SdkSourceBinding[] ReadSources(JsonElement values) => values.TryGetProperty("sources", out var entries)
        ? entries.Deserialize<SdkSourceBinding[]>(WorkerProtocol.Json) ?? [] : [];

    private SdkExecutionEnvironment Scope(SdkSourceBinding[] sources)
    {
        var environment = createEnvironment(sources);
        if (sources.Length == 0) return environment;
        var bindings = sources.ToDictionary(SdkNativeSources.CredentialReference, StringComparer.Ordinal);
        var state = new SdkInvocationState(new(""), environment, sources, defaultSource: null);
        var credentials = new System.Collections.Concurrent.ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        return environment with { Resolve = (resource, reference) =>
        {
            if (!bindings.TryGetValue(reference, out var binding) || binding.Definition.Resource != resource)
                throw new UniversalDataException((int)StatusCode.Auth, "Native ETL credential cannot be resolved in this source snapshot");
            state.Require(binding, ConnectionAccess.Open);
            return credentials.GetOrAdd(reference, _ => binding.Definition.IsDatabase
                ? state.NativeConnection(binding) : state.SourceCredential(binding));
        } };
    }

    private static async ValueTask<SourceSeats> ReserveAsync(SdkExecutionEnvironment environment,
        IEnumerable<SdkSourceBinding> sources, CancellationToken cancellationToken)
    {
        var seats = new SourceSeats();
        try
        {
            foreach (var source in sources.OrderBy(source => source.Definition.Name, StringComparer.Ordinal))
            {
                var name = source.Definition.Name;
                if (!source.Enabled || environment.CanBorrow?.Invoke(name, source.Revision) == false ||
                    !environment.Authorize(source.Definition.Resource, ConnectionAccess.Open))
                    throw new UniversalDataException((int)StatusCode.Auth, "The named ETL database is unavailable");
                if (environment.ReserveConnection is { } reserve)
                    seats.Add(await reserve(name, cancellationToken));
            }
            return seats;
        }
        catch
        {
            await seats.DisposeAsync();
            throw;
        }
    }

    private Execution CreateExecution(JsonElement values, SdkExecutionEnvironment environment, string id = "", string? directory = null,
        bool resume = false, Catalog? record = null)
    {
        return policy.CreateExecution(Text(values, "package"), Text(values, "parameters"), id, directory, resume,
            observe: record is null ? null : record.Record,
            eventSequenceStart: record is null || id.Length == 0 ? 0 : record.LatestRevision(CatalogKind.Event, id),
            environment: environment);
    }

    private async ValueTask<(WorkerResponseFrame, WorkerBatchBuffer?)> StartAsync(WorkerRequestFrame request,
        CancellationToken cancellationToken)
    {
        if (execution is not null)
            throw new UniversalDataException((int)StatusCode.Busy, "A Native ETL execution already owns the Worker");
        var values = request.Values;
        string id = Text(values, "id");
        string ticket = Text(values, "ticket");
        bool resume = values.GetProperty("resume").GetBoolean();
        if (!Guid.TryParseExact(ticket, "N", out _) || !HostIdentity.ValidId(id))
            throw new ArgumentException("Invalid Native execution or Journal identity");

        var sources = ReadSources(values);
        var environment = Scope(sources);
        var seats = await ReserveAsync(environment, sources, cancellationToken);
        try
        {
            catalog = policy.OpenCatalog();
            execution = CreateExecution(values, environment, id, policy.ExecutionDirectory(ticket), resume, catalog);
            if (!resume)
            {
                string auditId = "cli/" + id;
                catalog.Put(CatalogKind.Audit, auditId, 1, JsonSerializer.Serialize(new
                {
                    version = ProductInfo.Protocol, id = auditId, ticket
                }, HostPolicy.JsonOptions));
            }
            catalog.Record(execution, checked(catalog.LatestRevision(CatalogKind.Execution, id) + 1));
            cancellationToken.ThrowIfCancellationRequested();
            execution.Start();
            var current = execution;
            var records = catalog;
            terminal = Task.Run(async () =>
            {
                await using (seats) return ObserveCompletion(current, records);
            });
            return Reply(request, StatusCode.Ok, new { id, ticket });
        }
        catch
        {
            await seats.DisposeAsync();
            throw;
        }
    }

    private static Terminal ObserveCompletion(Execution execution, Catalog catalog)
    {
        var result = StatusCode.Ok;
        OperationError? detail = null;
        try
        {
            while (!execution.Wait(250)) Thread.Yield();
        }
        catch (UniversalDataException error)
        {
            result = error.Code;
            detail = error.Detail;
        }

        var metrics = execution.Metrics;
        try
        {
            catalog.Record(execution, checked(catalog.LatestRevision(CatalogKind.Execution, execution.Id) + 1));
        }
        catch (UniversalDataException error)
        {
            result = result == StatusCode.Ok ? error.Code : result;
            detail ??= error.Detail;
        }
        return new(result, metrics, detail);
    }

    private Execution RequireExecution() => execution ??
        throw new UniversalDataException((int)StatusCode.Invalid, "No Native ETL execution is active");

    private static string Text(JsonElement values, string name) =>
        values.TryGetProperty(name, out var value) ? value.GetString() ?? "" : "";

    private static (WorkerResponseFrame, WorkerBatchBuffer?) JsonReply(WorkerRequestFrame request, string json)
    {
        using var document = JsonDocument.Parse(json);
        return Reply(request, StatusCode.Ok, document.RootElement);
    }

    private static (WorkerResponseFrame, WorkerBatchBuffer?) Reply(WorkerRequestFrame request, StatusCode status,
        object? value = null, OperationError? error = null) =>
        (new(ProductInfo.WorkerRevision, request.SessionId, request.Sequence, "result", status,
            value is null ? null : JsonSerializer.SerializeToElement(value, WorkerProtocol.Json), error), null);

    public void Dispose()
    {
        if (terminal is { IsCompleted: false } current)
        {
            execution!.Cancel();
            if (!current.Wait(TimeSpan.FromSeconds(policy.CancellationGraceSeconds))) return;
        }
        try { execution?.Dispose(); }
        finally { catalog?.Dispose(); }
    }

    private sealed record Terminal(StatusCode Result, ExecutionMetrics Metrics, OperationError? Error);

    private sealed class SourceSeats : IAsyncDisposable
    {
        private readonly Stack<IAsyncDisposable> seats = new();

        internal void Add(IAsyncDisposable seat) => seats.Push(seat);

        public async ValueTask DisposeAsync()
        {
            while (seats.TryPop(out var seat)) await seat.DisposeAsync();
        }
    }
}
