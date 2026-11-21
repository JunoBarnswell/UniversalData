using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal sealed class SdkEtlSession(HostPolicy policy,
    SdkExecutionEnvironment environment, int cpu) : IDisposable
{
    // Keep the decoded definition for one small recurring job on each logical session.
    private const int CachedPackageCharacters = 64 * 1024;
    private string? cachedDocument;
    private NativePackageDefinition? cachedDefinition;
    private Execution? execution;
    private Catalog? catalog;
    private Task<Terminal>? terminal;
    private Terminal? completed;
    private long startSequence;
    private string? executionId;

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
                ValidateSources(environment, inspected, cancellationToken);
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
                    .Where(node => node.GetProperty("component").GetString()?.EndsWith(".source", StringComparison.Ordinal) == true)
                    .Select(node => Text(node.GetProperty("config"), "connection")).ToHashSet(StringComparer.Ordinal);
                var previewSources = ReadSources(values);
                var previewScope = Scope(previewSources);
                ValidateSources(previewScope, previewSources.Where(source => selected.Contains(source.Definition.Name)),
                    cancellationToken);
                using (var preview = CreateExecution(values, previewScope))
                    return JsonReply(request, preview.Preview(pipeline.GetRawText(),
                        Text(values, "component"), values.GetProperty("sourceRows").GetUInt32(),
                        values.GetProperty("sampleRows").GetUInt32()));
            case "etl.messageConnection":
                var messageSources = ReadSources(values);
                var messageEnvironment = Scope(messageSources);
                var messageConnection = Text(values, "connection");
                ValidateSources(messageEnvironment,
                    messageSources.Where(source => source.Definition.Name == messageConnection), cancellationToken);
                using (var messageExecution = CreateExecution(values, messageEnvironment))
                {
                    var test = messageExecution.TestMessageConnection(messageConnection);
                    return Reply(request, StatusCode.Ok,
                        new { capabilities = test.Capabilities, elapsedMs = test.ElapsedMs });
                }
            case "etl.start":
                return Start(request, cancellationToken);
            case "etl.next":
                var next = RequireExecution(request).ReadResult(250, out var batch);
                if (next == StatusCode.Ok)
                    return BatchReply(request, batch);
                batch?.Dispose();
                if (next == StatusCode.End)
                    return await CompleteAsync(request, streamEnd: true, cancellationToken);
                return Reply(request, next);
            case "etl.progress":
                return JsonReply(request, RequireExecution(request).Progress());
            case "etl.wait":
                return await CompleteAsync(request, streamEnd: false, cancellationToken);
            case "etl.diagnostics":
                return JsonReply(request, RequireExecution(request).Diagnostics());
            case "etl.stop":
                RequireExecution(request).Stop();
                return Reply(request, StatusCode.Ok);
            case "etl.cancel":
                RequireExecution(request).Cancel();
                return Reply(request, StatusCode.Ok);
            default:
                return Reply(request, StatusCode.Protocol);
        }
    }

    private static SdkSourceBinding[] ReadSources(JsonElement values) => values.TryGetProperty("sources", out var entries)
        ? entries.Deserialize<SdkSourceBinding[]>(WorkerProtocol.Json) ?? [] : [];

    private SdkExecutionEnvironment Scope(SdkSourceBinding[] sources)
    {
        if (sources.Length == 0) return environment;
        var bindings = sources.ToDictionary(SdkNativeSources.CredentialReference, StringComparer.Ordinal);
        var credentials = new System.Collections.Concurrent.ConcurrentDictionary<string, string>(StringComparer.Ordinal);
        var fallback = environment.Resolve;
        string Credential(SdkSourceBinding binding)
        {
            if (!binding.Enabled || environment.CanBorrow?.Invoke(binding.Definition.Name, binding.Revision) == false ||
                !environment.Authorize(binding.Definition.Resource, ConnectionAccess.Open))
                throw new UniversalDataException((int)StatusCode.Auth,
                    "The named ETL source is unavailable in the current Worker scope");
            return credentials.GetOrAdd(SdkNativeSources.CredentialReference(binding), _ =>
            {
                var definition = binding.Definition;
                if (definition.NativeConnectionString is { } native) return native;
                if (definition.CredentialReference is { } reference)
                    return fallback?.Invoke(definition.Resource, reference)
                        ?? throw new UniversalDataException((int)StatusCode.Auth,
                            "The source credential is unavailable in the current Worker scope");
                return definition.ConnectionString;
            });
        }
        return environment with { Resolve = (resource, reference) =>
        {
            if (!reference.StartsWith("sdk-source/", StringComparison.Ordinal))
                return fallback?.Invoke(resource, reference)
                    ?? throw new UniversalDataException((int)StatusCode.Auth,
                        "Native ETL credential cannot be resolved in this Worker scope");
            if (!bindings.TryGetValue(reference, out var binding) || binding.Definition.Resource != resource)
                throw new UniversalDataException((int)StatusCode.Auth, "Native ETL credential cannot be resolved in this source snapshot");
            string credential = Credential(binding);
            return binding.Definition.IsDatabase
                ? NativeSqlConfiguration.Create(SqlNative.Dialect(binding.Definition.Kind), credential)
                : credential;
        } };
    }

    private static void ValidateSources(SdkExecutionEnvironment environment,
        IEnumerable<SdkSourceBinding> sources, CancellationToken cancellationToken)
    {
        foreach (var source in sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!source.Enabled || environment.CanBorrow?.Invoke(source.Definition.Name, source.Revision) == false ||
                !environment.Authorize(source.Definition.Resource, ConnectionAccess.Open))
                throw new UniversalDataException((int)StatusCode.Auth, "The named ETL source is unavailable");
        }
    }

    private Execution CreateExecution(JsonElement values, SdkExecutionEnvironment environment, string id = "", string? directory = null,
        bool resume = false, Catalog? record = null, NativePackageDefinition? prepared = null,
        TaskCompletionSource? finished = null)
    {
        Action<ExecutionEvent>? observe = record is null ? null : record.Record;
        if (record is not null && finished is not null)
            observe = item =>
            {
                try { record.Record(item); }
                finally
                {
                    if (item.Kind == ExecutionEventKind.State && item.TaskId.Length == 0 &&
                        item.State is ExecutionState.Succeeded or ExecutionState.Failed or ExecutionState.Cancelled or
                            ExecutionState.Stopped or ExecutionState.RecoveryRequired)
                        finished.TrySetResult();
                }
            };
        return policy.CreateExecution(prepared?.Json ?? Text(values, "package"), Text(values, "parameters"), id, directory, resume,
            observe: observe,
            eventSequenceStart: record is null || id.Length == 0 ? 0 : record.LatestRevision(CatalogKind.Event, id),
            environment: environment, prepared: prepared);
    }

    private NativePackageDefinition PrepareDefinition(string document)
    {
        if (cachedDefinition is { } current && cachedDocument == document)
            return current;
        var prepared = NativePackageDefinition.Read(document, policy.MemoryLimit);
        if (document.Length <= CachedPackageCharacters)
        {
            cachedDocument = document;
            cachedDefinition = prepared;
        }
        return prepared;
    }

    private (WorkerResponseFrame, WorkerBatchBuffer?) Start(WorkerRequestFrame request,
        CancellationToken cancellationToken)
    {
        if (execution is { } previous)
        {
            if (previous.Metrics.State is ExecutionState.Created or ExecutionState.Validating or
                ExecutionState.Ready or ExecutionState.Running or ExecutionState.Draining)
                throw new UniversalDataException((int)StatusCode.Busy, "A Native ETL execution already owns this session");
            Dispose();
        }
        var values = request.Values;
        string id = Text(values, "id");
        string ticket = Text(values, "ticket");
        bool resume = values.GetProperty("resume").GetBoolean();
        bool readResults = values.TryGetProperty("readResults", out var resultFlag) && resultFlag.GetBoolean();
        if (!Guid.TryParseExact(ticket, "N", out _) || !HostIdentity.ValidId(id))
            throw new ArgumentException("Invalid Native execution or Journal identity");

        var sources = ReadSources(values);
        var environment = Scope(sources);
        ValidateSources(environment, sources, cancellationToken);
        var package = PrepareDefinition(Text(values, "package"));
        if (!readResults || package.RequiresJournal)
            catalog = policy.OpenCatalog();
        var finished = catalog is not null && !readResults
            ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : null;
        execution = CreateExecution(values, environment, id, policy.ExecutionDirectory(ticket), resume, catalog, package, finished);
        if (catalog is not null)
        {
            if (!resume)
            {
                string auditId = "cli/" + id;
                catalog.Put(CatalogKind.Audit, auditId, 1, JsonSerializer.Serialize(new
                {
                    version = ProductInfo.Protocol, id = auditId, ticket
                }, HostPolicy.JsonOptions));
            }
            catalog.Record(execution, checked(catalog.LatestRevision(CatalogKind.Execution, id) + 1));
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (readResults) execution.OpenResults();
        if (cpu >= 0) execution.StartOnCpu((uint)cpu);
        else execution.Start();
        startSequence = request.Sequence;
        executionId = id;
        if (finished is not null) terminal = ObserveCompletionAsync(execution, catalog!, finished.Task);
        return Reply(request, StatusCode.Ok, new { id, ticket });
    }

    private async ValueTask<(WorkerResponseFrame, WorkerBatchBuffer?)> CompleteAsync(WorkerRequestFrame request,
        bool streamEnd, CancellationToken cancellation)
    {
        var active = RequireExecution(request);
        Terminal? result;
        if (terminal is { } durable)
        {
            if (!durable.IsCompleted && await Task.WhenAny(durable, Task.Delay(250, cancellation)) != durable)
            {
                cancellation.ThrowIfCancellationRequested();
                return Reply(request, StatusCode.Busy);
            }
            result = await durable;
        }
        else
            result = ReadCompletion(active);
        if (result is null) return Reply(request, StatusCode.Busy);
        if (streamEnd && result.Result == StatusCode.Ok) return Reply(request, StatusCode.End);
        return Reply(request, result.Result, result.Metrics, result.Error);
    }

    private Terminal? ReadCompletion(Execution active)
    {
        if (completed is not null) return completed;
        var result = WaitForCompletion(active);
        if (result is null) return null;
        return completed = catalog is { } records ? RecordCompletion(active, records, result) : result;
    }

    private static Terminal? WaitForCompletion(Execution execution, uint timeoutMilliseconds = 250)
    {
        try
        {
            return execution.Wait(timeoutMilliseconds) ? new(StatusCode.Ok, execution.Metrics, null) : null;
        }
        catch (UniversalDataException error)
        {
            return new(error.Code, execution.Metrics, error.Detail);
        }
    }

    private static async Task<Terminal> ObserveCompletionAsync(Execution execution, Catalog catalog, Task finished)
    {
        await finished;
        var result = WaitForCompletion(execution, 0)
            ?? throw new InvalidDataException("Native execution notified completion before reaching a terminal state");
        return RecordCompletion(execution, catalog, result);
    }

    private static Terminal RecordCompletion(Execution execution, Catalog catalog, Terminal finished)
    {
        try
        {
            catalog.Record(execution, checked(catalog.LatestRevision(CatalogKind.Execution, execution.Id) + 1));
        }
        catch (UniversalDataException error)
        {
            return finished with { Result = finished.Result == StatusCode.Ok ? error.Code : finished.Result,
                Error = finished.Error ?? error.Detail };
        }
        return finished;
    }

    private Execution RequireExecution(WorkerRequestFrame request)
    {
        if (execution is null || !request.Values.TryGetProperty("startSequence", out var owner) ||
            !owner.TryGetInt64(out var sequence) || startSequence != sequence || executionId != Text(request.Values, "id"))
            throw new UniversalDataException((int)StatusCode.Fenced, "The Native execution no longer owns this session");
        return execution;
    }

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

    private static (WorkerResponseFrame, WorkerBatchBuffer?) BatchReply(WorkerRequestFrame request,
        WorkerBatchBuffer? batch)
    {
        if (batch is null)
            throw new UniversalDataException((int)StatusCode.Protocol,
                "Native result reader returned no wire batch");
        return (new(ProductInfo.WorkerRevision, request.SessionId, request.Sequence, "result", StatusCode.Ok,
            JsonSerializer.SerializeToElement(new { length = batch.Length }, WorkerProtocol.Json)), batch);
    }

    public void Dispose()
    {
        if (execution is { } current)
        {
            try
            {
                if (current.Metrics.State is ExecutionState.Running or ExecutionState.Draining)
                {
                    current.Cancel();
                    if (!current.Wait(policy.CancellationGraceSeconds * 1000))
                        throw new UniversalDataException((int)StatusCode.Timeout, "Native ETL did not stop within the Worker cancellation budget");
                }
            }
            catch (UniversalDataException error) when (error.Code != StatusCode.Timeout ||
                current.Metrics.State is not (ExecutionState.Running or ExecutionState.Draining)) { }
            if (terminal is { } completion && !completion.Wait(TimeSpan.FromSeconds(policy.CancellationGraceSeconds)))
                throw new UniversalDataException((int)StatusCode.Timeout, "Native ETL did not record a terminal state within the Worker cancellation budget");
            if (terminal is null && catalog is not null &&
                current.Metrics.State is not (ExecutionState.Created or ExecutionState.Validating or ExecutionState.Ready))
                _ = ReadCompletion(current);
        }
        try { execution?.Dispose(); }
        finally
        {
            catalog?.Dispose();
            execution = null;
            catalog = null;
            terminal = null;
            completed = null;
            startSequence = 0;
            executionId = null;
        }
    }

    private sealed record Terminal(StatusCode Result, ExecutionMetrics Metrics, OperationError? Error);
}
