using System.Text.Json;
using SqlSugar;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal static class SdkWorker
{
    internal static async Task<int> Run()
    {
        var input = Console.OpenStandardInput();
        var output = Console.OpenStandardOutput();
        Console.SetOut(TextWriter.Null);
        NativeSqlWorker? sql = null;
        Execution? execution = null;
        Catalog? catalog = null;
        bool terminalRecorded = false;
        long previous = 0;
        try
        {
            while (true)
            {
                var frame = await WorkerProtocol.ReadAsync<WorkerRequestFrame>(input);
                using var receivedBatch = frame.Batch;
                var request = frame.Message;
                if (request.Revision != ProductInfo.WorkerRevision || request.Sequence != checked(previous + 1))
                    return (int)StatusCode.Protocol;
                previous = request.Sequence;
                (WorkerResponseFrame Message, WorkerBatchBuffer? Batch) reply;
                try
                {
                    if (request.Operation == "sql.openAuthorized")
                    {
                        if (sql is not null || execution is not null)
                            throw new UniversalDataException((int)StatusCode.Busy, "Worker session is open");
                        var bound = LoadPolicy(request.Values);
                        execution = bound.CreateExecution(NativeSqlWorker.Text(request.Values, "package"));
                        sql = new NativeSqlWorker(execution.OpenSqlSession);
                        reply = sql.Invoke(request with { Operation = "sql.open", Values =
                            JsonSerializer.SerializeToElement(new ConnectionConfig
                            { ConfigId = NativeSqlWorker.Text(request.Values, "connection") }, WorkerProtocol.Json) },
                            null, null);
                    }
                    else if (request.Operation.StartsWith("sql.", StringComparison.Ordinal))
                    {
                        sql ??= new NativeSqlWorker();
                        reply = sql.Invoke(request, frame.Batch, capacity =>
                        {
                            WorkerProtocol.WriteAsync(output, new WorkerResponseFrame(ProductInfo.WorkerRevision,
                                request.Sequence, "input", StatusCode.Ok, Capacity: capacity), null)
                                .AsTask().GetAwaiter().GetResult();
                            var batch = WorkerProtocol.ReadAsync<WorkerBatchFrame>(input)
                                .AsTask().GetAwaiter().GetResult();
                            return (batch.Message, batch.Batch);
                        });
                    }
                    else
                    {
                        if (frame.Batch is not null || sql is not null)
                            return (int)StatusCode.Protocol;
                        reply = RunEtl(request, ref execution, ref catalog, ref terminalRecorded);
                    }
                }
                catch (Exception error) when (error is UniversalDataException or IOException or InvalidDataException or JsonException or
                    ArgumentException or InvalidOperationException or UnauthorizedAccessException or DllNotFoundException)
                {
                    StatusCode code = error switch
                    {
                        UniversalDataException native => native.Code,
                        DllNotFoundException or FileNotFoundException => StatusCode.DriverUnavailable,
                        UnauthorizedAccessException => StatusCode.Auth,
                        InvalidDataException or JsonException => StatusCode.Protocol,
                        IOException => StatusCode.Io,
                        _ => StatusCode.Invalid
                    };
                    reply = NativeSqlWorker.Reply(request, (int)code,
                        error: (error as UniversalDataException)?.Detail);
                }
                using (reply.Batch)
                    await WorkerProtocol.WriteAsync(output, reply.Message, reply.Batch);
            }
        }
        catch (EndOfStreamException) { return 0; }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException)
        { return (int)StatusCode.Protocol; }
        finally
        {
            sql?.Dispose();
            execution?.Dispose();
            catalog?.Dispose();
        }
    }

    private static HostPolicy LoadPolicy(JsonElement request)
    {
        string path = NativeSqlWorker.Text(request, "policyFile");
        string tenant = NativeSqlWorker.Text(request, "tenant");
        if (!Path.IsPathFullyQualified(path) || !HostIdentity.ValidId(tenant))
            throw new ArgumentException("An absolute Worker policy and tenant are required");
        var host = HostPolicy.Load(path);
        HostIdentity identity = HostIdentity.Operator(tenant);
        return host.Tenants is null ? host with { Tenant = tenant, Identity = identity } : host.Bind(identity);
    }

    private static (WorkerResponseFrame, WorkerBatchBuffer?) RunEtl(WorkerRequestFrame request, ref Execution? execution,
        ref Catalog? catalog, ref bool terminalRecorded)
    {
        var values = request.Values;
        switch (request.Operation)
        {
            case "etl.instantiate":
                using (var package = new NativePackage(NativeSqlWorker.Text(values, "package"), LoadPolicy(values).MemoryLimit))
                using (var expanded = JsonDocument.Parse(package.ToJson()))
                    return NativeSqlWorker.Reply(request, 0, expanded.RootElement.Clone());
            case "etl.inspect":
            case "etl.catalog":
            case "etl.discover":
                using (var inspection = LoadPolicy(values).CreateExecution(NativeSqlWorker.Text(values, "package"),
                    NativeSqlWorker.Text(values, "parameters")))
                {
                    string json = request.Operation switch
                    {
                        "etl.inspect" => inspection.Explain(),
                        "etl.catalog" => inspection.ComponentCatalog(),
                        _ => inspection.DiscoverSchema(NativeSqlWorker.Text(values, "connection"),
                            NativeSqlWorker.Text(values, "table"))
                    };
                    using var document = JsonDocument.Parse(json);
                    return NativeSqlWorker.Reply(request, 0, document.RootElement.Clone());
                }
            case "etl.preview":
                using (var preview = LoadPolicy(values).CreateExecution(NativeSqlWorker.Text(values, "package"),
                    NativeSqlWorker.Text(values, "parameters")))
                {
                    string result = preview.Preview(values.GetProperty("pipeline").GetRawText(),
                        NativeSqlWorker.Text(values, "component"), values.GetProperty("sourceRows").GetUInt32(),
                        values.GetProperty("sampleRows").GetUInt32());
                    using var document = JsonDocument.Parse(result);
                    return NativeSqlWorker.Reply(request, 0, document.RootElement.Clone());
                }
            case "etl.start":
                if (execution is not null)
                    throw new UniversalDataException((int)StatusCode.Busy, "Worker execution is open");
                var policy = LoadPolicy(values);
                string id = NativeSqlWorker.Text(values, "id");
                string ticket = NativeSqlWorker.Text(values, "ticket");
                bool resume = values.GetProperty("resume").GetBoolean();
                if (!Guid.TryParseExact(ticket, "N", out _) || resume && !HostIdentity.ValidId(id))
                    throw new ArgumentException("Invalid Native execution or Journal identity");
                catalog = policy.OpenCatalog();
                var record = catalog;
                execution = policy.CreateExecution(NativeSqlWorker.Text(values, "package"),
                    NativeSqlWorker.Text(values, "parameters"), id, policy.ExecutionDirectory(ticket), resume,
                    observe: record.Record, eventSequenceStart: id.Length == 0 ? 0 :
                        record.LatestRevision(CatalogKind.Event, id));
                if (!resume)
                {
                    string key = "cli/" + execution.Id;
                    catalog.Put(CatalogKind.Audit, key, 1, JsonSerializer.Serialize(new
                    {
                        version = ProductInfo.Protocol, id = key, ticket
                    }, HostPolicy.JsonOptions));
                }
                catalog.Record(execution, checked(catalog.LatestRevision(CatalogKind.Execution, execution.Id) + 1));
                execution.Start();
                return NativeSqlWorker.Reply(request, 0, new { id = execution.Id, ticket });
            case "etl.progress":
                using (var progress = JsonDocument.Parse(Required(execution).Progress()))
                    return NativeSqlWorker.Reply(request, 0, progress.RootElement.Clone());
            case "etl.wait":
                var running = Required(execution);
                int status = 0;
                OperationError? error = null;
                try
                {
                    if (!running.Wait(250))
                        return NativeSqlWorker.Reply(request, (int)StatusCode.Busy, running.Metrics);
                }
                catch (UniversalDataException failure)
                {
                    status = failure.Status;
                    error = failure.Detail;
                }
                if (!terminalRecorded && catalog is not null)
                {
                    catalog.Record(running, checked(catalog.LatestRevision(CatalogKind.Execution, running.Id) + 1));
                    terminalRecorded = true;
                }
                return NativeSqlWorker.Reply(request, status, running.Metrics, error: error);
            case "etl.diagnostics":
                using (var diagnostics = JsonDocument.Parse(Required(execution).Diagnostics()))
                    return NativeSqlWorker.Reply(request, 0, diagnostics.RootElement.Clone());
            case "etl.cancel":
                Required(execution).Cancel();
                return NativeSqlWorker.Reply(request, 0);
            default:
                return NativeSqlWorker.Reply(request, (int)StatusCode.Protocol);
        }
    }

    private static Execution Required(Execution? execution) => execution ??
        throw new UniversalDataException((int)StatusCode.Invalid, "Worker execution is closed");
}
