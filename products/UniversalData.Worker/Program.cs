using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args.Length != 1) throw new ArgumentException("Worker requires a host-policy file");
var policy = HostPolicy.Load(Path.GetFullPath(args[0]));
var input = await WorkerProtocol.ReadLine(Console.In, 67108864) ?? throw new InvalidDataException("Missing worker request");
var request = JsonSerializer.Deserialize<WorkerRequest>(input, HostPolicy.JsonOptions)
              ?? throw new InvalidDataException("Invalid worker request");
if (request.Version != ProductInfo.Protocol || !Guid.TryParseExact(request.Ticket, "N", out _))
    throw new InvalidDataException("Incompatible worker request");
Directory.CreateDirectory(policy.ExecutionDirectory(request.Ticket));
using var catalog = policy.OpenCatalog();
using var execution = policy.CreateExecution(request.Package, request.Parameters, request.ExecutionId,
    policy.ExecutionDirectory(request.Ticket), request.Resume, observe: catalog.Record,
    eventSequenceStart: request.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Event, request.ExecutionId));
var output = new object();
if (request.Debug is { } debug) execution.ConfigureDebug(debug.GetRawText());
void Reply(object value) { lock (output) { Console.WriteLine(JsonSerializer.Serialize(value, HostPolicy.JsonOptions)); } }
catalog.Record(execution, request.Resume ? request.Revision + 1 : 1);
Reply(new { version = ProductInfo.Protocol, kind = "ready", ticket = request.Ticket, executionId = execution.Id });
// A worker cannot perform business I/O until the service records its Native ID and authorizes start.
var start = await WorkerProtocol.ReadLine(Console.In, 64);
if (start != "start") return 2;
execution.Start();
using var controls = new CancellationTokenSource();
var controlServer = WorkerControl.Serve(request.Ticket, execution, controls.Token);
_ = Task.Run(async () =>
{
    while (await WorkerProtocol.ReadLine(Console.In, 64) is { } command)
        if (command == "cancel") execution.Cancel();
    execution.Cancel(); /* Service pipe loss requests cancellation; it never authorizes replay. */
});
int result = 0;
try { while (!execution.Wait(200)) { } }
catch (UniversalDataException error) { result = error.Status; }
controls.Cancel();
await controlServer;
var metrics = execution.Metrics;
catalog.Record(execution, request.Resume ? request.Revision + 2 : 2);
Reply(new { version = ProductInfo.Protocol, kind = "terminal", ticket = request.Ticket, executionId = execution.Id,
    state = metrics.State.ToString(), result, metrics, diagnostics = execution.Diagnostics() });
return result == 0 ? 0 : 1;

sealed record WorkerRequest(int Version, string Ticket, string Package, string Parameters = "{}",
    string ExecutionId = "", bool Resume = false, ulong Revision = 0, JsonElement? Debug = null);
