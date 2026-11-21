using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAcceptance;
using SqlSugar;
using UniversalData;

if (args is ["--atomic", var atomicFixture, .. var pluginRoots])
{
    await AtomicCase.RunAsync(atomicFixture, pluginRoots);
    return;
}
if (args is ["--atomic-decision", var decisionFixture, var decisionArms, .. var decisionPluginRoots])
{
    await AtomicCase.RunAsync(decisionFixture, decisionPluginRoots, decisionArms);
    return;
}
if (args is ["--latency", var latencyPolicy, var slowDatabase, var fastDatabase, var latencyOrigin])
{
    await LatencyCase.RunAsync(latencyPolicy, slowDatabase, fastDatabase, latencyOrigin);
    return;
}
if (args is ["--mq-burst", var mqPolicy, var mqCredential, var queue, var databaseDirectory,
    var messages, var concurrency, var seconds])
{
    await MqBurstCase.RunAsync(mqPolicy, mqCredential, queue, databaseDirectory,
        ulong.Parse(messages), uint.Parse(concurrency), int.Parse(seconds));
    return;
}
if (args is ["--shared", var sharedPolicy, var sharedOrders, var sharedLedger, var sharedOrigin])
{
    await SharedSessionCase.RunAsync(sharedPolicy, sharedOrders, sharedLedger, sharedOrigin);
    return;
}
if (args is ["--node-capacity", var nodePolicy, var nodeDatabase, _, var nodeOrigin])
{
    await NodeCapacityCase.RunAsync(nodePolicy, nodeDatabase, nodeOrigin);
    return;
}
if (args is ["--native-orm", var ormFixture, .. var ormPluginRoots])
{
    await NativeOrmCase.RunAsync(ormFixture, ormPluginRoots);
    return;
}
if (args is ["--management-cdc", var cdcPolicy, var managementAddress, var sourceTable,
    var cdcSlot, var cdcPublication, var cdcPosition, var cdcTarget, var cdcSubject, var cdcPassword])
{
    await ManagementCdcCase.RunAsync(cdcPolicy, managementAddress, sourceTable, cdcSlot,
        cdcPublication, cdcPosition, cdcTarget, cdcSubject, cdcPassword);
    return;
}
if (args is not [var policy, var firstDatabase, var secondDatabase, var substitute, var address, var atomicMarker])
    throw new ArgumentException("Expected policy, two SQLite files, replacement file, HTTP origin and Atomic marker");

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(95));
var token = deadline.Token;
var builder = DataClient.CreateBuilder(policy, "fluent")
    .AddSqlite("orders", "Data Source=" + firstDatabase, options => options.ProvisionTransactionReceipts = true)
    .AddSqlite("ledger", "Data Source=" + secondDatabase);
Jobs.AddHttpSources(builder, address);
await using var data = builder
    .ConfigureWorkers(options => options.MaxConcurrentSessions(4).MaxBranchesPerSession(2)
        .MaxConnectionsPerSource(2).ExecutionTimeout(TimeSpan.FromSeconds(15))
        .CancellationGrace(TimeSpan.FromSeconds(2)))
    .Build();

Require(typeof(ISqlSugarClient).Assembly.GetName().Name == "SqlSugar",
    "The SDK must expose the official SqlSugar assembly");
Require(data.Sources.List().Count >= 3, "Named source registration differs");
var probe = await data.Sources.ProbeAsync("orders", token);
Require(probe.Connected && probe.Source.Revision == 1, "A real SQLite source must be reachable");
await LocalOrmCase.Run(data, token);

var overlap = await Task.WhenAll(Start(data, Jobs.OverlapOrders, token),
    Start(data, Jobs.OverlapLedger, token));
try
{
    Require(overlap[0].WorkerProcessId != overlap[1].WorkerProcessId &&
        overlap.All(run => run.WorkerProcessId != Environment.ProcessId),
        "Parallel Native jobs must use separate physical Worker processes");
    await Task.WhenAll(overlap.Select(run => run.WaitAsync(token)));
}
finally { await DisposeAll(overlap); }
Require(await Jobs.FirstTag(data, Jobs.Orders, token) == "orders-original" &&
    await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
    "Parallel Native jobs must preserve independent SQLite reads");

using var driver = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(5) };
var heldReplacement = await Start(data, Jobs.HoldOrdersReplace, token);
try
{
    await Entered(driver, "replace", token);
    var heldStale = await Start(data, Jobs.HoldOrdersStale, token);
    try
    {
        await Entered(driver, "stale-borrow", token);
        var source = data.Sources.Get("orders");
        var selected = new DataSourceDefinition("orders", DataSourceKind.Sqlite,
            "Data Source=" + substitute);
        Require(data.Sources.Replace("orders", source.Revision, selected).Revision == source.Revision + 1,
            "A replacement must increment the source revision");
        try
        {
            data.Sources.Replace("orders", source.Revision, selected);
            throw new InvalidOperationException("A stale registry revision was accepted");
        }
        catch (UniversalDataException error) when (error.Code == StatusCode.Fenced) { }
        await Release(driver, "replace", token);
        await Release(driver, "stale-borrow", token);
        await heldReplacement.WaitAsync(token);
        try
        {
            await heldStale.WaitAsync(token);
            throw new InvalidOperationException("A stale Native source revision was admitted after replacement");
        }
        catch (UniversalDataException error) when (error.Code is StatusCode.Auth or StatusCode.Fenced) { }
    }
    finally { await heldStale.DisposeAsync(); }
}
finally
{
    await ReleaseIfEntered(driver, "replace", token);
    await heldReplacement.DisposeAsync();
}
Require(await Jobs.FirstTag(data, Jobs.Orders, token) == "orders-replacement",
    "A newly compiled Native job must use the replacement source revision");

var slow = await Start(data, Jobs.HoldOrdersSlow, token);
try
{
    await Entered(driver, "slow", token);
    var fast = await Start(data, Jobs.OverlapLedger, token);
    try
    {
        Require(fast.WorkerProcessId != slow.WorkerProcessId,
            "A held HTTP request must leave a different Native Worker runnable");
        await fast.WaitAsync(token);
    }
    finally { await fast.DisposeAsync(); }
    await Release(driver, "slow", token);
    await slow.WaitAsync(token);
}
finally
{
    await ReleaseIfEntered(driver, "slow", token);
    await slow.DisposeAsync();
}

var failed = await Start(data, Jobs.Fail, token);
try
{
    await failed.WaitAsync(token);
    throw new InvalidOperationException("A Native HTTP failure reported success");
}
catch (UniversalDataException error) when (error.Code != StatusCode.Ok) { }
finally { await failed.DisposeAsync(); }
Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
    "A failed Native job must leave the next Worker usable");

var doomed = await Start(data, Jobs.HoldCrash, token);
try
{
    await Entered(driver, "crash", token);
    using (var process = Process.GetProcessById(doomed.WorkerProcessId))
        process.Kill(entireProcessTree: true);
    try
    {
        await doomed.WaitAsync(token);
        throw new InvalidOperationException("A killed Native Worker reported success");
    }
    catch (UniversalDataException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown) { }
}
finally
{
    await ReleaseIfEntered(driver, "crash", token);
    await doomed.DisposeAsync();
}
Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
    "A killed Native Worker must not poison the next job");

var cancelled = await Start(data, Jobs.HoldLedgerCancel, token);
try
{
    await Entered(driver, "cancel", token);
    await cancelled.CancelAsync(token);
    await Release(driver, "cancel", token);
    try
    {
        await cancelled.WaitAsync(token);
        throw new InvalidOperationException("A cancelled Native Worker returned success");
    }
    catch (UniversalDataException error) when (error.Code is StatusCode.Cancelled or StatusCode.CommitUnknown) { }
}
finally
{
    await ReleaseIfEntered(driver, "cancel", token);
    await cancelled.DisposeAsync();
}
Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
    "Worker cancellation must preserve a subsequent Native job");

var timed = await Start(data, Jobs.HoldTimeout, token);
try
{
    await Entered(driver, "timeout", token);
    await timed.CancelAsync(token);
    await Release(driver, "timeout", token);
    try { await timed.WaitAsync(token); }
    catch (UniversalDataException error) when (error.Code is StatusCode.Cancelled or StatusCode.CommitUnknown) { }
}
finally
{
    await ReleaseIfEntered(driver, "timeout", token);
    await timed.DisposeAsync();
}

EtlRun? unsupported = null;
try
{
    var run = await Start(data, Jobs.UnsupportedAtomic, token);
    unsupported = run;
    await run.WaitAsync(token);
    throw new InvalidOperationException("An Atomic scope admitted independent SQLite participants");
}
catch (UniversalDataException error) when (error.Code == StatusCode.Unsupported) { }
finally
{
    if (unsupported is not null) await unsupported.DisposeAsync();
}
Require(!await Jobs.AtomicProbeExists(data, token) && !File.Exists(atomicMarker),
    "An unsupported Atomic scope must reject admission before changing either SQLite file");

var last = data.Sources.Disable("orders", data.Sources.Get("orders").Revision);
Require(!last.Enabled, "A disabled source must be visible in the registry");
try
{
    await Jobs.FirstTag(data, Jobs.Orders, token);
    throw new InvalidOperationException("A new Native job borrowed a disabled source");
}
catch (UniversalDataException error) when (error.Code is StatusCode.Auth or StatusCode.Fenced or StatusCode.Invalid) { }

Console.WriteLine(JsonSerializer.Serialize(new
{
    Workers = overlap.Select(run => run.WorkerProcessId).ToArray(),
    SourceRevision = last.Revision,
    NativeFailure = true,
    NativeCancellation = true,
    AtomicAdmission = true
}));

static Task<EtlRun> Start(DataClient data, DataJob<object> job, CancellationToken cancellation) =>
    data.StartAsync(job, new object(), cancellation);

static async Task DisposeAll(IEnumerable<EtlRun> runs)
{
    foreach (var run in runs)
        await run.DisposeAsync();
}

static async Task Entered(HttpClient driver, string key, CancellationToken cancellation)
{
    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
    deadline.CancelAfter(TimeSpan.FromSeconds(10));
    while (!await driver.GetFromJsonAsync<bool>("entered/" + key, deadline.Token))
        await Task.Delay(20, deadline.Token);
}

static async Task Release(HttpClient driver, string key, CancellationToken cancellation)
{
    using var reply = await driver.GetAsync("release/" + key, cancellation);
    reply.EnsureSuccessStatusCode();
}

static async Task ReleaseIfEntered(HttpClient driver, string key, CancellationToken cancellation)
{
    try { await Release(driver, key, cancellation); }
    catch (HttpRequestException) { }
    catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { }
}

static void Require(bool value, string message)
{
    if (!value) throw new InvalidOperationException(message);
}
