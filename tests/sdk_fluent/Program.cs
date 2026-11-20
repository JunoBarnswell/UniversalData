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
if (args is not [var policy, var firstDatabase, var secondDatabase, var substitute, var address, var atomicMarker])
    throw new ArgumentException("Expected policy, two SQLite files, replacement file, HTTP origin and Atomic marker");

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(95));
var token = deadline.Token;
await using var data = DataClient.CreateBuilder(policy, "fluent")
    .AddSqlite("orders", "Data Source=" + firstDatabase)
    .AddSqlite("ledger", "Data Source=" + secondDatabase)
    .AddHttp("pricing", new Uri(address))
    .AddModule(typeof(Jobs).Assembly)
    .ConfigureWorkers(options => options.MaxConcurrentSessions(4).MaxBranchesPerSession(2)
        .MaxConnectionsPerSource(2).ExecutionTimeout(TimeSpan.FromSeconds(15))
        .CancellationGrace(TimeSpan.FromSeconds(2)))
    .Build();

Require(typeof(ISqlSugarClient).Assembly.GetName().Name == "SqlSugar", "The SDK must expose the official SqlSugar assembly");
Require(data.Sources.List().Count == 3, "Named source registration differs");
var probe = await data.Sources.ProbeAsync("orders", token);
Require(probe.Connected && probe.Source.Revision == 1, "A real SQLite source must be reachable");

var overlap = await Task.WhenAll(data.RunAsync(Jobs.Overlap, "first", token),
    data.RunAsync(Jobs.Overlap, "second", token));
Require(overlap[0].ProcessId != overlap[1].ProcessId && overlap.All(result =>
    result.ProcessId != Environment.ProcessId && result.Orders == "orders-original" && result.Ledger == "ledger"),
    "Parallel requests must overlap on separate physical Worker processes");

using var driver = new HttpClient { BaseAddress = new Uri(address), Timeout = TimeSpan.FromSeconds(5) };
int sessionPid;
await using (var session = await data.OpenSessionAsync(token))
{
    sessionPid = session.ProcessId;
    var before = await session.RunAsync(Jobs.Scope, "orders", token);
    Require(before.ProcessId == sessionPid && before.Value == "orders-original", "Explicit session must use its leased process");

    var inFlight = session.RunAsync(Jobs.WaitThenRead, new HeldRead("replace", "orders"), token);
    await Entered(driver, "replace", token);
    var source = data.Sources.Get("orders");
    var selected = new DataSourceDefinition("orders", DataSourceKind.Sqlite, "Data Source=" + substitute);
    Require(data.Sources.Replace("orders", source.Revision, selected).Revision == source.Revision + 1,
        "A replacement must increment the source revision");
    try
    {
        data.Sources.Replace("orders", source.Revision, selected);
        throw new InvalidOperationException("A stale registry revision was accepted");
    }
    catch (UniversalDataException error) when (error.Code == StatusCode.Fenced) { }
    await Release(driver, "replace", token);
    Require(await inFlight == "orders-original", "In-flight work must resolve its original source snapshot");
    var after = await session.RunAsync(Jobs.Scope, "orders", token);
    Require(after.ProcessId == sessionPid && after.Value == "orders-replacement" && after.StaleScopeRejected,
        "The next invocation must reuse the PID, refresh source revisions and invalidate the previous context");

    var isolated = session.RunAsync(Jobs.WaitThenRead, new HeldRead("slow", "orders"), token);
    await Entered(driver, "slow", token);
    try
    {
        var fast = await data.RunAsync(Jobs.Scope, "ledger", token).WaitAsync(TimeSpan.FromSeconds(8), token);
        Require(fast.ProcessId != sessionPid && fast.Value == "ledger" && !isolated.IsCompleted,
            "A held HTTP request must leave a different session runnable");
    }
    finally { await Release(driver, "slow", token); }
    Require(await isolated == "orders-replacement", "The held request should still finish in its own session");

    try
    {
        await Task.WhenAll(data.RunAsync(Jobs.Fail, token), session.RunAsync(Jobs.VerifyLedger, token));
        throw new InvalidOperationException("The Worker suppressed the business failure");
    }
    catch (DataBusinessException error) when (error.RemoteType == typeof(InvalidOperationException).FullName &&
        error.Message.Contains("settlement rejected", StringComparison.Ordinal)) { }
    catch (InvalidOperationException error) when (error.Message == "settlement rejected") { }
    Require(await session.RunAsync(Jobs.Scope, "ledger", token) is { Value: "ledger", ProcessId: var unchangedPid } && unchangedPid == sessionPid,
        "A business failure must leave the other Worker session usable");

    await using (var doomed = await data.OpenSessionAsync(token))
    {
        Require(doomed.ProcessId != sessionPid, "A physical crash fixture must own an independent process");
        try
        {
            await doomed.RunAsync(Jobs.Crash, token);
            throw new InvalidOperationException("A crashed Worker reported success");
        }
        catch (UniversalDataException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown) { }
        await session.RunAsync(Jobs.VerifyLedger, token);
    }

    using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
    await using (var cancelled = await data.OpenSessionAsync(token))
    {
        var hold = cancelled.RunAsync(Jobs.WaitThenRead, new HeldRead("cancel", "ledger"), requestCancellation.Token);
        await Entered(driver, "cancel", token);
        requestCancellation.Cancel();
        try
        {
            await hold.WaitAsync(TimeSpan.FromSeconds(8), token);
            throw new InvalidOperationException("A cancelled Worker returned a result");
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
        catch (UniversalDataException error) when (error.Code is StatusCode.Cancelled or StatusCode.CommitUnknown) { }
        finally { await Release(driver, "cancel", token); }
        await session.RunAsync(Jobs.VerifyLedger, token);
    }

    await using (var timed = await data.OpenSessionAsync(token))
    {
        var timeoutPid = timed.ProcessId;
        var uncooperative = timed.RunAsync(Jobs.IgnoreCancellation, token);
        await Entered(driver, "timeout", token);
        await session.RunAsync(Jobs.VerifyLedger, token);
        try
        {
            await uncooperative.WaitAsync(TimeSpan.FromSeconds(20), token);
            throw new InvalidOperationException("A deadline did not stop an uncooperative Worker");
        }
        catch (DataExecutionException error) when (error.Code == StatusCode.Timeout || error.Detail?.Cause == StatusCode.Timeout) { }
        await Exited(timeoutPid, token);
        await session.RunAsync(Jobs.VerifyLedger, token);
    }
}
await Exited(sessionPid, token);

try
{
    await data.RunAsync(Jobs.UnsupportedAtomic, atomicMarker, token);
    throw new InvalidOperationException("An Atomic scope admitted independent SQLite participants");
}
catch (UniversalDataException error) when (error.Code == StatusCode.Unsupported) { }
Require(!File.Exists(atomicMarker), "An unsupported Atomic scope must reject admission before invoking business code");
await data.RunAsync(LocalOrmCase.Run, token);

var last = data.Sources.Disable("orders", data.Sources.Get("orders").Revision);
Require(!last.Enabled, "A disabled source must be visible in the registry");
try
{
    await data.RunAsync(Jobs.Scope, "orders", token);
    throw new InvalidOperationException("A new request borrowed a disabled source");
}
catch (UniversalDataException error) when (error.Code is StatusCode.Auth or StatusCode.Fenced or StatusCode.Invalid) { }

Console.WriteLine(JsonSerializer.Serialize(new { Requests = overlap, ReusedProcess = sessionPid, PhysicalCrash = true, AtomicAdmission = true }));

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

static async Task Exited(int id, CancellationToken cancellation)
{
    try
    {
        using var process = Process.GetProcessById(id);
        await process.WaitForExitAsync(cancellation).WaitAsync(TimeSpan.FromSeconds(6), cancellation);
    }
    catch (ArgumentException) { }
}

static void Require(bool value, string message)
{
    if (!value) throw new InvalidOperationException(message);
}
