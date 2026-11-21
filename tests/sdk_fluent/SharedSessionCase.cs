using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using UniversalData;

namespace FluentAcceptance;

internal static class SharedSessionCase
{
    internal static async Task RunAsync(string policy, string orders, string ledger, string origin)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var token = deadline.Token;
        var builder = DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite("orders", "Data Source=" + orders)
            .AddSqlite("ledger", "Data Source=" + ledger);
        Jobs.AddHttpSources(builder, origin);
        await using var data = builder
            .ConfigureWorkers(w => w.MaxConcurrentSessions(4).MaxSessionsPerWorker(2)
                .MaxConnectionsPerSource(2).MaxBranchesPerSession(2).ExecutionTimeout(TimeSpan.FromSeconds(20))
                .CancellationGrace(TimeSpan.FromSeconds(2)))
            .Build();
        using var fixture = new HttpClient { BaseAddress = new Uri(origin), Timeout = TimeSpan.FromSeconds(5) };

        var overlap = await Task.WhenAll(Start(data, Jobs.OverlapOrders, token),
            Start(data, Jobs.OverlapLedger, token));
        try
        {
            Require(overlap[0].WorkerProcessId == overlap[1].WorkerProcessId &&
                overlap[0].WorkerProcessId != Environment.ProcessId,
                "Concurrent Native jobs must share one cooperative Worker process");
            await Task.WhenAll(overlap.Select(run => run.WaitAsync(token)));
        }
        finally { await DisposeAll(overlap); }
        Require(await Jobs.FirstTag(data, Jobs.Orders, token) == "orders-original" &&
            await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
            "Cooperative Native jobs must preserve independent SQLite reads");

        var held = await Start(data, Jobs.HoldSharedCancel, token);
        try
        {
            await Entered(fixture, "shared-cancel", token);
            var peer = await Start(data, Jobs.OverlapLedger, token);
            try
            {
                Require(peer.WorkerProcessId == held.WorkerProcessId,
                    "A cooperative peer must remain on the held Worker process");
                await peer.WaitAsync(token);
            }
            finally { await peer.DisposeAsync(); }

            await held.CancelAsync(token);
            await Release(fixture, "shared-cancel", token);
            try
            {
                await held.WaitAsync(token);
                throw new InvalidOperationException("The cancelled cooperative Native job returned success");
            }
            catch (UniversalDataException error) when (error.Code is StatusCode.Cancelled or StatusCode.CommitUnknown) { }
        }
        finally
        {
            await ReleaseIfEntered(fixture, "shared-cancel", token);
            await held.DisposeAsync();
        }
        Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
            "Cancelling one cooperative Native job must preserve the peer source");

        var failed = await Start(data, Jobs.Fail, token);
        try
        {
            await failed.WaitAsync(token);
            throw new InvalidOperationException("A shared Worker HTTP failure reported success");
        }
        catch (UniversalDataException error) when (error.Code != StatusCode.Ok) { }
        finally { await failed.DisposeAsync(); }
        Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
            "A failed cooperative Native job must leave its Worker reusable");

        var firstCrash = await Start(data, Jobs.HoldCrash, token);
        var secondCrash = await Start(data, Jobs.HoldCrash, token);
        try
        {
            await Entered(fixture, "crash", token);
            Require(firstCrash.WorkerProcessId == secondCrash.WorkerProcessId,
                "Cooperative crash coverage must use one physical Worker process");
            using (var process = Process.GetProcessById(firstCrash.WorkerProcessId))
                process.Kill(entireProcessTree: true);
            await Lost(firstCrash, token);
            await Lost(secondCrash, token);
        }
        finally
        {
            await ReleaseIfEntered(fixture, "crash", token);
            await firstCrash.DisposeAsync();
            await secondCrash.DisposeAsync();
        }
        Require(await Jobs.FirstTag(data, Jobs.Ledger, token) == "ledger",
            "A killed cooperative Worker must not poison a later Native job");

        Console.WriteLine(JsonSerializer.Serialize(new
        {
            SharedPid = overlap[0].WorkerProcessId,
            SharedBranches = overlap.Length,
            CooperativeCancellation = true,
            CooperativeCrash = true
        }));
    }

    private static Task<EtlRun> Start(DataClient data, DataJob<object> job, CancellationToken cancellation) =>
        data.StartAsync(job, new object(), cancellation);

    private static async Task DisposeAll(IEnumerable<EtlRun> runs)
    {
        foreach (var run in runs)
            await run.DisposeAsync();
    }

    private static async Task Entered(HttpClient fixture, string gate, CancellationToken cancellation)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        while (!await fixture.GetFromJsonAsync<bool>("entered/" + gate, deadline.Token))
            await Task.Delay(20, deadline.Token);
    }

    private static async Task Release(HttpClient fixture, string gate, CancellationToken cancellation)
    {
        using var response = await fixture.GetAsync("release/" + gate, cancellation);
        response.EnsureSuccessStatusCode();
    }

    private static async Task ReleaseIfEntered(HttpClient fixture, string gate, CancellationToken cancellation)
    {
        try { await Release(fixture, gate, cancellation); }
        catch (HttpRequestException) { }
        catch (OperationCanceledException) when (!cancellation.IsCancellationRequested) { }
    }

    private static async Task Lost(EtlRun run, CancellationToken cancellation)
    {
        try
        {
            await run.WaitAsync(cancellation);
            throw new InvalidOperationException("A physically terminated Worker returned Native success");
        }
        catch (UniversalDataException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown) { }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
