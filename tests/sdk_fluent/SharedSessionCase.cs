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
        await using var data = DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite("orders", "Data Source=" + orders)
            .AddSqlite("ledger", "Data Source=" + ledger)
            .AddHttp("pricing", new Uri(origin))
            .AddModule(typeof(Jobs).Assembly)
            .ConfigureWorkers(w => w.MaxConcurrentSessions(4).MaxSessionsPerWorker(2)
                .MaxConnectionsPerSource(2).MaxBranchesPerSession(2).ExecutionTimeout(TimeSpan.FromSeconds(20))
                .CancellationGrace(TimeSpan.FromSeconds(2)))
            .Build();
        using var fixture = new HttpClient { BaseAddress = new Uri(origin), Timeout = TimeSpan.FromSeconds(5) };
        await using var first = await data.OpenSessionAsync(token);
        await using var peer = await data.OpenSessionAsync(token);
        Require(first.ProcessId == peer.ProcessId && first.ProcessId != Environment.ProcessId,
            "Two cooperative sessions must share one physical Worker PID");
        var overlap = await Task.WhenAll(first.RunAsync(Jobs.Overlap, "first", token),
            peer.RunAsync(Jobs.Overlap, "second", token));
        Require(overlap.All(row => row.ProcessId == peer.ProcessId && row.Orders == "orders-original" && row.Ledger == "ledger"),
            "Every concurrent branch must read its own source in the shared Worker");
        var firstScope = await first.RunAsync(Jobs.Scope, "orders", token);
        var peerScope = await peer.RunAsync(Jobs.Scope, "ledger", token);
        Require(!firstScope.StaleScopeRejected && !peerScope.StaleScopeRejected,
            "A shared Worker must load and dispose its business scopes per logical session");

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var held = first.RunAsync(Jobs.WaitThenRead, new HeldRead("shared-cancel", "orders"), cancellation.Token);
        await Entered(fixture, "shared-cancel", token);
        try
        {
            await peer.RunAsync(Jobs.VerifyLedger, token).WaitAsync(TimeSpan.FromSeconds(8), token);
            Require(!held.IsCompleted, "A same-PID fast session must complete while another holds HTTP");
            await cancellation.CancelAsync();
            try
            {
                await held.WaitAsync(TimeSpan.FromSeconds(8), token);
                throw new InvalidOperationException("The cancelled shared session returned a result");
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested) { }
            catch (UniversalDataException error) when (error.Code is StatusCode.Cancelled or StatusCode.CommitUnknown) { }
        }
        finally { await Release(fixture, "shared-cancel", token); }

        try
        {
            await first.RunAsync(Jobs.Fail, token);
            throw new InvalidOperationException("The shared business exception was swallowed");
        }
        catch (DataBusinessException error) when (error.RemoteType == typeof(InvalidOperationException).FullName &&
            error.Message.Contains("settlement rejected", StringComparison.Ordinal)) { }
        await first.DisposeAsync();
        var survived = await peer.RunAsync(Jobs.Scope, "ledger", token);
        Require(survived.ProcessId == peer.ProcessId && survived.Value == "ledger" && survived.StaleScopeRejected,
            "Cancelling and closing one logical session must preserve the same-PID peer and its scope lifetime");

        await using var next = await data.OpenSessionAsync(token);
        await using var another = await data.OpenSessionAsync(token);
        var groups = new[] { peer, next, another }.GroupBy(s => s.ProcessId).OrderByDescending(group => group.Count()).ToArray();
        Require(groups.Length == 2 && groups[0].Count() == 2 && groups[1].Count() == 1,
            "Two session slots per physical Worker must fill before allocating a third PID");
        var colocated = groups[0].ToArray();
        var unrelated = groups[1].Single();
        var victim = colocated[0].RunAsync(Jobs.WaitThenRead, new HeldRead("shared-crash", "ledger"), token);
        await Entered(fixture, "shared-crash", token);
        try
        {
            await Lost(colocated[1].RunAsync(Jobs.Crash, token), token);
            await Lost(victim, token);
            var final = await unrelated.RunAsync(Jobs.Scope, "ledger", token);
            Require(final.ProcessId == unrelated.ProcessId && final.Value == "ledger",
                "The physical crash must remain confined to sessions sharing the terminated PID");
        }
        finally { await Release(fixture, "shared-crash", token); }
        Console.WriteLine(JsonSerializer.Serialize(new { SharedPid = overlap[0].ProcessId, SharedBranches = 4,
            CrashPid = colocated[0].ProcessId, SurvivorPid = unrelated.ProcessId }));
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

    private static async Task Lost(Task request, CancellationToken cancellation)
    {
        try { await request.WaitAsync(TimeSpan.FromSeconds(10), cancellation); }
        catch (UniversalDataException error) when (error.Code is StatusCode.WorkerLost or StatusCode.CommitUnknown) { return; }
        throw new InvalidOperationException("A physically terminated Worker returned a logical success");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
