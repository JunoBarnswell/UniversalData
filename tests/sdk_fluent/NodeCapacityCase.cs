using System.Net.Http.Json;
using UniversalData;

namespace FluentAcceptance;

internal static class NodeCapacityCase
{
    internal static async Task RunAsync(string policy, string orders, string origin)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var token = deadline.Token;
        await using var firstClient = Client(policy, "orders", orders, origin);
        await using var secondClient = Client(policy, "alias", orders, origin);
        var first = await Start(firstClient, Jobs.HoldNodeFirst, token);
        var second = await Start(secondClient, Jobs.HoldNodeSecond, token);
        using var fixture = new HttpClient { BaseAddress = new Uri(origin), Timeout = TimeSpan.FromSeconds(5) };
        Task<EtlRun>? queued = null;
        EtlRun? third = null;
        int thirdPid = 0;
        try
        {
            await Entered(fixture, "node-first", token);
            await Entered(fixture, "node-second", token);

            var pending = Start(secondClient, Jobs.AliasWork, token);
            queued = pending;
            if (await Task.WhenAny(pending, Task.Delay(TimeSpan.FromMilliseconds(750), token)) == pending)
                throw new InvalidOperationException("Two SDK hosts admitted more Workers than the common node limit");

            await Release(fixture, "node-second", token);
            await second.WaitAsync(token);
            await second.DisposeAsync();
            var openedThird = await pending;
            third = openedThird;
            thirdPid = openedThird.WorkerProcessId;
            Require(openedThird.WorkerProcessId != first.WorkerProcessId,
                "A queued Native job reused the Worker still occupying the other node slot");
            await openedThird.WaitAsync(token);
            await openedThird.DisposeAsync();

            await Release(fixture, "node-first", token);
            await first.WaitAsync(token);
            Require(await Jobs.FirstTag(firstClient, Jobs.Orders, token) == "orders-original" &&
                await Jobs.FirstTag(secondClient, Jobs.Alias, token) == "orders-original",
                "Cross-host Native jobs must preserve real SQLite reads after node admission");
            Console.WriteLine($"One node limit shared by two SDK hosts; Workers {first.WorkerProcessId} and " +
                $"{second.WorkerProcessId} held the slots, {thirdPid} entered after release.");
        }
        finally
        {
            await ReleaseIfEntered(fixture, "node-first", token);
            await ReleaseIfEntered(fixture, "node-second", token);
            if (third is not null) await third.DisposeAsync();
            if (queued is { IsCompletedSuccessfully: true } && third is null)
            {
                try { await (await queued).DisposeAsync(); }
                catch (UniversalDataException) { }
            }
            await second.DisposeAsync();
            await first.DisposeAsync();
        }
    }

    private static Task<EtlRun> Start(DataClient client, DataJob<object> job, CancellationToken cancellation) =>
        client.StartAsync(job, new object(), cancellation);

    private static DataClient Client(string policy, string name, string orders, string origin)
    {
        var builder = DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite(name, "Data Source=" + orders);
        Jobs.AddHttpSources(builder, origin);
        return builder
            .ConfigureWorkers(worker => worker.MaxConcurrentSessions(2).MaxConnectionsPerSource(1)
                .QueueTimeout(TimeSpan.FromSeconds(10)).ExecutionTimeout(TimeSpan.FromSeconds(60))
                .CancellationGrace(TimeSpan.FromSeconds(3)))
            .Build();
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

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
