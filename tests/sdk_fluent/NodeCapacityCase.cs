using System.Net.Http.Json;
using UniversalData;

namespace FluentAcceptance;

internal static class NodeCapacityCase
{
    internal static async Task RunAsync(string policy, string orders, string origin)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var firstClient = Client("orders");
        await using var secondClient = Client("alias");
        int warmedPid;
        await using (var warm = await firstClient.OpenSessionAsync(deadline.Token))
            warmedPid = warm.ProcessId;
        await using var first = await firstClient.OpenSessionAsync(deadline.Token);
        if (first.ProcessId != warmedPid)
            throw new InvalidOperationException("A healthy trusted Worker did not accept another exclusive session");
        await using var second = await secondClient.OpenSessionAsync(deadline.Token);
        if (first.ProcessId == second.ProcessId)
            throw new InvalidOperationException("Separate SDK hosts unexpectedly shared physical session identity");

        await CheckPhysicalSourceLimitAsync(first, second, origin, deadline.Token);

        Task<DataSession> waiting = firstClient.OpenSessionAsync(deadline.Token);
        if (await Task.WhenAny(waiting, Task.Delay(TimeSpan.FromMilliseconds(750), deadline.Token)) == waiting)
        {
            await using var unexpected = await waiting;
            throw new InvalidOperationException("Two SDK hosts admitted more Workers than the common node limit");
        }

        await second.DisposeAsync();
        await using var third = await waiting;
        if (third.ProcessId == second.ProcessId || third.ProcessId == first.ProcessId)
            throw new InvalidOperationException("A reclaimed reservation must belong to the new dedicated process");
        Console.WriteLine($"One node limit shared by two SDK hosts; Workers {first.ProcessId} and {second.ProcessId} held the slots, {third.ProcessId} entered after release.");

        DataClient Client(string name) => DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite(name, "Data Source=" + orders)
            .AddHttp("pricing", new Uri(origin)).AddModule(typeof(Jobs).Assembly)
            .ConfigureWorkers(worker => worker.MaxConcurrentSessions(2)
                .ExecutionTimeout(TimeSpan.FromSeconds(60)).CancellationGrace(TimeSpan.FromSeconds(3))).Build();
    }

    private static async Task CheckPhysicalSourceLimitAsync(DataSession first, DataSession second,
        string origin, CancellationToken cancellation)
    {
        const string gate = "node-source-limit";
        using var http = new HttpClient { BaseAddress = new Uri(origin) };
        var held = first.RunAsync(Jobs.HoldLeaseThenRead, new HeldRead(gate, "orders"), cancellation);
        while (!await http.GetFromJsonAsync<bool>("entered/" + gate, cancellation))
            await Task.Delay(20, cancellation);
        var peer = second.RunAsync(Jobs.Scope, "alias", cancellation);
        try
        {
            if (await Task.WhenAny(peer, Task.Delay(300, cancellation)) == peer)
                throw new InvalidOperationException("Two Worker processes exceeded the same physical source limit");
        }
        finally
        {
            using var response = await http.GetAsync("release/" + gate, cancellation);
            response.EnsureSuccessStatusCode();
        }
        if (await held != "orders-original" || (await peer).Value != "orders-original")
            throw new InvalidOperationException("Source admission did not preserve the actual database and results");
    }
}
