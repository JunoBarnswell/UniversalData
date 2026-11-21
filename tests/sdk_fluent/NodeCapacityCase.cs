using UniversalData;

namespace FluentAcceptance;

internal static class NodeCapacityCase
{
    internal static async Task RunAsync(string policy, string orders)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await using var firstClient = Client();
        await using var secondClient = Client();
        await using var first = await firstClient.OpenSessionAsync(deadline.Token);
        await using var second = await secondClient.OpenSessionAsync(deadline.Token);
        if (first.ProcessId == second.ProcessId)
            throw new InvalidOperationException("Separate SDK hosts unexpectedly shared physical session identity");

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

        DataClient Client() => DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite("orders", "Data Source=" + orders)
            .ConfigureWorkers(worker => worker.MaxConcurrentSessions(2)
                .ExecutionTimeout(TimeSpan.FromSeconds(60)).CancellationGrace(TimeSpan.FromSeconds(3))).Build();
    }
}
