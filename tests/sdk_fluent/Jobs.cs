using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

public sealed record HeldRead(string Gate, string Source);
public sealed record ScopeResult(int ProcessId, string Value, bool StaleScopeRejected);
public sealed record OverlapResult(int ProcessId, string Orders, string Ledger);

[SugarTable("sdk_orders")]
public sealed class FixtureRow
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string Tag { get; set; } = "";
}

public static class Jobs
{
    private static DataContext? earlierScope;

    public static async Task<OverlapResult> Overlap(DataContext context, string request, CancellationToken cancellation)
    {
        var (left, right) = await context.ParallelAsync(
            (branch, token) => Join(branch, request, "orders", token),
            (branch, token) => Join(branch, request, "ledger", token), cancellation);
        if (!ReferenceEquals(DataContext.Current, context))
            throw new InvalidOperationException("Parallel execution replaced the ambient parent context");
        return new(Environment.ProcessId, left, right);
    }

    private static async Task<string> Join(DataContext branch, string request, string source,
        CancellationToken cancellation)
    {
        if (!ReferenceEquals(DataContext.Current, branch))
            throw new InvalidOperationException("A parallel branch did not install its own ambient context");
        var value = await Read(branch, source, cancellation);
        if (!await branch.Http("pricing").GetAsync<bool>($"meet/{request}/{source}/{Environment.ProcessId}", cancellation))
            throw new InvalidOperationException("Four concurrent branches did not reach the same rendezvous");
        return value;
    }

    public static async Task<ScopeResult> Scope(DataContext context, string source, CancellationToken cancellation)
    {
        if (!ReferenceEquals(DataContext.Current, context))
            throw new InvalidOperationException("A request did not install its ambient context");
        var previous = Interlocked.Exchange(ref earlierScope, context);
        var denied = false;
        if (previous is not null)
        {
            try { _ = await Read(previous, source, cancellation); }
            catch (ObjectDisposedException) { denied = true; }
            if (!denied) throw new InvalidOperationException("A finished request context was still usable");
        }
        return new(Environment.ProcessId, await Read(context, source, cancellation), denied);
    }

    public static async Task<string> WaitThenRead(DataContext context, HeldRead request,
        CancellationToken cancellation)
    {
        if (!await context.Http("pricing").GetAsync<bool>("hold/" + request.Gate, cancellation))
            throw new InvalidOperationException("The HTTP fixture could not hold a Worker branch");
        return await Read(context, request.Source, cancellation);
    }

    public static async Task VerifyLedger(DataContext context, CancellationToken cancellation)
    {
        if (await Read(context, "ledger", cancellation) != "ledger")
            throw new InvalidOperationException("The surviving Worker returned an unexpected row");
    }

    public static Task Fail(DataContext context, CancellationToken cancellation) =>
        Task.FromException(new InvalidOperationException("settlement rejected"));

    public static Task Crash(DataContext context, CancellationToken cancellation)
    {
        Environment.Exit(47);
        return Task.CompletedTask;
    }

    public static async Task IgnoreCancellation(DataContext context, CancellationToken cancellation)
    {
        _ = await context.Http("pricing").GetAsync<bool>("signal/timeout", cancellation);
        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    public static Task UnsupportedAtomic(DataContext context, string marker, CancellationToken cancellation) =>
        context.Atomic("orders", "ledger").RunAsync((_, _) =>
        {
            File.WriteAllText(marker, "unexpected business invocation");
            return Task.CompletedTask;
        }, cancellation);

    private static Task<string> Read(DataContext context, string source, CancellationToken cancellation) =>
        context.Database(source).UseAsync(async (database, _) =>
        {
            var row = await database.Queryable<FixtureRow>().Where(value => value.Id == 1).FirstAsync();
            return row?.Tag ?? throw new InvalidOperationException("The expected database row does not exist");
        }, cancellation);
}
