using System.Diagnostics;
using UniversalData;

namespace FluentAcceptance;

public static class LatencyJobs
{
    public static Task<FastTiming> Fast(DataContext context, CancellationToken cancellation)
    {
        long start = Stopwatch.GetTimestamp();
        return context.Database("fast").UseAsync(async (database, _) =>
        {
            long leased = Stopwatch.GetTimestamp();
            var row = await database.Queryable<FixtureRow>().Where(value => value.Id == 1).FirstAsync();
            return new FastTiming(row.Tag, Stopwatch.GetElapsedTime(start, leased).TotalMilliseconds,
                Stopwatch.GetElapsedTime(leased).TotalMilliseconds, Environment.ProcessId);
        }, cancellation);
    }

    public static async Task Slow(DataContext context, LatencyWork work, CancellationToken cancellation)
    {
        if (work.Stage == "http")
        {
            _ = await context.Http("pricing").GetAsync<bool>("hold/" + work.Signal, cancellation);
            return;
        }
        _ = await context.Http("pricing").GetAsync<bool>("signal/" + work.Signal, cancellation);
        if (work.Stage == "sql")
        {
            _ = await context.Database("slow").UseAsync(async (database, _) => await database.Ado.ExecuteCommandAsync(
                "UPDATE sdk_orders SET Tag = Tag WHERE Id = 1"), cancellation);
            return;
        }
        while (!cancellation.IsCancellationRequested) Thread.SpinWait(4096);
        cancellation.ThrowIfCancellationRequested();
    }
}
