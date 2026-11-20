using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using UniversalData;

namespace FluentAcceptance;

internal static class LatencyCase
{
    private const int PerSession = 500;
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(12);

    internal static async Task RunAsync(string policy, string slowDatabase, string fastDatabase, string origin)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(150));
        var token = deadline.Token;
        var isolation = Environment.GetEnvironmentVariable("UD_TEST_FLUENT_LOAD_REQUIRE_LIMITS") != "0";
        await using var data = DataClient.CreateBuilder(policy, "fluent")
            .AddSqlite("slow", "Data Source=" + slowDatabase + ";Default Timeout=45")
            .AddSqlite("fast", "Data Source=" + fastDatabase)
            .AddHttp("pricing", new Uri(origin))
            .AddModule(typeof(LatencyJobs).Assembly)
            .ConfigureWorkers(options => options.MaxConcurrentSessions(4).MaxBranchesPerSession(2)
                .MaxConnectionsPerSource(2).ExecutionTimeout(TimeSpan.FromSeconds(45))
                .RequireResourceIsolation(isolation))
            .Build();
        using var fixture = new HttpClient { BaseAddress = new Uri(origin), Timeout = TimeSpan.FromSeconds(10) };
        long lease = Stopwatch.GetTimestamp();
        var opening = await Task.WhenAll(data.OpenSessionAsync(token), data.OpenSessionAsync(token));
        var acquire = Stopwatch.GetElapsedTime(lease).TotalMilliseconds;
        await using var first = opening[0];
        await using var second = opening[1];
        var fastBudgets = new[] { Budget(first.ProcessId, isolation), Budget(second.ProcessId, isolation) };
        await Measure(first, second, 125, token);
        var baseline = await Measure(first, second, PerSession, token);
        Console.WriteLine(JsonSerializer.Serialize(new { Stage = "baseline", Samples = 2 * PerSession,
            SessionAcquireMs = acquire, SessionPids = new[] { first.ProcessId, second.ProcessId }, Summary = baseline,
            RequiredResourceLimits = isolation, FastBudgets = fastBudgets }));

        var excess = new List<string>();
        foreach (string stage in new[] { "http", "sql", "cpu" })
        {
            using var stageCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            lease = Stopwatch.GetTimestamp();
            await using var delayed = await data.OpenSessionAsync(token);
            acquire = Stopwatch.GetElapsedTime(lease).TotalMilliseconds;
            using var process = Process.GetProcessById(delayed.ProcessId);
            var slowBudget = Budget(delayed.ProcessId, isolation);
            var before = process.TotalProcessorTime;
            if (stage == "sql") await Control(fixture, "lock", token);
            Task work = delayed.RunAsync(LatencyJobs.Slow, stage, stageCancellation.Token);
            try
            {
                while (!await fixture.GetFromJsonAsync<bool>("entered/" + stage, token))
                    await Task.Delay(20, token);
                var measure = await Measure(first, second, PerSession, token);
                if (work.IsCompleted) throw new InvalidOperationException($"The {stage} fixture stopped before the fast samples completed");
                process.Refresh();
                double ratio = measure.P95Ms / baseline.P95Ms;
                Console.WriteLine(JsonSerializer.Serialize(new { Stage = stage, Samples = 2 * PerSession,
                    SlowPid = delayed.ProcessId, SessionAcquireMs = acquire, CpuMs = (process.TotalProcessorTime - before).TotalMilliseconds,
                    ResidentBytes = process.WorkingSet64, PeakResidentBytes = process.PeakWorkingSet64,
                    Summary = measure, Ratio = ratio, MaximumRatio = 1.10, SlowBudget = slowBudget }));
                if (ratio > 1.10) excess.Add($"{stage}={ratio:F3}");
            }
            finally
            {
                if (stage == "http") await Control(fixture, "release/http", token);
                if (stage == "sql") await Control(fixture, "unlock", token);
                if (stage == "cpu") await stageCancellation.CancelAsync();
                try { await work.WaitAsync(TimeSpan.FromSeconds(8), token); }
                catch (OperationCanceledException) when (stage == "cpu" && !token.IsCancellationRequested) { }
                catch (UniversalDataException error) when (stage == "cpu" && error.Code == StatusCode.Cancelled) { }
            }
        }
        if (excess.Count != 0) throw new InvalidOperationException("Fast-request P95 exceeded 1.10 × baseline: " + string.Join(", ", excess));
    }

    private static async Task Control(HttpClient fixture, string path, CancellationToken token)
    {
        using var response = await fixture.GetAsync(path, token);
        response.EnsureSuccessStatusCode();
    }

    private sealed record ResourceBudget(string ControlGroup, long CpuQuotaUs, long CpuPeriodUs, long MaxMemoryBytes);

    private static ResourceBudget? Budget(int pid, bool required)
    {
        if (!OperatingSystem.IsLinux() || !required) return null;
        var membership = File.ReadLines($"/proc/{pid}/cgroup").Single(line => line.StartsWith("0::", StringComparison.Ordinal))[3..];
        var directory = Path.Combine("/sys/fs/cgroup", membership.TrimStart('/'));
        var cpu = File.ReadAllText(Path.Combine(directory, "cpu.max")).Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (cpu.Length != 2 || !long.TryParse(cpu[0], out long quota) || !long.TryParse(cpu[1], out long period) ||
            quota != 100_000 || period != 100_000 ||
            !long.TryParse(File.ReadAllText(Path.Combine(directory, "memory.max")), out long memory) || memory != 536_870_912)
            throw new InvalidOperationException($"Worker {pid} cgroup did not reserve the configured single CPU and 512 MiB process budget");
        return new(membership, quota, period, memory);
    }

    private sealed record Summary(double P95Ms, double P95ScheduleToResultMs, double P95DispatchDelayMs,
        double P95ConnectionLeaseMs, double P95SqlMs, double ObservedRequestsPerSecond, int Timeouts);

    private sealed record Sample(double RequestMs, double ScheduleToResultMs, double DispatchMs, FastTiming Timing);

    private static async Task<Summary> Measure(DataSession first, DataSession second, int samples, CancellationToken token)
    {
        long start = Stopwatch.GetTimestamp();
        async Task<Sample[]> RunLane(DataSession session, double offset)
        {
            var result = new Sample[samples];
            for (int i = 0; i < samples; i++)
            {
                var scheduled = Interval * (i + offset);
                var wait = scheduled - Stopwatch.GetElapsedTime(start);
                if (wait > TimeSpan.Zero) await Task.Delay(wait, token);
                long submitted = Stopwatch.GetTimestamp();
                var row = await session.RunAsync(LatencyJobs.Fast, token);
                long completed = Stopwatch.GetTimestamp();
                if (row.Value != "ledger") throw new InvalidOperationException("The fast SQL request returned an unexpected row");
                result[i] = new(Stopwatch.GetElapsedTime(submitted, completed).TotalMilliseconds,
                    (Stopwatch.GetElapsedTime(start, completed) - scheduled).TotalMilliseconds,
                    Math.Max((Stopwatch.GetElapsedTime(start, submitted) - scheduled).TotalMilliseconds, 0), row);
            }
            return result;
        }
        var result = (await Task.WhenAll(RunLane(first, 0), RunLane(second, 0.5))).SelectMany(rows => rows).ToArray();
        var elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
        return new(P95(result.Select(row => row.RequestMs)), P95(result.Select(row => row.ScheduleToResultMs)),
            P95(result.Select(row => row.DispatchMs)),
            P95(result.Select(row => row.Timing.ConnectionLeaseMs)), P95(result.Select(row => row.Timing.SqlMs)),
            result.Length / elapsed, 0);
    }

    private static double P95(IEnumerable<double> input)
    {
        var sorted = input.Order().ToArray();
        return sorted[(int)Math.Ceiling(sorted.Length * 0.95) - 1];
    }
}

public sealed record FastTiming(string Value, double ConnectionLeaseMs, double SqlMs);

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
                Stopwatch.GetElapsedTime(leased).TotalMilliseconds);
        }, cancellation);
    }

    public static async Task Slow(DataContext context, string stage, CancellationToken cancellation)
    {
        if (stage == "http")
        {
            _ = await context.Http("pricing").GetAsync<bool>("hold/http", cancellation);
            return;
        }
        _ = await context.Http("pricing").GetAsync<bool>("signal/" + stage, cancellation);
        if (stage == "sql")
        {
            _ = await context.Database("slow").UseAsync(async (database, _) => await database.Ado.ExecuteCommandAsync(
                "UPDATE sdk_orders SET Tag = Tag WHERE Id = 1"), cancellation);
            return;
        }
        while (!cancellation.IsCancellationRequested) Thread.SpinWait(4096);
        cancellation.ThrowIfCancellationRequested();
    }
}
