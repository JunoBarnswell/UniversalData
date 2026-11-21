using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Channels;
using UniversalData;

namespace FluentAcceptance;

internal static class LatencyCase
{
    private const int PerSession = 500;
    private const double RatioLimit = 1.10;
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(12);

    internal static async Task RunAsync(string policy, string slowDatabase, string fastDatabase, string origin)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(165));
        using var fixture = new HttpClient { BaseAddress = new Uri(origin), Timeout = TimeSpan.FromSeconds(10) };
        bool isolation = Environment.GetEnvironmentVariable("UD_TEST_FLUENT_LOAD_REQUIRE_LIMITS") != "0";
        var failures = new List<string>();
        foreach (int sessionsPerProcess in new[] { 1, 4 })
        {
            var mode = sessionsPerProcess == 1 ? "dedicated" : "shared";
            await using var data = DataClient.CreateBuilder(policy, "fluent")
                .AddSqlite("slow", "Data Source=" + slowDatabase)
                .AddSqlite("fast", "Data Source=" + fastDatabase)
                .AddHttp("pricing", new Uri(origin))
                .AddModule(typeof(LatencyJobs).Assembly)
                .ConfigureWorkers(options => options.MaxConcurrentSessions(4).MaxSessionsPerWorker(sessionsPerProcess)
                    .MaxBranchesPerSession(2).MaxConnectionsPerSource(2).ExecutionTimeout(TimeSpan.FromSeconds(45))
                    .RequireResourceIsolation(isolation))
                .Build();

            await MeasureShortSessions(data, mode, deadline.Token);
            await VerifyInterference(data, fixture, mode, sessionsPerProcess, isolation, failures, deadline.Token);
        }
        if (failures.Count != 0)
            throw new InvalidOperationException("Fast-request P95 exceeded 1.10 × baseline: " + string.Join(", ", failures));
    }

    private static async Task MeasureShortSessions(DataClient data, string mode, CancellationToken cancellation)
    {
        long started = Stopwatch.GetTimestamp();
        var cold = await data.RunAsync(LatencyJobs.Fast, cancellation);
        RequireLedger(cold);
        var coldMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        started = Stopwatch.GetTimestamp();
        var samples = new double[24];
        var workers = new HashSet<int>();
        for (int index = 0; index < samples.Length; index += 2)
        {
            var submissions = await Task.WhenAll(Run(), Run());
            for (var offset = 0; offset < submissions.Length; offset++)
            {
                var (sample, worker) = submissions[offset];
                samples[index + offset] = sample;
                workers.Add(worker);
            }
        }
        Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = "short-session", Samples = samples.Length,
            ColdMs = coldMs, P95Ms = Percentile(samples, 0.95), P99Ms = Percentile(samples, 0.99),
            RequestsPerSecond = samples.Length / Stopwatch.GetElapsedTime(started).TotalSeconds, WorkerPids = workers.Count }));

        async Task<(double Duration, int Worker)> Run()
        {
            long stamp = Stopwatch.GetTimestamp();
            var result = await data.RunAsync(LatencyJobs.Fast, cancellation);
            RequireLedger(result);
            return (Stopwatch.GetElapsedTime(stamp).TotalMilliseconds, result.ProcessId);
        }
    }

    private static async Task VerifyInterference(DataClient data, HttpClient fixture, string mode,
        int sessionsPerProcess, bool isolation, List<string> failures, CancellationToken token)
    {
        long acquired = Stopwatch.GetTimestamp();
        var opened = await Task.WhenAll(data.OpenSessionAsync(token), data.OpenSessionAsync(token));
        double acquireMs = Stopwatch.GetElapsedTime(acquired).TotalMilliseconds;
        await using var first = opened[0];
        await using var second = opened[1];
        if ((first.ProcessId == second.ProcessId) != (sessionsPerProcess > 1))
            throw new InvalidOperationException("Fast sessions were placed in the wrong physical Worker mode");
        var fastBudgets = opened.Select(session => Budget(session.ProcessId, sessionsPerProcess, isolation)).ToArray();
        if (sessionsPerProcess == 1 && fastBudgets[0] is { } firstBudget && firstBudget.CpuAffinity == fastBudgets[1]?.CpuAffinity)
            throw new InvalidOperationException("The dedicated fast Workers share a logical CPU");
        await Measure(first, second, 100, token);

        string[] stages = ["baseline", "http", "sql", "cpu"];
        var measurements = stages.ToDictionary(stage => stage, _ => new List<LatencyMeasurement>(2));
        // Each condition occupies a matching early and late position. Include every planned arrival in its P95.
        for (int window = 1; window <= 2; window++)
        {
            foreach (string stage in window == 1 ? stages : stages.Reverse())
            {
                var measured = stage == "baseline" ? await Measure(first, second, PerSession, token) :
                    await MeasureSlow(data, fixture, mode, stage, window, first, second, fastBudgets, sessionsPerProcess, isolation, token);
                measurements[stage].Add(measured);
                Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = stage, Window = window,
                    Samples = measured.Samples.Length, Summary = Summarize([measured]) }));
            }
        }

        var baseline = Summarize(measurements["baseline"]);
        Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = "baseline", Samples = 4 * PerSession,
            SessionAcquireMs = acquireMs, SessionPids = opened.Select(session => session.ProcessId), Summary = baseline,
            RequiredResourceLimits = isolation, FastBudgets = fastBudgets }));
        foreach (string stage in stages.Skip(1))
        {
            var measure = Summarize(measurements[stage]);
            double requestRatio = measure.P95Ms / baseline.P95Ms;
            double plannedRatio = measure.P95ScheduleToResultMs / baseline.P95ScheduleToResultMs;
            Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = stage, Samples = 4 * PerSession,
                Summary = measure, RequestRatio = requestRatio, PlannedRatio = plannedRatio, MaximumRatio = RatioLimit }));
            if (requestRatio > RatioLimit || plannedRatio > RatioLimit)
                failures.Add($"{mode}/{stage} SDK {requestRatio:F3}, planned {plannedRatio:F3}");
        }
    }

    private static async Task<LatencyMeasurement> MeasureSlow(DataClient data, HttpClient fixture, string mode, string stage,
        int window, DataSession first, DataSession second, LatencyWorkerBudget?[] fastBudgets, int sessionsPerProcess,
        bool isolation, CancellationToken token)
    {
        using var stageCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        long acquired = Stopwatch.GetTimestamp();
        await using var delayed = await data.OpenSessionAsync(token);
        double acquireMs = Stopwatch.GetElapsedTime(acquired).TotalMilliseconds;
        if ((delayed.ProcessId == first.ProcessId) != (sessionsPerProcess > 1))
            throw new InvalidOperationException("The slow session must run in the requested physical isolation mode");
        using var process = Process.GetProcessById(delayed.ProcessId);
        var before = process.TotalProcessorTime;
        var slowBudget = Budget(delayed.ProcessId, sessionsPerProcess, isolation);
        if (sessionsPerProcess == 1 && slowBudget is not null && fastBudgets.Any(fast => fast?.CpuAffinity == slowBudget.CpuAffinity))
            throw new InvalidOperationException("The dedicated slow Worker shares a logical CPU with a fast Worker");
        string signal = $"{mode}-{stage}-{window}";
        if (stage == "sql") await Control(fixture, "lock", token);
        var pending = delayed.RunAsync(LatencyJobs.Slow, new LatencyWork(stage, signal), stageCancellation.Token);
        try
        {
            while (!await fixture.GetFromJsonAsync<bool>("entered/" + signal, token))
                await Task.Delay(20, token);
            var measure = await Measure(first, second, PerSession, token);
            if (pending.IsCompleted) throw new InvalidOperationException($"The {stage} fixture stopped before all samples completed");
            process.Refresh();
            Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = stage, Window = window,
                SlowPid = delayed.ProcessId, SessionAcquireMs = acquireMs,
                CpuMs = (process.TotalProcessorTime - before).TotalMilliseconds, ResidentBytes = process.WorkingSet64,
                PeakResidentBytes = process.PeakWorkingSet64, SlowBudget = slowBudget }));
            return measure;
        }
        finally
        {
            if (stage == "http") await Control(fixture, "release/" + signal, token);
            if (stage == "sql") await Control(fixture, "unlock", token);
            if (stage == "cpu") await stageCancellation.CancelAsync();
            try { await pending.WaitAsync(TimeSpan.FromSeconds(8), token); }
            catch (OperationCanceledException) when (stage == "cpu" && !token.IsCancellationRequested) { }
            catch (UniversalDataException error) when (stage == "cpu" && error.Code == StatusCode.Cancelled) { }
        }
    }

    private static async Task Control(HttpClient fixture, string path, CancellationToken cancellation)
    {
        using var response = await fixture.GetAsync(path, cancellation);
        response.EnsureSuccessStatusCode();
    }

    private static void RequireLedger(FastTiming result)
    {
        if (result.Value != "ledger") throw new InvalidOperationException("The fast SQL request returned an unexpected row");
    }

    private static LatencyWorkerBudget? Budget(int processId, int sessionsPerProcess, bool required)
    {
        if (!OperatingSystem.IsLinux() || !required) return null;
        var membership = File.ReadLines($"/proc/{processId}/cgroup").Single(line => line.StartsWith("0::", StringComparison.Ordinal))[3..];
        var directory = Path.Combine("/sys/fs/cgroup", membership.TrimStart('/'));
        string cpuQuota = File.ReadAllText(Path.Combine(directory, "cpu.max")).Trim();
        string memoryLimit = File.ReadAllText(Path.Combine(directory, "memory.max")).Trim();
        if (cpuQuota != $"{100_000 * sessionsPerProcess} 100000" ||
            memoryLimit != ((long)(sessionsPerProcess + 1) * 268_435_456).ToString())
            throw new InvalidOperationException($"Worker {processId} cgroup does not have the configured aggregate CPU and memory budget");
        const string field = "Cpus_allowed_list:";
        string cpuAffinity = File.ReadLines($"/proc/{processId}/status")
            .Single(line => line.StartsWith(field, StringComparison.Ordinal))[field.Length..].Trim();
        int processors = cpuAffinity.Split(',').Sum(range =>
        {
            string[] ends = range.Split('-');
            return int.Parse(ends[^1], CultureInfo.InvariantCulture) - int.Parse(ends[0], CultureInfo.InvariantCulture) + 1;
        });
        if (processors != sessionsPerProcess)
            throw new InvalidOperationException($"Worker {processId} CPU affinity differs from its reserved slots");
        return new(membership, cpuQuota, memoryLimit, cpuAffinity);
    }

    private static async Task<LatencyMeasurement> Measure(DataSession first, DataSession second, int samples, CancellationToken token)
    {
        var arrival = new[] { Channel.CreateUnbounded<long>(), Channel.CreateUnbounded<long>() };
        var data = new LatencySample[2 * samples];
        long started = Stopwatch.GetTimestamp() + Stopwatch.Frequency / 10;
        long step = (long)(Interval.TotalSeconds * Stopwatch.Frequency / 2);
        var producer = new Thread(() =>
        {
            Exception? failure = null;
            try
            {
                for (int index = 0; index < samples * 2; index++)
                {
                    long scheduled = started + index * step;
                    LatencyArrivalClock.WaitUntil(scheduled, token);
                    arrival[index % 2].Writer.TryWrite(scheduled);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception error) { failure = error; }
            finally
            {
                foreach (var lane in arrival) lane.Writer.TryComplete(failure);
            }
        }) { IsBackground = true };
        producer.Start();
        await Task.WhenAll(Consume(first, arrival[0], 0), Consume(second, arrival[1], samples));
        return new(data, Stopwatch.GetElapsedTime(started).TotalSeconds);

        async Task Consume(DataSession session, Channel<long> lane, int offset)
        {
            int index = offset;
            await foreach (long scheduled in lane.Reader.ReadAllAsync(token))
            {
                long submitted = Stopwatch.GetTimestamp();
                var result = await session.RunAsync(LatencyJobs.Fast, token);
                long completed = Stopwatch.GetTimestamp();
                RequireLedger(result);
                data[index++] = new(Stopwatch.GetElapsedTime(submitted, completed).TotalMilliseconds,
                    Stopwatch.GetElapsedTime(scheduled, completed).TotalMilliseconds,
                    Stopwatch.GetElapsedTime(scheduled, submitted).TotalMilliseconds, result.ConnectionLeaseMs, result.SqlMs);
            }
            if (index != offset + samples) throw new InvalidOperationException("The planned arrival schedule did not complete");
        }
    }

    private static LatencySummary Summarize(IReadOnlyList<LatencyMeasurement> measurements)
    {
        var data = measurements.SelectMany(window => window.Samples).ToArray();
        return new(Percentile(data.Select(row => row.RequestMs), 0.95), Percentile(data.Select(row => row.RequestMs), 0.99),
            data.Max(row => row.RequestMs),
            Percentile(data.Select(row => row.PlannedMs), 0.95), Percentile(data.Select(row => row.PlannedMs), 0.99),
            Percentile(data.Select(row => row.DispatchMs), 0.95), data.Max(row => row.DispatchMs),
            Percentile(data.Select(row => row.ConnectionLeaseMs), 0.95), Percentile(data.Select(row => row.SqlMs), 0.95),
            data.Length / measurements.Sum(window => window.ElapsedSeconds));
    }

    private static double Percentile(IEnumerable<double> samples, double percentile)
    {
        var sorted = samples.Order().ToArray();
        return sorted[(int)Math.Ceiling(sorted.Length * percentile) - 1];
    }
}
