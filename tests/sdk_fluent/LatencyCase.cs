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
        await ReadLedger(data, cancellation);
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
            var result = await ReadLedger(data, cancellation);
            return (Stopwatch.GetElapsedTime(stamp).TotalMilliseconds, result.ProcessId);
        }
    }

    private static async Task VerifyInterference(DataClient data, HttpClient fixture, string mode,
        int sessionsPerProcess, bool isolation, List<string> failures, CancellationToken token)
    {
        await Measure(data, 100, token);

        string[] stages = ["baseline", "http", "sql", "cpu"];
        var measurements = stages.ToDictionary(stage => stage, _ => new List<LatencyMeasurement>(2));
        // Each condition occupies a matching early and late position. Include every planned arrival in its P95.
        for (int window = 1; window <= 2; window++)
        {
            foreach (string stage in window == 1 ? stages : stages.Reverse())
            {
                var measured = stage == "baseline" ? await Measure(data, PerSession, token) :
                    await MeasureSlow(data, fixture, mode, stage, window, sessionsPerProcess, isolation, token);
                measurements[stage].Add(measured);
                Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = stage, Window = window,
                    Samples = measured.Samples.Length, Summary = Summarize([measured]) }));
            }
        }

        var baseline = Summarize(measurements["baseline"]);
        var workers = measurements["baseline"].SelectMany(window => window.Samples)
            .Select(sample => sample.ProcessId).Distinct().ToArray();
        Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = "baseline", Samples = 4 * PerSession,
            SessionPids = workers, Summary = baseline, RequiredResourceLimits = isolation }));
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
        int window, int sessionsPerProcess, bool isolation, CancellationToken token)
    {
        string signal = $"{mode}-{stage}-{window}";
        if (stage == "sql") await Control(fixture, "lock", token);
        long acquired = Stopwatch.GetTimestamp();
        EtlRun? delayed = null;
        Task<ExecutionMetrics>? pending = null;
        try
        {
            delayed = await data.StartAsync(LatencyJobs.Slow(stage, signal), token);
            double startupMs = Stopwatch.GetElapsedTime(acquired).TotalMilliseconds;
            using var process = Process.GetProcessById(delayed.WorkerProcessId);
            var before = process.TotalProcessorTime;
            var slowBudget = Budget(delayed.WorkerProcessId, sessionsPerProcess, isolation);
            pending = delayed.WaitAsync(token);
            while (!await fixture.GetFromJsonAsync<bool>("entered/" + signal, token))
            {
                if (pending.IsCompleted) throw new InvalidOperationException($"The {stage} fixture stopped before it became ready");
                await Task.Delay(20, token);
            }
            var measure = await Measure(data, PerSession, token);
            if (pending.IsCompleted) throw new InvalidOperationException($"The {stage} fixture stopped before all samples completed");
            var fastWorkers = measure.Samples.Select(sample => sample.ProcessId).Distinct().ToArray();
            foreach (int worker in fastWorkers)
            {
                if ((worker == delayed.WorkerProcessId) != (sessionsPerProcess > 1))
                    throw new InvalidOperationException("Fast and slow jobs were placed in the wrong physical Worker mode");
                if (sessionsPerProcess == 1 && slowBudget is not null &&
                    Budget(worker, sessionsPerProcess, isolation)?.CpuAffinity == slowBudget.CpuAffinity)
                    throw new InvalidOperationException("The slow Worker shares the CPU reserved by a fast Worker");
            }
            process.Refresh();
            Console.WriteLine(JsonSerializer.Serialize(new { Mode = mode, Stage = stage, Window = window,
                SlowPid = delayed.WorkerProcessId, StartupMs = startupMs, FastPids = fastWorkers,
                CpuMs = (process.TotalProcessorTime - before).TotalMilliseconds, ResidentBytes = process.WorkingSet64,
                PeakResidentBytes = process.PeakWorkingSet64, SlowBudget = slowBudget }));
            return measure;
        }
        finally
        {
            await using var owned = delayed;
            if (stage == "http") await Control(fixture, "release/" + signal, token);
            if (stage == "sql") await Control(fixture, "unlock", token);
            if (stage == "cpu" && delayed is not null) await delayed.CancelAsync(token);
            try { if (pending is not null) await pending.WaitAsync(TimeSpan.FromSeconds(8), token); }
            catch (UniversalDataException error) when (stage == "cpu" && error.Code == StatusCode.Cancelled) { }
        }
    }

    private static async Task Control(HttpClient fixture, string path, CancellationToken cancellation)
    {
        using var response = await fixture.GetAsync(path, cancellation);
        response.EnsureSuccessStatusCode();
    }

    private static async Task<FastTiming> ReadLedger(DataClient data, CancellationToken cancellation)
    {
        long started = Stopwatch.GetTimestamp();
        await using var run = await data.StartAsync(LatencyJobs.Fast, cancellation);
        long opened = Stopwatch.GetTimestamp();
        int count = 0;
        await foreach (var row in run.ReadRowsAsync<FixtureRow>(cancellation))
        {
            if (++count != 1 || row.Id != 1 || row.Tag != "ledger")
                throw new InvalidOperationException("The fast SQL request returned an unexpected row");
        }
        if (count != 1) throw new InvalidOperationException("The fast SQL request returned no rows");
        return new(Stopwatch.GetElapsedTime(started, opened).TotalMilliseconds,
            Stopwatch.GetElapsedTime(opened).TotalMilliseconds, run.WorkerProcessId);
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

    private static async Task<LatencyMeasurement> Measure(DataClient client, int samples, CancellationToken token)
    {
        var arrival = new[] { Channel.CreateUnbounded<(long Scheduled, long Released)>(),
            Channel.CreateUnbounded<(long Scheduled, long Released)>() };
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
                    arrival[index % 2].Writer.TryWrite((scheduled, Stopwatch.GetTimestamp()));
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
        await Task.WhenAll(Consume(arrival[0], 0), Consume(arrival[1], samples));
        return new(data, Stopwatch.GetElapsedTime(started).TotalSeconds);

        async Task Consume(Channel<(long Scheduled, long Released)> lane, int offset)
        {
            int index = offset;
            await foreach (var (scheduled, released) in lane.Reader.ReadAllAsync(token))
            {
                long submitted = Stopwatch.GetTimestamp();
                var result = await ReadLedger(client, token);
                long completed = Stopwatch.GetTimestamp();
                data[index++] = new(Stopwatch.GetElapsedTime(submitted, completed).TotalMilliseconds,
                    Stopwatch.GetElapsedTime(scheduled, completed).TotalMilliseconds,
                    Stopwatch.GetElapsedTime(scheduled, released).TotalMilliseconds,
                    Stopwatch.GetElapsedTime(released, submitted).TotalMilliseconds,
                    result.StartupMs, result.ResultMs, result.ProcessId);
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
            Percentile(data.Select(row => row.ReleaseMs), 0.95), Percentile(data.Select(row => row.QueueMs), 0.95),
            Percentile(data.Select(row => row.StartupMs), 0.95), Percentile(data.Select(row => row.ResultMs), 0.95),
            data.Length / measurements.Sum(window => window.ElapsedSeconds));
    }

    private static double Percentile(IEnumerable<double> samples, double percentile)
    {
        var sorted = samples.Order().ToArray();
        return sorted[(int)Math.Ceiling(sorted.Length * percentile) - 1];
    }
}
