using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData;

namespace FluentAcceptance;

internal static class MqBurstCase
{
    private const int WorkerCount = 3;

    internal static async Task RunAsync(string policy, string credentialFile, string queue,
        string directory, ulong expected, uint concurrency, int timeoutSeconds)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
        var token = deadline.Token;
        var builder = DataClient.CreateBuilder(policy, "burst")
            .AddSource(new("broker", DataSourceKind.RabbitMq, File.ReadAllText(credentialFile)))
            .ConfigureWorkers(worker => worker.MaxConcurrentSessions(WorkerCount).MaxSessionsPerWorker(1)
                .MaxBranchesPerSession(1).MaxConnectionsPerSource(WorkerCount));
        for (int index = 0; index < WorkerCount; index++)
            builder.AddSqlite($"sink-{index}", "Data Source=" + Path.Combine(directory, $"sink-{index}.db"));

        await using var data = builder.Build();
        string module = typeof(MqBurstModule).Assembly.Location;
        string digest = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(module))).ToLowerInvariant();
        long start = Stopwatch.GetTimestamp();
        var runs = await Task.WhenAll(Enumerable.Range(0, WorkerCount).Select(index =>
            data.StartAsync(PackageFor(index), cancellation: token)));
        try
        {
            int[] pids = runs.Select(run => run.WorkerProcessId).ToArray();
            if (pids.Distinct().Count() != WorkerCount || pids.Contains(Environment.ProcessId))
                throw new InvalidOperationException("The burst must use three separate Worker processes");
            Console.WriteLine(JsonSerializer.Serialize(new { Stage = "workers", Pids = pids }));
            var completions = runs.Select(run => run.WaitAsync(token)).ToArray();
            double nextReport = 0;
            while (true)
            {
                if (completions.FirstOrDefault(completion => completion.IsCompleted) is { } finished)
                {
                    await finished;
                    throw new InvalidOperationException("A Worker stopped before the queue was drained");
                }
                var progress = await Task.WhenAll(runs.Select(run => run.ProgressAsync(token)));
                ulong committed = progress.Aggregate(0UL, (count, run) => checked(count + run.CommittedRows));
                ulong acknowledged = progress.Aggregate(0UL, (count, run) => checked(count + run.AcknowledgedRows));
                if (committed > expected || acknowledged > expected || progress.Any(run => run.RejectedRows != 0))
                    throw new InvalidOperationException("The burst committed an unexpected number of rows or rejected a message");
                double elapsed = Stopwatch.GetElapsedTime(start).TotalSeconds;
                if (elapsed >= nextReport || acknowledged == expected)
                {
                    Console.WriteLine(JsonSerializer.Serialize(new { Stage = "progress", Committed = committed,
                        Acknowledged = acknowledged, ElapsedSeconds = elapsed }));
                    nextReport = elapsed + 10;
                }
                if (acknowledged == expected) break;
                await Task.Delay(200, token);
            }

            await Task.WhenAll(runs.Select(run => run.StopAsync(token)));
            var metrics = await Task.WhenAll(completions);
            if (metrics.Any(item => item.Result != StatusCode.Ok || item.State != ExecutionState.Stopped ||
                item.RejectedRows != 0 || item.CommittedRows != item.AcknowledgedRows) ||
                metrics.Aggregate(0UL, (count, item) => checked(count + item.AcknowledgedRows)) != expected)
                throw new InvalidOperationException("Native Worker completion and RabbitMQ acknowledgements differ");
            Console.WriteLine(JsonSerializer.Serialize(new { Stage = "complete", Workers = pids.Select((pid, index) =>
            {
                using var process = Process.GetProcessById(pid);
                return new { Pid = pid, metrics[index].CommittedRows, metrics[index].AcknowledgedRows,
                    process.PeakWorkingSet64 };
            }).ToArray(), ElapsedSeconds = Stopwatch.GetElapsedTime(start).TotalSeconds }));
        }
        finally
        {
            foreach (var run in runs) await run.DisposeAsync();
        }

        Package PackageFor(int index)
        {
            var job = DataJob.Define($"mq-burst-{index}")
                .WithOptions(new(BatchRows: concurrency, EdgeCapacity: 1, MessagePrefetch: concurrency))
                .FromMessages<MqBurstRow>("broker", queue, queue: true)
                .ToTable($"sink-{index}", "mq_completed").Append();
            var document = JsonNode.Parse(data.Compile(job).ToJson())!;
            document["extensions"] = JsonSerializer.SerializeToNode(new[]
            {
                new { kind = "managedAssembly", path = module, sha256 = digest }
            });
            var flow = document["dataFlows"]![0]!;
            const string delay = "business-delay";
            flow["components"]!.AsArray().Add(JsonSerializer.SerializeToNode(new
            {
                id = delay, component = MqBurstModule.Component,
                config = new { timingFile = Path.Combine(directory, $"sink-{index}.timing.jsonl") }
            }));
            var paths = flow["paths"]!.AsArray();
            string sink = paths[0]!["to"]!.GetValue<string>();
            paths[0]!["to"] = delay;
            paths.Add(JsonSerializer.SerializeToNode(new { from = delay, to = sink, output = 0, input = 0 }));
            var result = Package.FromJson(document.ToJsonString());
            return result.Value ?? throw new UniversalDataException((int)result.Code,
                "The burst demo package is invalid", result.Error);
        }
    }
}
