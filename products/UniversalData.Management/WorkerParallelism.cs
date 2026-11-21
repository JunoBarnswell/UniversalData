using System.Text.Json;
using System.Text.Json.Serialization;

namespace UniversalData.Management;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record WorkerParallelism(uint WorkerProcesses = 1, uint ConsumerLanesPerWorker = 1, uint ThreadsPerWorker = 1)
{
    internal static WorkerParallelism? Read(string package, uint limit)
    {
        using var document = JsonDocument.Parse(package);
        var root = document.RootElement;
        uint? nativeThreads = null;
        if (root.TryGetProperty("policy", out var policy) && policy.TryGetProperty("maxConcurrency", out var concurrency))
        {
            if (!concurrency.TryGetUInt32(out uint count) || count == 0 || count > limit)
                throw new UniversalDataException((int)StatusCode.Resource, $"Job parallelism must be between 1 and {limit}");
            nativeThreads = count;
        }
        WorkerParallelism? ordinary = nativeThreads is { } threads ? new(ThreadsPerWorker: threads) : null;
        if (!root.TryGetProperty("dataFlows", out var flows))
            return ordinary;
        if (flows.EnumerateArray().Any(IsKafkaStage))
        {
            var staged = ReadStageParallelism(flows, limit);
            if (nativeThreads > staged.ThreadsPerWorker)
                throw new ArgumentException("Job parallelism exceeds the declared Worker threads");
            return staged;
        }
        var configured = flows.EnumerateArray().Where(flow => flow.TryGetProperty("parallelism", out _)).ToArray();
        if (configured.Length == 0)
            return ordinary;
        var flow = configured[0];
        var plan = flow.GetProperty("parallelism").Deserialize<WorkerParallelism>(UniversalData.Hosting.HostPolicy.JsonOptions)
            ?? throw new ArgumentException("Invalid Data Flow parallelism");
        if (configured.Length != 1 || plan.WorkerProcesses == 0 || plan.WorkerProcesses > limit ||
            plan.ThreadsPerWorker == 0 || plan.ThreadsPerWorker > limit || plan.ConsumerLanesPerWorker == 0 ||
            plan.ConsumerLanesPerWorker > plan.ThreadsPerWorker || nativeThreads > plan.ThreadsPerWorker)
            throw new ArgumentException("Data Flow parallelism exceeds its Worker limits");
        if (plan is { WorkerProcesses: 1, ConsumerLanesPerWorker: 1 })
            return plan;
        var control = root.GetProperty("controlFlow");
        var tasks = control.GetProperty("tasks");
        if (tasks.GetArrayLength() != 1 || tasks[0].GetProperty("kind").GetString() != "dataFlow" ||
            tasks[0].GetProperty("config").GetProperty("pipeline").GetString() != flow.GetProperty("id").GetString() ||
            (control.TryGetProperty("constraints", out var constraints) && constraints.GetArrayLength() != 0))
            throw new ArgumentException("Sharded execution requires one direct Data Flow task");
        return plan;
    }

    private static WorkerParallelism ReadStageParallelism(JsonElement flows, uint limit)
    {
        WorkerParallelism? expected = null;
        foreach (var flow in flows.EnumerateArray()) {
            if (!IsKafkaStage(flow) || !flow.TryGetProperty("parallelism", out var settings))
                throw new ArgumentException("Every staged Data Flow requires declared parallelism");
            var plan = settings.Deserialize<WorkerParallelism>(UniversalData.Hosting.HostPolicy.JsonOptions);
            if (plan is null || plan.WorkerProcesses < 2 || plan.WorkerProcesses > limit ||
                plan.ConsumerLanesPerWorker != 1 || plan.ThreadsPerWorker == 0 || plan.ThreadsPerWorker > limit ||
                expected is not null && expected != plan)
                throw new ArgumentException("Staged Data Flows require identical Worker parallelism with multiple Workers and one lane each");
            expected = plan;
        }
        return expected!;
    }

    internal static bool UsesKafkaStages(string package)
    {
        using var document = JsonDocument.Parse(package);
        return document.RootElement.TryGetProperty("dataFlows", out var flows) && flows.EnumerateArray().Any(IsKafkaStage);
    }

    private static bool IsKafkaStage(JsonElement flow) =>
        flow.TryGetProperty("delivery", out var delivery) && delivery.ValueKind == JsonValueKind.Object &&
        delivery.TryGetProperty("kafkaStage", out var stage) && stage.ValueKind == JsonValueKind.True;

    internal static bool UsesBrokerRecovery(string package)
    {
        using var document = JsonDocument.Parse(package);
        var root = document.RootElement;
        if (!root.TryGetProperty("dataFlows", out var flows) || flows.GetArrayLength() != 1 ||
            !root.TryGetProperty("controlFlow", out var control) ||
            !control.TryGetProperty("tasks", out var tasks) || tasks.GetArrayLength() != 1 ||
            control.TryGetProperty("constraints", out var constraints) && constraints.GetArrayLength() != 0)
            return false;
        var flow = flows[0];
        var task = tasks[0];
        return task.GetProperty("kind").GetString() == "dataFlow" &&
            task.TryGetProperty("config", out var settings) &&
            settings.TryGetProperty("pipeline", out var pipeline) &&
            pipeline.GetString() == flow.GetProperty("id").GetString() &&
            flow.TryGetProperty("delivery", out var delivery) &&
            delivery.TryGetProperty("mode", out var mode) &&
            mode.GetString() == "kafkaTransaction";
    }
}
