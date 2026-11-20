using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal static class KafkaElasticPlanner
{
    internal static KafkaSource ReadSource(this KafkaElasticity settings, string package, uint hostLimit)
    {
        if (settings.MinProcesses is < 1 or > 64 || settings.MaxProcesses < settings.MinProcesses || settings.MaxProcesses > 64 ||
            settings.MinProcesses > hostLimit || settings.TargetLagPerProcess == 0 || settings.MaxPartitionsPerProcess is < 1 or > 64)
            throw new ArgumentException("Invalid Kafka elasticity limits");

        if (!WorkerParallelism.UsesBrokerRecovery(package) || WorkerParallelism.UsesKafkaStages(package))
            throw new ArgumentException("Elasticity requires one direct transactional Kafka Data Flow");
        using var document = JsonDocument.Parse(package);
        var flow = document.RootElement.GetProperty("dataFlows")[0];
        if (flow.TryGetProperty("parallelism", out _))
            throw new ArgumentException("An elastic Data Flow cannot declare fixed parallelism");
        var sources = flow.GetProperty("components").EnumerateArray()
            .Where(component => component.GetProperty("component").GetString() == "mq.source").ToArray();
        if (sources.Length != 1)
            throw new ArgumentException("Elasticity requires one Kafka Source");
        var source = sources[0].GetProperty("config");
        if (source.TryGetProperty("partition", out _) || source.TryGetProperty("endOffsets", out _) ||
            source.TryGetProperty("partitionByWorker", out _))
            throw new ArgumentException("Elasticity requires an open Kafka topic without fixed partitions or cuts");
        return new(source.GetProperty("connection").GetString()!,source.GetProperty("address").GetString()!,
            source.GetProperty("group").GetString()!,source.GetProperty("initialPosition").GetString()!);
    }

    internal static KafkaElasticState Freeze(this KafkaElasticity settings, KafkaSource source, KafkaPartitionPosition[] positions, uint limit, long now)
    {
        int count = positions.Length;
        uint perProcess = Math.Min(settings.MaxPartitionsPerProcess, limit);
        int initial = checked((int)Math.Max(settings.MinProcesses, ((uint)count + perProcess - 1) / perProcess));
        if (count == 0 || initial > count || initial > Math.Min(settings.MaxProcesses,limit))
            throw new ArgumentException("Kafka partitions cannot fit the elastic process bounds");
        var frozen = new KafkaPartitionInput[count];
        for (int index = 0; index < count; index++)
        {
            var value = positions[index];
            long start = value.Committed >= 0 ? value.Committed : source.InitialPosition switch
            {
                "earliest" => value.Earliest,
                "latest" => value.Latest,
                _ => -1
            };
            if (value.Partition != index || start < value.Earliest || start > value.Latest)
                throw new UniversalDataException((int)StatusCode.Fenced,"Kafka starting position is unavailable");
            frozen[index] = new(value.Partition,start);
        }
        var groups = new KafkaElasticGroup[initial];
        for (int index = 0; index < initial; index++)
            groups[index] = KafkaElasticGroup.Create(frozen.Where((_, part) => part % initial == index), false);
        return new(1,source,frozen,groups,LastChangeMs:now,LastSampleMs:now);
    }

    internal static KafkaElasticState Observe(this KafkaElasticity settings, KafkaElasticState plan, KafkaPartitionPosition[] values, uint limit, long now)
    {
        if (values.Length < plan.Frozen.Length)
            return plan with { Reason="KafkaPartitionMissing",LastSampleMs=now,HighSamples=0,LowSinceMs=0 };
        ulong lag = 0;
        for (int i = 0; i < plan.Frozen.Length; i++)
        {
            var position = values[i];
            var initial = plan.Frozen[i];
            long committed = position.Committed < 0 ? initial.StartOffset : position.Committed;
            if (position.Partition != initial.Partition || committed < initial.StartOffset ||
                committed < position.Earliest || committed > position.Latest)
                return plan with { Reason="KafkaOffsetMismatch",LastSampleMs=now,HighSamples=0,LowSinceMs=0 };
            lag += Math.Min((ulong)(position.Latest - committed), ulong.MaxValue - lag);
        }
        int count = plan.Groups.Length;
        ulong threshold = (ulong)count * settings.TargetLagPerProcess;
        int high = lag > threshold ? Math.Min(3,plan.HighSamples + 1) : 0;
        long low = lag < (ulong)Math.Max(1,count - 1) * settings.TargetLagPerProcess / 2 ?
            plan.LowSinceMs == 0 ? now : plan.LowSinceMs : 0;
        var sampled = plan with { LastSampleMs=now, Lag=lag, HighSamples=high,LowSinceMs=low,Reason=null };
        if (now - plan.LastChangeMs < 30000 || plan.Next is not null)
            return sampled;
        if (high >= 3 && count < Math.Min(Math.Min(settings.MaxProcesses,limit),(uint)values.Length))
        {
            var largest = plan.Groups.OrderByDescending(group => group.Partitions.Length).First();
            if (largest.Partitions.Length <= 1) return sampled;
            int midpoint = largest.Partitions.Length / 2;
            return sampled with { Next = [..plan.Groups.Where(group => group != largest),
                KafkaElasticGroup.Create(largest.Partitions.Take(midpoint),true),
                KafkaElasticGroup.Create(largest.Partitions.Skip(midpoint),true)] };
        }
        if (low != 0 && now - low >= 120000 && count > settings.MinProcesses)
        {
            var pair = plan.Groups.OrderBy(group => group.Partitions.Length).Take(2).ToArray();
            if (pair[0].Partitions.Length + pair[1].Partitions.Length <= Math.Min(settings.MaxPartitionsPerProcess,limit))
                return sampled with { Next = [..plan.Groups.Where(group => !pair.Contains(group)),
                    KafkaElasticGroup.Create(pair.SelectMany(group => group.Partitions),true)] };
        }
        return sampled;
    }
}

public sealed record KafkaSource(string Connection, string Topic, string Group, string InitialPosition);

public sealed record KafkaElasticGroup(string Id, KafkaPartitionInput[] Partitions, WorkerPlacement? Reservation = null)
{
    internal static KafkaElasticGroup Create(IEnumerable<KafkaPartitionInput> partitions, bool restore) =>
        new(Guid.NewGuid().ToString("N"),partitions.OrderBy(value => value.Partition)
            .Select(value => value with { RequireCheckpoint=restore }).ToArray());

    internal WorkerParallelism Parallelism =>
        new(1,(uint)Partitions.Length,(uint)Partitions.Length);
}

public sealed record KafkaElasticState(ulong Revision, KafkaSource Source, KafkaPartitionInput[] Frozen,
    KafkaElasticGroup[] Groups, KafkaElasticGroup[]? Next = null, string? Reason = null,
    long LastChangeMs = 0, long LastSampleMs = 0, ulong Lag = 0, int HighSamples = 0, long LowSinceMs = 0);
