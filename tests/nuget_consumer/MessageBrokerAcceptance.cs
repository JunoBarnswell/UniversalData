using System.Text.Json;
using SqlSugar;
using UniversalData;

internal static class MessageBrokerAcceptance
{
    private const int ExpectedRows = 2;

    internal static async Task<Dictionary<string, MqAcceptanceMetrics>> RunAsync(
        IDataClient data, IReadOnlyDictionary<string, DataSourceKind> kinds,
        IReadOnlyDictionary<string, string> targets, IReadOnlyDictionary<string, string> groups,
        string database, CancellationToken cancellation)
    {
        var results = new Dictionary<string, MqAcceptanceMetrics>(StringComparer.Ordinal);
        foreach (var (broker, kind) in kinds.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            Console.Error.WriteLine($"Validating the installed {broker} connector");
            if (!targets.TryGetValue(broker, out var target))
                throw new InvalidOperationException($"The {broker} fixture did not provide a JSON target address");
            bool queue = kind is DataSourceKind.RabbitMq or DataSourceKind.ActiveMq;
            string? group = kind is DataSourceKind.Kafka or DataSourceKind.RocketMq
                ? groups.GetValueOrDefault(broker)
                : null;
            if (kind is DataSourceKind.Kafka or DataSourceKind.RocketMq && string.IsNullOrWhiteSpace(group))
                throw new InvalidOperationException($"The {broker} fixture did not provide a consumer group");

            var publish = await data.RunAsync(PublishJob(broker, target, queue), cancellation)
                .ConfigureAwait(false);
            Require(publish.Metrics.State == ExecutionState.Succeeded &&
                publish.Metrics.Result == StatusCode.Ok && publish.CommittedRows == ExpectedRows &&
                publish.RejectedRows == 0,
                $"The {broker} Native SQLite-to-MQ job did not commit exactly {ExpectedRows} messages");

            var consume = await ConsumeUntilCommittedAsync(data,
                ConsumeJob(broker, target, group, queue, kind == DataSourceKind.Kafka), database,
                cancellation).ConfigureAwait(false);
            Require(consume.State is ExecutionState.Cancelled or ExecutionState.Stopped or ExecutionState.Succeeded,
                $"The {broker} Native MQ-to-SQLite job ended in {consume.State}");
            Require(consume.CommittedRows == ExpectedRows && consume.AcknowledgedRows == ExpectedRows &&
                consume.RejectedRows == 0,
                $"The {broker} Native MQ-to-SQLite job did not commit and acknowledge all messages");
            await VerifyRowsAsync(database, broker, cancellation).ConfigureAwait(false);
            await data.RunAsync(DataJob.Define($"clear-mq-inbox-{broker}")
                .ExecuteSql("finance", "DELETE FROM nuget_mq_inbox"), cancellation).ConfigureAwait(false);
            results[broker] = new(publish.CommittedRows, consume.CommittedRows, consume.AcknowledgedRows);
        }
        return results;
    }

    private static DataJob<object> PublishJob(string broker, string target, bool queue)
    {
        var job = DataJob.Define<object>($"mq-publish-{broker}");
        return job.FromTable<BrokerInputRow>("finance", "nuget_mq_outbox")
            .Select(row => new BrokerMessageRow { Id = row.Id, Label = row.Label })
            .ToMessages(broker, target, queue);
    }

    private static DataJob<object> ConsumeJob(string broker, string source, string? group,
        bool queue, bool earliest)
    {
        var job = DataJob.Define<object>($"mq-consume-{broker}");
        return job.FromMessages<BrokerMessageRow>(broker, source, group, queue,
                earliest ? "earliest" : "require")
            .ToTable("finance", "nuget_mq_inbox")
            .Append();
    }

    private static async Task<ExecutionMetrics> ConsumeUntilCommittedAsync(IDataClient data,
        DataJob<object> job, string database, CancellationToken cancellation)
    {
        await using var run = await data.StartAsync(job, new object(), cancellation).ConfigureAwait(false);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(75));
        var completion = run.WaitAsync(deadline.Token);
        try
        {
            while (!completion.IsCompleted)
            {
                var progress = await run.ProgressAsync(deadline.Token).ConfigureAwait(false);
                if (Confirmed(progress) &&
                    await CountRowsAsync(database, deadline.Token).ConfigureAwait(false) == ExpectedRows)
                {
                    await run.CancelAsync(deadline.Token).ConfigureAwait(false);
                    break;
                }
                await Task.WhenAny(completion, Task.Delay(TimeSpan.FromMilliseconds(100), deadline.Token))
                    .ConfigureAwait(false);
            }
            return await completion.ConfigureAwait(false);
        }
        catch (DataExecutionException error) when (error.Code == StatusCode.Cancelled && error.Metrics is not null)
        {
            return error.Metrics!;
        }
        finally
        {
            deadline.Cancel();
        }
    }

    private static bool Confirmed(DataRunProgress progress) =>
        progress.Pipelines.Any(pipeline => pipeline.ConfirmedEpoch > 0);

    private static async Task<int> CountRowsAsync(string database, CancellationToken cancellation)
    {
        using var verifier = new SqlSugarClient(new ConnectionConfig { DbType = DbType.Sqlite,
            ConnectionString = "Data Source=" + database, IsAutoCloseConnection = true });
        return Convert.ToInt32(await verifier.Ado.GetScalarAsync(
            "SELECT COUNT(*) FROM nuget_mq_inbox").ConfigureAwait(false));
    }

    private static async Task VerifyRowsAsync(string database, string broker, CancellationToken cancellation)
    {
        using var verifier = new SqlSugarClient(new ConnectionConfig { DbType = DbType.Sqlite,
            ConnectionString = "Data Source=" + database, IsAutoCloseConnection = true });
        var rows = await verifier.Queryable<BrokerMessageRow>().OrderBy(row => row.Id).ToListAsync()
            .ConfigureAwait(false);
        if (rows.Count != ExpectedRows || rows[0].Id != 1 || rows[0].Label != "broker-record" ||
            rows[1].Id != 2 || rows[1].Label is not null)
            throw new InvalidOperationException($"The {broker} independent SQLite readback differs from the MQ payload");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
