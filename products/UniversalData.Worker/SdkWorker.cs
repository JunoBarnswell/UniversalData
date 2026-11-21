using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal static class SdkWorker
{
    internal static async Task<int> RunAsync(HostPolicy policy, SdkWorkerConfiguration configuration,
        WorkerSourceAdmission? admission)
    {
        Console.SetOut(TextWriter.Null);
        using var lifetime = new CancellationTokenSource();
        using var finished = new CancellationTokenSource();
        var ownerFinished = finished.Token;
        // Console.In blocks while the owner is alive. Keep the read off the request thread pool.
        _ = Task.Factory.StartNew(() => WatchOwnerAsync(Console.In, lifetime, ownerFinished),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
        _ = EnforceOwnerExitAsync(lifetime.Token, finished.Token, configuration.Options.CancellationGrace);
        var sessions = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        using var slots = new SemaphoreSlim(configuration.Capacity, configuration.Capacity);
        uint maxConnections = checked((uint)(configuration.Capacity * configuration.Options.MaxBranchesPerSession *
            configuration.Options.MaxConnectionsPerSource));
        using var pool = new ConnectionPool(maxConnections, policy.MemoryLimit, admission?.File, admission?.MaxSessions ?? 0);
        var trim = admission is null ? Task.CompletedTask : pool.TrimIdleAsync(lifetime.Token);
        var work = new HashSet<Task>();
        NamedPipeServerStream? listener = Listen(configuration.Endpoint);
        try
        {
            while (!lifetime.IsCancellationRequested)
            {
                await listener.WaitForConnectionAsync(lifetime.Token);
                work.RemoveWhere(task => task.IsCompletedSuccessfully);
                var accepted = listener;
                listener = Listen(configuration.Endpoint);
                work.Add(ServeAsync(policy, configuration, pool, accepted, sessions, slots, lifetime.Token));
            }
            return 0;
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { return 0; }
        finally
        {
            listener.Dispose();
            await lifetime.CancelAsync();
            await Task.WhenAll(work);
            await trim;
            await finished.CancelAsync();
        }
    }

    private static NamedPipeServerStream Listen(string endpoint) =>
        SdkSessionProtocol.Listen(endpoint, NamedPipeServerStream.MaxAllowedServerInstances);

    private static async Task ServeAsync(HostPolicy policy, SdkWorkerConfiguration configuration,
        ConnectionPool pool, NamedPipeServerStream owner, ConcurrentDictionary<string, byte> sessions, SemaphoreSlim slots,
        CancellationToken stopping)
    {
        await using var enrolled = owner;
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        handshake.CancelAfter(TimeSpan.FromSeconds(15));
        try
        {
            var frame = await WorkerProtocol.ReadAsync<SdkSessionAssignment>(owner, handshake.Token);
            using var batch = frame.Batch;
            var assignment = frame.Message;
            if (batch is not null || !SdkSessionProtocol.Authenticate(assignment.Revision, assignment.WorkerCapability, configuration.Capability) ||
                !Guid.TryParseExact(assignment.SessionId, "N", out _) || assignment.Capability is not { Length: 64 } ||
                !SdkSessionProtocol.ValidEndpoint(assignment.Endpoint))
                throw SdkSessionHostClient.Failure(StatusCode.Auth, "Invalid SDK session assignment");
            if (!slots.Wait(0)) throw SdkSessionHostClient.Failure(StatusCode.Busy, "SDK Worker session capacity is exhausted");
            bool registered = sessions.TryAdd(assignment.SessionId, 0);
            try
            {
                if (!registered) throw SdkSessionHostClient.Failure(StatusCode.Fenced, "SDK session already exists");
                await using var direct = SdkSessionProtocol.Listen(assignment.Endpoint);
                await ReportAsync(owner, StatusCode.Ok, handshake.Token);
                using var session = CancellationTokenSource.CreateLinkedTokenSource(stopping);
                using var finished = new CancellationTokenSource();
                var cancel = WatchSessionAsync(owner, session, finished.Token);
                try { await new SdkWorkerSession(policy, configuration, assignment, pool).RunAsync(direct, session); }
                finally
                {
                    await finished.CancelAsync();
                    await cancel;
                }
            }
            finally
            {
                if (registered) sessions.TryRemove(assignment.SessionId, out _);
                slots.Release();
            }
            await ReportAsync(owner, StatusCode.Ok, stopping);
        }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException or
            OperationCanceledException or UniversalDataException or InvalidOperationException or ArgumentException)
        {
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
                await ReportAsync(owner, (error as UniversalDataException)?.Code ?? StatusCode.WorkerLost, timeout.Token);
            }
            catch (Exception reply) when (reply is IOException or OperationCanceledException or InvalidOperationException) { }
        }
    }

    private static ValueTask ReportAsync(Stream owner, StatusCode status, CancellationToken cancellation) =>
        WorkerProtocol.WriteAsync(owner, new SdkHostStatus(ProductInfo.WorkerRevision, status), null, cancellation);

    private static async Task WatchSessionAsync(Stream owner, CancellationTokenSource lifetime, CancellationToken finished)
    {
        try { await owner.ReadExactlyAsync(new byte[1], finished); }
        catch (IOException) { }
        catch (OperationCanceledException) when (finished.IsCancellationRequested) { }
        if (!finished.IsCancellationRequested) await lifetime.CancelAsync();
    }

    private static async Task WatchOwnerAsync(TextReader owner, CancellationTokenSource lifetime, CancellationToken finished)
    {
        try { await WorkerProtocol.ReadLine(owner, 64, finished); }
        catch (IOException) { }
        catch (OperationCanceledException) when (finished.IsCancellationRequested) { }
        if (!finished.IsCancellationRequested) await lifetime.CancelAsync();
    }

    private static async Task EnforceOwnerExitAsync(CancellationToken lifetime, CancellationToken finished, TimeSpan grace)
    {
        using var ended = CancellationTokenSource.CreateLinkedTokenSource(lifetime, finished);
        try { await Task.Delay(Timeout.InfiniteTimeSpan, ended.Token); }
        catch (OperationCanceledException) { }
        if (finished.IsCancellationRequested) return;
        try { await Task.Delay(grace, finished); }
        catch (OperationCanceledException) { return; }
        Environment.Exit(1);
    }
}
