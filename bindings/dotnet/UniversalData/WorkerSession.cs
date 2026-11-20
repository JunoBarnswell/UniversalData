using System.Diagnostics;
using System.Text.Json;

namespace UniversalData;

internal sealed class WorkerSession : IAsyncDisposable
{
    private readonly Process process;
    private readonly Stream input;
    private readonly Stream output;
    private readonly SemaphoreSlim operations = new(1, 1);
    private readonly Task stderr;
    private long sequence;
    private int closed, disposed;

    internal int ProcessId => process.Id;

    private WorkerSession(Process process)
    {
        this.process = process;
        input = process.StandardInput.BaseStream;
        output = process.StandardOutput.BaseStream;
        stderr = WorkerProtocol.Discard(process.StandardError, CancellationToken.None);
    }

    internal static Result<WorkerSession> Start(string? workerPath)
    {
        try
        {
            string executable = workerPath ?? Environment.GetEnvironmentVariable("UD_WORKER_PATH") ??
                Path.Combine(AppContext.BaseDirectory, "runtimes",
                    OperatingSystem.IsWindows() ? "win-x64" : "linux-x64", "worker",
                    OperatingSystem.IsWindows() ? "ud_worker.exe" : "ud_worker");
            if (!Path.IsPathFullyQualified(executable) || !File.Exists(executable))
                return new(StatusCode.DriverUnavailable, null, new(ErrorSource.Worker,
                    StatusCode.DriverUnavailable, "The UniversalData Worker executable is unavailable"));
            var start = new ProcessStartInfo(executable)
            {
                UseShellExecute = false, RedirectStandardInput = true, RedirectStandardOutput = true,
                RedirectStandardError = true, CreateNoWindow = true
            };
            if (!OperatingSystem.IsWindows() && workerPath is null && Environment.GetEnvironmentVariable("UD_WORKER_PATH") is null)
                File.SetUnixFileMode(executable, File.GetUnixFileMode(executable) | UnixFileMode.UserExecute);
            start.ArgumentList.Add("session");
            var process = Process.Start(start) ?? throw new IOException("Worker could not start");
            return Result<WorkerSession>.Success(new(process));
        }
        catch (Exception error) when (error is IOException or InvalidOperationException or UnauthorizedAccessException or System.ComponentModel.Win32Exception)
        {
            return new(StatusCode.WorkerLost, null, new(ErrorSource.Worker, StatusCode.WorkerLost,
                "Could not start the UniversalData Worker"));
        }
    }

    internal async ValueTask<(WorkerResponseFrame Response, WorkerBatchBuffer? Batch)> InvokeAsync(
        string operation, object? values = null, WorkerBatchBuffer? batch = null, bool mayWrite = false,
        Func<uint, (WorkerBatchFrame Control, WorkerBatchBuffer? Batch)>? source = null,
        CancellationToken cancellation = default)
    {
        try { await operations.WaitAsync(cancellation).ConfigureAwait(false); }
        catch (OperationCanceledException) { return Failure(StatusCode.Cancelled, false); }
        bool sent = false;
        try
        {
            if (Volatile.Read(ref closed) != 0 || process.HasExited)
                return Failure(StatusCode.WorkerLost, false);
            if (cancellation.IsCancellationRequested)
                return Failure(StatusCode.Cancelled, false);
            long id = checked(++sequence);
            var request = new WorkerRequestFrame(ProductInfo.WorkerRevision, id, operation,
                JsonSerializer.SerializeToElement(values ?? new {}, WorkerProtocol.Json));
            sent = true;
            await WorkerProtocol.WriteAsync(input, request, batch, cancellation).ConfigureAwait(false);
            while (true)
            {
                var result = await WorkerProtocol.ReadAsync<WorkerResponseFrame>(output, cancellation).ConfigureAwait(false);
                if (result.Message.Revision != ProductInfo.WorkerRevision || result.Message.Sequence != id)
                {
                    result.Batch?.Dispose();
                    throw new InvalidDataException("Worker response identity differs");
                }
                if (result.Message.Kind == "result")
                {
                    if (result.Batch is not null && (operation != "sql.read" || result.Message.Code != StatusCode.Ok))
                    {
                        result.Batch.Dispose();
                        throw new InvalidDataException("Unexpected Worker result batch");
                    }
                    return (result.Message, result.Batch);
                }
                if (result.Message.Kind != "input" || source is null || result.Batch is not null ||
                    result.Message.Capacity == 0)
                {
                    result.Batch?.Dispose();
                    throw new InvalidDataException("Invalid Worker response");
                }
                var next = source(result.Message.Capacity);
                using var transmitted = next.Batch;
                // Send the cancelled input to the transaction owner and wait for its rollback result.
                if (next.Control.Code == StatusCode.Cancelled)
                    cancellation = CancellationToken.None;
                await WorkerProtocol.WriteAsync(input, next.Control with
                {
                    Revision = ProductInfo.WorkerRevision, Sequence = id
                }, transmitted, cancellation).ConfigureAwait(false);
            }
        }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException or OperationCanceledException or InvalidOperationException)
        {
            var cause = error is OperationCanceledException ? StatusCode.Cancelled :
                error is InvalidDataException or JsonException ? StatusCode.Protocol : StatusCode.WorkerLost;
            var failure = Failure(cause, sent && mayWrite);
            Terminate();
            return failure;
        }
        finally { operations.Release(); }
    }

    private (WorkerResponseFrame, WorkerBatchBuffer?) Failure(StatusCode cause, bool uncertain)
    {
        int? exitCode = process.HasExited ? process.ExitCode : null;
        var error = new OperationError(ErrorSource.Worker, cause, cause switch
        {
            StatusCode.Cancelled => "Worker operation cancelled",
            StatusCode.Protocol => "Invalid Worker protocol message",
            _ => "Worker process stopped responding"
        }, ExitCode: exitCode);
        return (new(ProductInfo.WorkerRevision, sequence, "result",
            uncertain ? StatusCode.CommitUnknown : cause, Error: error), null);
    }

    private void Terminate()
    {
        if (Interlocked.Exchange(ref closed, 1) == 0 && !process.HasExited)
            try { process.Kill(entireProcessTree: true); }
            catch (InvalidOperationException) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;
        await operations.WaitAsync().ConfigureAwait(false);
        try
        {
            Terminate();
            await process.WaitForExitAsync().ConfigureAwait(false);
            await stderr.ConfigureAwait(false);
            process.Dispose();
        }
        finally { operations.Release(); }
    }
}
