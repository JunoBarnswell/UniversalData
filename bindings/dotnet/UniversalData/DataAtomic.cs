using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

public sealed class DataAtomic
{
    private readonly DataContext parent;
    private readonly SdkSourceBinding[] participants;

    internal DataAtomic(DataContext parent, SdkSourceBinding[] participants)
    { this.parent = parent; this.participants = participants; }

    public async Task RunAsync(Func<DataContext, CancellationToken, Task> body, CancellationToken cancellation = default) =>
        _ = await RunAsync(async (ctx, ct) => { await body(ctx, ct).ConfigureAwait(false); return true; }, cancellation)
            .ConfigureAwait(false);

    public Task<TResult> RunAsync<TResult>(Func<DataContext, CancellationToken, Task<TResult>> body,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        return SdkAtomicRuntime.RunAsync(parent, participants, body, cancellation);
    }
}
