using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

internal sealed class SdkTransactionScope : IDisposable
{
    private readonly object gate = new();
    private readonly Dictionary<string, Branch> branches;
    private readonly CancellationTokenSource lifetime = new();
    private bool closed;
    private int active;

    internal SdkTransactionScope(string name, Func<CancellationToken, ISqlSugarClient> open) =>
        branches = new(StringComparer.Ordinal) { [name] = new(open) };

    internal SdkTransactionScope(IEnumerable<SdkSourceBinding> bindings, IntPtr activation) =>
        branches = bindings.ToDictionary(binding => binding.Definition.Name,
            binding => new Branch(token => OfficialSugar.OpenActivation(activation, binding.Definition.Name, token)), StringComparer.Ordinal);

    internal async Task<TResult> UseAsync<TResult>(string name, Func<ISqlSugarClient, CancellationToken, Task<TResult>> callback,
        CancellationToken cancellation)
    {
        Branch branch;
        CancellationTokenSource tokens;
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(closed, this);
            if (!branches.TryGetValue(name, out branch!))
                throw DataSourceRegistry.SourceError(StatusCode.Auth, "The database is outside the transaction participants");
            if (!branch.Access.Wait(0))
                throw DataSourceRegistry.SourceError(StatusCode.Busy, "The transaction participant already has an active operation");
            ++active;
            tokens = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        }
        using (tokens)
        {
            try
            {
                tokens.Token.ThrowIfCancellationRequested();
                using var client = branch.Open(tokens.Token);
                return DataDatabase.CheckResult(await callback(client, tokens.Token).ConfigureAwait(false));
            }
            finally
            {
                lock (gate) --active;
                branch.Access.Release();
            }
        }
    }

    internal void RequireIdle()
    {
        lock (gate)
            if (active != 0)
                throw DataSourceRegistry.SourceError(StatusCode.Busy, "The transaction callback left database operations running");
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (closed) return;
            closed = true;
        }
        lifetime.Cancel();
        try
        {
            foreach (var branch in branches.Values)
            {
                branch.Access.Wait();
                branch.Access.Release();
            }
        }
        finally { lifetime.Dispose(); }
    }

    private sealed class Branch(Func<CancellationToken, ISqlSugarClient> open)
    {
        internal SemaphoreSlim Access { get; } = new(1, 1);
        internal ISqlSugarClient Open(CancellationToken cancellation) => open(cancellation);
    }
}
