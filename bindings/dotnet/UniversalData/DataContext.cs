using System.Collections.Concurrent;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

public sealed class DataContext
{
    private static readonly AsyncLocal<DataContext?> ambient = new();
    private readonly bool isBranch;
    private readonly CancellationToken cancellation;
    private int closed;

    internal DataContext(SdkInvocationState state, CancellationToken cancellation, SdkTransactionScope? transaction = null,
        bool isBranch = false)
    {
        State = state;
        this.cancellation = cancellation;
        Transaction = transaction;
        this.isBranch = isBranch;
    }

    internal SdkInvocationState State { get; }
    internal SdkTransactionScope? Transaction { get; }
    public DataRunReference RunReference => State.Reference;
    public static DataContext Current
    {
        get
        {
            var context = ambient.Value ?? throw new InvalidOperationException("No SDK function is running in this scope");
            context.CheckOpen();
            return context;
        }
    }

    public DataDatabase Database(string? name = null)
    {
        CheckOpen();
        return new(this, ResolveBinding(State.Sources, name, State.DefaultSource, false));
    }

    public DataHttp Http(string name)
    {
        CheckOpen();
        if (Transaction is not null) throw new InvalidOperationException("HTTP requires a scope outside the database transaction");
        return new(this, ResolveBinding(State.Sources, name, null, true));
    }

    public DataAtomic Atomic(string first, string second, params string[] additional) => Atomic([first, second, .. additional]);

    public DataAtomic Atomic(IEnumerable<string> participantNames)
    {
        CheckOpen();
        ArgumentNullException.ThrowIfNull(participantNames);
        var names = participantNames.Take(65).ToArray();
        if (names.Length is < 2 or > 64 || names.Distinct(StringComparer.Ordinal).Count() != names.Length)
            throw new ArgumentException("An atomic operation needs two or more distinct databases", nameof(participantNames));
        if (Transaction is not null) throw new InvalidOperationException("The current scope already owns a transaction");
        return new(this, names.Select(name => ResolveBinding(State.Sources, name, null, false)).ToArray());
    }

    public async Task<(TLeft, TRight)> ParallelAsync<TLeft, TRight>(
        Func<DataContext, CancellationToken, Task<TLeft>> left, Func<DataContext, CancellationToken, Task<TRight>> right,
        CancellationToken cancellation = default)
    {
        var results = await ParallelAsync<Func<DataContext, CancellationToken, Task<object?>>, object?>(
            [async (ctx, ct) => await left(ctx, ct).ConfigureAwait(false),
             async (ctx, ct) => await right(ctx, ct).ConfigureAwait(false)],
            (ctx, work, ct) => work(ctx, ct), cancellation).ConfigureAwait(false);
        return ((TLeft)results[0]!, (TRight)results[1]!);
    }

    public async Task<(TFirst, TSecond, TThird)> ParallelAsync<TFirst, TSecond, TThird>(
        Func<DataContext, CancellationToken, Task<TFirst>> first, Func<DataContext, CancellationToken, Task<TSecond>> second,
        Func<DataContext, CancellationToken, Task<TThird>> third, CancellationToken cancellation = default)
    {
        var results = await ParallelAsync<Func<DataContext, CancellationToken, Task<object?>>, object?>(
            [async (ctx, ct) => await first(ctx, ct).ConfigureAwait(false),
             async (ctx, ct) => await second(ctx, ct).ConfigureAwait(false),
             async (ctx, ct) => await third(ctx, ct).ConfigureAwait(false)],
            (ctx, work, ct) => work(ctx, ct), cancellation).ConfigureAwait(false);
        return ((TFirst)results[0]!, (TSecond)results[1]!, (TThird)results[2]!);
    }

    public async Task<IReadOnlyList<TResult>> ParallelAsync<TItem, TResult>(IEnumerable<TItem> items,
        Func<DataContext, TItem, CancellationToken, Task<TResult>> body, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(body);
        using var siblings = Link(cancellation);
        using var input = items.GetEnumerator();
        var results = new List<TResult>();
        var cursor = new object();
        ExceptionDispatchInfo? failure = null;
        async Task RunBranch()
        {
            await Task.Yield();
            try
            {
                while (true)
                {
                    siblings.Token.ThrowIfCancellationRequested();
                    TItem item;
                    int index;
                    lock (cursor)
                    {
                        if (!input.MoveNext()) return;
                        item = input.Current;
                        index = results.Count;
                        results.Add(default!);
                    }
                    bool seat = !isBranch;
                    if (seat) await State.Branches.WaitAsync(siblings.Token).ConfigureAwait(false);
                    try
                    {
                        var branch = new DataContext(State, siblings.Token, Transaction, true);
                        var value = await branch.InvokeAsync((ctx, ct) => body(ctx, item, ct)).ConfigureAwait(false);
                        lock (cursor) results[index] = value;
                    }
                    finally { if (seat) State.Branches.Release(); }
                }
            }
            catch (Exception error)
            {
                if (Interlocked.CompareExchange(ref failure, ExceptionDispatchInfo.Capture(error), null) is null)
                    await siblings.CancelAsync().ConfigureAwait(false);
            }
        }
        var running = new Task[isBranch ? 1 : State.Environment.MaxBranches];
        for (int index = 0; index < running.Length; index++) running[index] = RunBranch();
        await Task.WhenAll(running).ConfigureAwait(false);
        failure?.Throw();
        return results;
    }

    internal CancellationTokenSource Link(CancellationToken token)
    {
        CheckOpen();
        return CancellationTokenSource.CreateLinkedTokenSource(cancellation, token);
    }

    internal void CheckOpen() => ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);

    internal async Task<T> InvokeAsync<T>(Func<DataContext, CancellationToken, Task<T>> body)
    {
        var prior = ambient.Value;
        ambient.Value = this;
        try { return await body(this, cancellation).ConfigureAwait(false); }
        finally
        {
            Interlocked.Exchange(ref closed, 1);
            ambient.Value = prior;
        }
    }

    internal static SdkSourceBinding ResolveBinding(IEnumerable<SdkSourceBinding> bindings, string? name, string? defaultSource,
        bool http)
    {
        name ??= defaultSource;
        SdkSourceBinding? selected = null;
        foreach (var binding in bindings)
        {
            if (http ? binding.Definition.Kind != DataSourceKind.Http : !binding.Definition.IsDatabase) continue;
            if (name is not null && !string.Equals(binding.Definition.Name, name, StringComparison.Ordinal)) continue;
            if (name is null && !binding.Enabled) continue;
            if (selected is not null) throw DataSourceRegistry.SourceError(StatusCode.Invalid, "Choose a named database");
            selected = binding;
        }
        if (selected is null) throw DataSourceRegistry.SourceError(StatusCode.Invalid, "No matching data source was registered");
        if (!selected.Enabled) throw DataSourceRegistry.SourceError(StatusCode.Auth, "The data source is disabled");
        return selected;
    }
}
