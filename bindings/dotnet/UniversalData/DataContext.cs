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
            if ((binding.Definition.Kind == DataSourceKind.Http) != http) continue;
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

internal sealed class SdkInvocationState
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> connectionSeats = new(StringComparer.Ordinal);
    private int transactionSequence;

    internal SdkInvocationState(DataRunReference reference, SdkExecutionEnvironment environment, SdkSourceBinding[] sources,
        string? defaultSource)
    {
        Reference = reference;
        Environment = environment;
        Sources = sources;
        DefaultSource = defaultSource;
        Branches = new(environment.MaxBranches, environment.MaxBranches);
    }

    internal DataRunReference Reference { get; }
    internal SdkExecutionEnvironment Environment { get; }
    internal SdkSourceBinding[] Sources { get; }
    internal string? DefaultSource { get; }
    internal SemaphoreSlim Branches { get; }
    internal SemaphoreSlim ConnectionSeats(string name) => connectionSeats.GetOrAdd(name,
        _ => new(Environment.MaxConnectionsPerSource, Environment.MaxConnectionsPerSource));
    internal string NextTransaction() => Interlocked.Increment(ref transactionSequence).ToString("D8",
        System.Globalization.CultureInfo.InvariantCulture);

    internal void Require(SdkSourceBinding binding, params ConnectionAccess[] operations)
    {
        if (Environment.CanBorrow?.Invoke(binding.Definition.Name, binding.Revision) == false)
            throw DataSourceRegistry.SourceError(StatusCode.Auth, "The source is unavailable in the current tenant scope");
        foreach (var operation in operations)
            if (!Environment.Authorize(binding.Definition.Resource, operation))
                throw DataSourceRegistry.SourceError(StatusCode.Auth, "The source is unavailable in the current tenant scope");
    }

    internal string Connection(SdkSourceBinding binding, bool native = false)
    {
        var definition = binding.Definition;
        if (native && definition.NativeConnectionString is { } nativeConnection) return nativeConnection;
        if (definition.CredentialReference is { } credential)
            return Environment.Resolve?.Invoke(definition.Resource, credential)
                ?? throw DataSourceRegistry.SourceError(StatusCode.Auth, "The source credential is unavailable");
        return definition.ConnectionString;
    }
}

public sealed class DataDatabase
{
    private static readonly ConnectionAccess[] readAccess = [ConnectionAccess.Open, ConnectionAccess.Read];
    private static readonly ConnectionAccess[] fullAccess = [ConnectionAccess.Open, ConnectionAccess.Read, ConnectionAccess.Write];
    private readonly DataContext context;
    private readonly SdkSourceBinding binding;

    internal DataDatabase(DataContext context, SdkSourceBinding binding) { this.context = context; this.binding = binding; }

    public void Use(Action<ISqlSugarClient> body) => Use(db => { body(db); return true; });
    public TResult Use<TResult>(Func<ISqlSugarClient, TResult> body) =>
        UseAsync((db, _) => Task.FromResult(body(db))).GetAwaiter().GetResult();

    public Task UseAsync(Func<ISqlSugarClient, Task> body, CancellationToken cancellation = default) =>
        UseAsync((db, _) => body(db), cancellation);

    public Task UseAsync(Func<ISqlSugarClient, CancellationToken, Task> body, CancellationToken cancellation = default) =>
        UseAsync(async (db, ct) => { await body(db, ct).ConfigureAwait(false); return true; }, cancellation);

    public Task<TResult> UseAsync<TResult>(Func<ISqlSugarClient, Task<TResult>> body, CancellationToken cancellation = default) =>
        UseAsync((db, _) => body(db), cancellation);

    public async Task<TResult> UseAsync<TResult>(Func<ISqlSugarClient, CancellationToken, Task<TResult>> body,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        using var scope = context.Link(cancellation);
        scope.Token.ThrowIfCancellationRequested();
        if (context.Transaction is { } transaction)
            return CheckResult(await transaction.UseAsync(binding.Definition.Name, body, scope.Token).ConfigureAwait(false));

        return await WithLeaseAsync(async ct =>
        {
            using var db = Open(context.State, binding);
            return CheckResult(await body(db, ct).ConfigureAwait(false));
        }, scope.Token, fullAccess).ConfigureAwait(false);
    }

    public async Task TransactionAsync(Func<DataContext, CancellationToken, Task> body, CancellationToken cancellation = default) =>
        _ = await TransactionAsync(async (ctx, ct) => { await body(ctx, ct).ConfigureAwait(false); return true; }, cancellation)
            .ConfigureAwait(false);

    public async Task<TResult> TransactionAsync<TResult>(Func<DataContext, CancellationToken, Task<TResult>> body,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        if (context.Transaction is not null) throw new InvalidOperationException("The current scope already owns a transaction");
        using var scope = context.Link(cancellation);
        return await WithLeaseAsync(async ct =>
        {
            using var owner = OfficialSugar.OpenLocal(binding.Definition.Kind, context.State.Connection(binding, true), ct);
            using var transaction = new SdkTransactionScope(binding.Definition.Name, owner.Borrow);
            var child = new DataContext(context.State, ct, transaction);
            TResult value;
            try
            {
                value = CheckResult(await child.InvokeAsync(body).ConfigureAwait(false));
                transaction.RequireIdle();
                ct.ThrowIfCancellationRequested();
            }
            catch
            {
                transaction.Dispose();
                owner.Rollback();
                throw;
            }
            try { owner.Commit(); }
            catch (UniversalDataException fault) when (fault.Code is StatusCode.Io or StatusCode.Transient or StatusCode.CommitUnknown or StatusCode.Timeout)
            {
                throw new DataExecutionException((int)StatusCode.CommitUnknown, "The database commit could not be confirmed",
                    context.RunReference, fault.Detail);
            }
            return value;
        }, scope.Token, fullAccess).ConfigureAwait(false);
    }

    internal async Task<bool> ProbeAsync(CancellationToken cancellation)
    {
        using var scope = context.Link(cancellation);
        return await WithLeaseAsync(async _ =>
        {
            using var database = Open(context.State, binding);
            string query = binding.Definition.Kind == DataSourceKind.Oracle ? "SELECT 1 FROM DUAL" : "SELECT 1";
            return await database.Ado.GetIntAsync(query).ConfigureAwait(false) == 1;
        }, scope.Token, readAccess).ConfigureAwait(false);
    }

    private async Task<TResult> WithLeaseAsync<TResult>(Func<CancellationToken, Task<TResult>> callback,
        CancellationToken cancellation, ConnectionAccess[] operations)
    {
        var state = context.State;
        var seats = state.ConnectionSeats(binding.Definition.Name);
        await seats.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            state.Require(binding, operations);
            await using var hostSeat = state.Environment.ReserveConnection is { } reserve
                ? await reserve(binding.Definition.Name, cancellation).ConfigureAwait(false) : null;
            state.Require(binding, operations);
            return await callback(cancellation).ConfigureAwait(false);
        }
        finally { seats.Release(); }
    }

    internal static ISqlSugarClient Open(SdkInvocationState state, SdkSourceBinding binding) =>
        OfficialSugar.Open(OfficialSugar.Dialect(binding.Definition.Kind), state.Connection(binding));

    internal static TResult CheckResult<TResult>(TResult value)
    {
        if (value is ISqlSugarClient or IDbConnection or IDataReader or Task or Delegate ||
            value?.GetType().Assembly == typeof(ISqlSugarClient).Assembly)
            throw new InvalidOperationException("A database callback must return detached data");
        return value;
    }
}

public sealed class DataHttp
{
    private static readonly HttpClient client = new(new SocketsHttpHandler { AllowAutoRedirect = false });
    private readonly DataContext context;
    private readonly SdkSourceBinding binding;

    internal DataHttp(DataContext context, SdkSourceBinding binding) { this.context = context; this.binding = binding; }

    internal async Task<bool> ProbeAsync(CancellationToken cancellation)
    {
        using var scope = context.Link(cancellation);
        context.State.Require(binding, ConnectionAccess.Network);
        using var request = new HttpRequestMessage(HttpMethod.Head, binding.Definition.BaseUri);
        try
        {
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, scope.Token)
                .ConfigureAwait(false);
            return true;
        }
        catch (HttpRequestException) { return false; }
    }

    public async Task<T> GetAsync<T>(string relativePath, CancellationToken cancellation = default)
    {
        using var scope = context.Link(cancellation);
        context.State.Require(binding, ConnectionAccess.Network);
        var configured = binding.Definition.BaseUri!;
        var root = new Uri(configured.AbsoluteUri.TrimEnd('/') + "/", UriKind.Absolute);
        if (!Uri.TryCreate(relativePath, UriKind.Relative, out var relative) || !Uri.TryCreate(root, relative, out var target) ||
            !root.IsBaseOf(target) || target.UserInfo.Length != 0)
            throw new ArgumentException("The HTTP path must remain under the registered base URI", nameof(relativePath));
        using var response = await client.GetAsync(target, HttpCompletionOption.ResponseHeadersRead, scope.Token).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new UniversalDataException((int)StatusCode.HttpStatus, "The HTTP source rejected the request",
                new(ErrorSource.Http, StatusCode.HttpStatus, "Unexpected HTTP status", HttpStatus: (int)response.StatusCode));
        await using var content = await response.Content.ReadAsStreamAsync(scope.Token).ConfigureAwait(false);
        return (await JsonSerializer.DeserializeAsync<T>(content, WorkerProtocol.Json, scope.Token).ConfigureAwait(false))!;
    }
}
