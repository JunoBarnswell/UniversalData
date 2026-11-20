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
            binding => new Branch(token => OfficialSugar.Enlist(activation, binding.Definition.Name, token)), StringComparer.Ordinal);

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

internal static class SdkAtomicRuntime
{
    private const string TaskName = "sdk.atomic";
    private const string ManifestName = "participants.json";

    private sealed record Participant(string Name, DataSourceKind Kind, ulong Revision, string Resource, string Digest);
    private sealed record Manifest(string[] Names, Participant[] Participants, string Package);
    private sealed record FrozenParticipant(SdkSourceBinding Binding, Participant Identity, string Credential);

    internal static async Task<TResult> RunAsync<TResult>(DataContext parent, SdkSourceBinding[] participants,
        Func<DataContext, CancellationToken, Task<TResult>> body, CancellationToken cancellation)
    {
        using var lifetime = parent.Link(cancellation);
        var state = parent.State;
        if (participants.Any(binding => binding.Definition.Kind == DataSourceKind.Sqlite))
            throw DataSourceRegistry.SourceError(StatusCode.Unsupported, "SQLite cannot prepare a distributed transaction");
        foreach (var binding in participants)
            state.Require(binding, ConnectionAccess.Open, ConnectionAccess.Read, ConnectionAccess.Write);

        var reference = state.Reference with { TransactionId = state.NextTransaction() };
        var reservations = new NativeReservations();
        try
        {
            foreach (var binding in participants.OrderBy(binding => binding.Definition.Name, StringComparer.Ordinal))
            {
                if (state.Environment.ReserveConnection is { } reserve)
                    reservations.Add(await reserve(binding.Definition.Name, lifetime.Token).ConfigureAwait(false));
            }
            foreach (var binding in participants)
                state.Require(binding, ConnectionAccess.Open, ConnectionAccess.Read, ConnectionAccess.Write);
            string directory = JournalDirectory(state.Environment, reference);
            Directory.CreateDirectory(directory);
            bool provision = participants.All(binding => binding.Definition.ProvisionTransactionReceipts);
            if (provision)
                foreach (var binding in participants) state.Require(binding, ConnectionAccess.Receipt);
            var package = Package(participants, provision);
            var frozen = participants.Select(binding => Freeze(state, binding)).ToArray();
            var manifest = new Manifest(participants.Select(binding => binding.Definition.Name).ToArray(),
                frozen.Select(binding => binding.Identity).ToArray(), package);
            using (var file = new FileStream(Path.Combine(directory, ManifestName), FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(file, manifest, WorkerProtocol.Json);
                file.Flush(flushToDisk: true);
            }

            using var callback = new ManagedAtomic<TResult>(parent, participants, body, lifetime.Token);
            using var registry = new Registry();
            Register(registry, () => callback);
            try
            {
                using var execution = OpenExecution(state, frozen, package, directory, reference, registry, false);
                using var signal = lifetime.Token.Register(execution.Cancel);
                lifetime.Token.ThrowIfCancellationRequested();
                execution.Start();
                while (!execution.Wait(100)) await Task.Yield();
                return callback.Value!;
            }
            catch (UniversalDataException fault)
            {
                if (fault.Code == StatusCode.ManagedComponent && callback.Fault is { } original)
                    original.Throw();
                if (fault.Code == StatusCode.Cancelled && lifetime.IsCancellationRequested)
                    throw new OperationCanceledException(lifetime.Token);
                if (callback.Fault?.SourceException is UniversalDataException originalStatus &&
                    originalStatus.Code == fault.Code)
                    throw new DataExecutionException(fault.Status, originalStatus.Message, reference, originalStatus.Detail);
                throw new DataExecutionException(fault.Status, fault.Message, reference, fault.Detail);
            }
        }
        finally { await reservations.DisposeAsync().ConfigureAwait(false); }
    }

    internal static async Task<DataRecoveryResult> RecoverAsync(SdkRecoveryInvocation invocation,
        SdkExecutionEnvironment environment, CancellationToken cancellation)
    {
        if (!Guid.TryParseExact(invocation.Reference.RunId, "N", out _))
            throw new ArgumentException("Invalid SDK recovery reference");
        var root = Path.Combine(environment.JournalRoot, invocation.Reference.RunId);
        var receipts = new List<DataTransactionRecovery>();
        if (Directory.Exists(root))
        {
            foreach (var directory in Directory.EnumerateDirectories(root).Order(StringComparer.Ordinal))
            {
                cancellation.ThrowIfCancellationRequested();
                string id = Path.GetFileName(directory);
                if (!ValidTransactionId(id) || invocation.Reference.TransactionId is { } wanted && wanted != id) continue;
                var reference = invocation.Reference with { TransactionId = id };
                var state = new SdkInvocationState(reference, environment, invocation.Sources, null);
                var outcome = await ResolveAsync(state, directory, cancellation).ConfigureAwait(false);
                receipts.Add(new(id, outcome));
            }
        }
        var summary = receipts.Count == 0 ? DataRecoveryState.NoDecision : receipts[0].State;
        if (receipts.Any(row => row.State != summary)) summary = DataRecoveryState.RecoveryRequired;
        return new(invocation.Reference, summary, receipts);
    }

    private static async Task<DataRecoveryState> ResolveAsync(SdkInvocationState state, string directory, CancellationToken cancellation)
    {
        try
        {
            using var input = File.OpenRead(Path.Combine(directory, ManifestName));
            var manifest = JsonSerializer.Deserialize<Manifest>(input, WorkerProtocol.Json)
                ?? throw new InvalidDataException("The recovery participant manifest is missing");
            var bindings = manifest.Names.Select(name => DataContext.ResolveBinding(state.Sources, name, null, false)).ToArray();
            foreach (var binding in bindings)
                state.Require(binding, ConnectionAccess.Open, ConnectionAccess.Read, ConnectionAccess.Write);
            var frozen = bindings.Select(binding => Freeze(state, binding)).ToArray();
            if (!frozen.Select(participant => participant.Identity).SequenceEqual(manifest.Participants))
                return DataRecoveryState.RecoveryRequired;
            await using var reservations = new NativeReservations();
            if (state.Environment.ReserveConnection is { } reserve)
                foreach (var binding in bindings.OrderBy(binding => binding.Definition.Name, StringComparer.Ordinal))
                    reservations.Add(await reserve(binding.Definition.Name, cancellation).ConfigureAwait(false));
            using var registry = new Registry();
            Register(registry, () => throw new InvalidOperationException("Recovery must never activate business code"));
            if (!Directory.Exists(Path.Combine(directory, "native"))) return DataRecoveryState.NoDecision;
            using var execution = OpenExecution(state, frozen, manifest.Package, directory, state.Reference, registry, true);
            using var signal = cancellation.Register(execution.Cancel);
            return execution.RecoverTransaction() switch
            {
                0 => DataRecoveryState.NoDecision,
                1 => DataRecoveryState.Committed,
                2 => DataRecoveryState.RolledBack,
                _ => DataRecoveryState.RecoveryRequired
            };
        }
        catch (Exception error) when (error is UniversalDataException or IOException or JsonException or UnauthorizedAccessException)
        {
            cancellation.ThrowIfCancellationRequested();
            return DataRecoveryState.RecoveryRequired;
        }
    }

    private static Execution OpenExecution(SdkInvocationState state, FrozenParticipant[] participants,
        string package, string directory, DataRunReference reference, Registry registry, bool resume)
    {
        string journal = Path.Combine(directory, "native");
        bool Authorize(string tenant, string resource, ConnectionAccess access) => tenant == state.Environment.Tenant &&
            (access is ConnectionAccess.CheckpointOpen or ConnectionAccess.CheckpointRead or ConnectionAccess.CheckpointWrite
                ? string.Equals(resource, journal, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
                : state.Environment.Authorize(resource, access));
        string ResolveCredential(string _, string resource, string credential)
        {
            var binding = participants.SingleOrDefault(p => p.Binding.Definition.Resource == resource &&
                SdkNativeSources.CredentialReference(p.Binding) == credential)
                ?? throw DataSourceRegistry.SourceError(StatusCode.Auth, "The transaction credential is outside the frozen participant binding");
            return binding.Credential;
        }
        if (!resume) Directory.CreateDirectory(journal);
        return new(package, state.Environment.Tenant, Authorize, executionId: $"{reference.RunId}/{reference.TransactionId}",
            resolve: ResolveCredential, stateDirectory: journal, resume: resume, maxConcurrency: 1,
            registry: registry);
    }

    private static string Package(SdkSourceBinding[] participants, bool provision) => JsonSerializer.Serialize(new
    {
        version = ProductInfo.Protocol, id = "sdk-atomic",
        connections = participants.Select(binding => new
        {
            id = binding.Definition.Name, driver = SdkNativeSources.PluginPath(binding.Definition.Kind),
            resource = binding.Definition.Resource, credentialReference = SdkNativeSources.CredentialReference(binding)
        }),
        controlFlow = new { tasks = new[] { new
        {
            id = "atomic", kind = TaskName, config = new { }, transaction = new
            { mode = "required", isolation = "readCommitted", provisionReceipts = provision,
                participants = participants.Select(p => p.Definition.Name) }
        } }, constraints = Array.Empty<object>() },
        dataFlows = Array.Empty<object>()
    }, WorkerProtocol.Json);

    private static void Register(Registry registry, Func<IManagedTask> factory) => registry.RegisterTask(
        new(TaskName, "SDK Atomic", "{\"type\":\"object\",\"additionalProperties\":false}", factory, ThreadSafe: true));

    private static FrozenParticipant Freeze(SdkInvocationState state, SdkSourceBinding binding)
    {
        var definition = binding.Definition;
        string credential = NativeSqlConfiguration.Create(Dialect(definition.Kind), state.Connection(binding, true));
        var identity = new Participant(definition.Name, definition.Kind, binding.Revision, definition.Resource,
            Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
            { definition.CredentialReference, credential }, WorkerProtocol.Json))));
        return new(binding, identity, credential);
    }

    internal static int Dialect(DataSourceKind kind) => kind switch
    {
        DataSourceKind.Sqlite => 1, DataSourceKind.PostgreSql => 2, DataSourceKind.MySql => 3,
        DataSourceKind.MariaDb => 4, DataSourceKind.SqlServer => 5, DataSourceKind.Oracle => 6,
        _ => throw new ArgumentException("A Native database is required", nameof(kind))
    };

    private static string JournalDirectory(SdkExecutionEnvironment environment, DataRunReference reference)
    {
        if (!Guid.TryParseExact(reference.RunId, "N", out _) || !ValidTransactionId(reference.TransactionId))
            throw new ArgumentException("Invalid transaction reference");
        return Path.Combine(environment.JournalRoot, reference.RunId, reference.TransactionId!);
    }

    private static bool ValidTransactionId(string? value) => value is { Length: 8 } && value.All(char.IsAsciiDigit);

    private sealed class ManagedAtomic<TResult>(DataContext parent, SdkSourceBinding[] participants,
        Func<DataContext, CancellationToken, Task<TResult>> body, CancellationToken cancellation) : IManagedTask
    {
        private readonly CancellationTokenSource lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        internal TResult? Value { get; private set; }
        internal ExceptionDispatchInfo? Fault { get; private set; }

        public void Execute(ManagedTaskContext activation, string configurationJson)
        {
            try
            {
                using var transaction = new SdkTransactionScope(participants, activation.Activation);
                var context = new DataContext(parent.State, lifetime.Token, transaction);
                Value = DataDatabase.CheckResult(context.InvokeAsync(body).GetAwaiter().GetResult());
                transaction.RequireIdle();
                lifetime.Token.ThrowIfCancellationRequested();
                _ = JsonSerializer.SerializeToElement(Value, WorkerProtocol.Json);
            }
            catch (Exception error)
            {
                Fault = ExceptionDispatchInfo.Capture(error);
                throw;
            }
        }
        public void Cancel() => lifetime.Cancel();
        public void Dispose() => lifetime.Dispose();
    }

    private sealed class NativeReservations : IAsyncDisposable
    {
        private readonly List<IAsyncDisposable> leases = [];
        internal void Add(IAsyncDisposable lease) => leases.Add(lease);
        public async ValueTask DisposeAsync()
        {
            for (int index = leases.Count - 1; index >= 0; index--)
                await leases[index].DisposeAsync().ConfigureAwait(false);
        }
    }
}
