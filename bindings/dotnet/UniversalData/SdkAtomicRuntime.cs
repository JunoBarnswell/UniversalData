using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

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

    internal static DataRecoveryResult Recover(SdkRecoveryInvocation invocation,
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
                using var state = new SdkInvocationState(reference, environment, invocation.Sources, null);
                var outcome = Resolve(state, directory, cancellation);
                receipts.Add(new(id, outcome));
            }
        }
        var summary = receipts.Count == 0 ? DataRecoveryState.NoDecision : receipts[0].State;
        if (receipts.Any(row => row.State != summary)) summary = DataRecoveryState.RecoveryRequired;
        return new(invocation.Reference, summary, receipts);
    }

    private static DataRecoveryState Resolve(SdkInvocationState state, string directory, CancellationToken cancellation)
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
            registry: registry, pool: state.Environment.Pool);
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
        string credential = NativeSqlConfiguration.Create(SqlNative.Dialect(definition.Kind), state.NativeConnection(binding));
        var identity = new Participant(definition.Name, definition.Kind, binding.Revision, definition.Resource,
            Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new
            { definition.CredentialReference, credential }, WorkerProtocol.Json))));
        return new(binding, identity, credential);
    }

    // Shared by the Worker ETL resolver; the native SQL binding is the single dialect map.
    internal static int Dialect(DataSourceKind kind) => SqlNative.Dialect(kind);

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
}
