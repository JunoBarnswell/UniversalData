using System.Collections.Concurrent;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

internal sealed class SdkInvocationState : IDisposable
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> connectionSeats = new(StringComparer.Ordinal);
    private readonly object sqlGate = new();
    private Execution? sql;
    private HashSet<string>? sqlSources;
    private bool disposed;
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
        Authorize(binding, operations);
    }

    internal void Authorize(SdkSourceBinding binding, ConnectionAccess[] operations)
    {
        foreach (var operation in operations)
            if (!Environment.Authorize(binding.Definition.Resource, operation))
                throw DataSourceRegistry.SourceError(StatusCode.Auth, "The source is unavailable in the current tenant scope");
    }

    internal string SourceCredential(SdkSourceBinding binding)
    {
        var definition = binding.Definition;
        return definition.NativeConnectionString ??
            (definition.CredentialReference is { } credential
                ? Environment.Resolve?.Invoke(definition.Resource, credential)
                    ?? throw DataSourceRegistry.SourceError(StatusCode.Auth, "The source credential is unavailable")
                : definition.ConnectionString);
    }

    internal string NativeConnection(SdkSourceBinding binding) =>
        NativeSqlConfiguration.Create(SqlNative.Dialect(binding.Definition.Kind), SourceCredential(binding));

    internal SqlSessionHandle OpenSql(SdkSourceBinding binding, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        Execution execution;
        lock (sqlGate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            execution = sql ??= CreateSqlExecution();
            if (!sqlSources!.Contains(binding.Definition.Name))
                throw new UniversalDataException((int)StatusCode.DriverUnavailable, "The Native database connector is unavailable");
        }
        var session = execution.OpenSqlSession(binding.Definition.Name);
        if (!cancellation.IsCancellationRequested) return session;
        session.Dispose();
        throw new OperationCanceledException(cancellation);
    }

    private Execution CreateSqlExecution()
    {
        var connections = new List<SdkNativeConnection>();
        var sources = new Dictionary<string, SdkSourceBinding>(StringComparer.Ordinal);
        foreach (var binding in Sources)
        {
            if (!binding.Enabled || !binding.Definition.IsDatabase) continue;
            var path = SdkNativeSources.FindPluginPath(binding.Definition.Kind);
            if (path is null) continue;
            var connection = SdkNativeSources.Describe(binding, path);
            connections.Add(connection);
            sources.Add(connection.CredentialReference, binding);
        }
        string Resolve(string _, string resource, string credential)
        {
            if (!sources.TryGetValue(credential, out var binding) || binding.Definition.Resource != resource)
                throw DataSourceRegistry.SourceError(StatusCode.Auth, "The credential is outside the frozen source binding");
            Require(binding, ConnectionAccess.Open);
            return NativeConnection(binding);
        }
        var package = JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id = "sdk-sql", connections,
            controlFlow = new { tasks = Array.Empty<object>(), constraints = Array.Empty<object>() },
            dataFlows = Array.Empty<object>()
        }, WorkerProtocol.Json);
        sqlSources = connections.Select(connection => connection.Id).ToHashSet(StringComparer.Ordinal);
        return new Execution(package, Environment.Tenant, (tenant, resource, access) =>
            tenant == Environment.Tenant && Environment.Authorize(resource, access), executionId: Reference.RunId,
            resolve: Resolve, memoryLimit: Environment.MemoryLimit, pool: Environment.Pool);
    }

    public void Dispose()
    {
        Execution? execution;
        SemaphoreSlim[] seats;
        lock (sqlGate)
        {
            if (disposed) return;
            disposed = true;
            execution = sql;
            seats = connectionSeats.Values.ToArray();
        }
        execution?.Dispose();
        Branches.Dispose();
        foreach (var seat in seats) seat.Dispose();
    }
}
