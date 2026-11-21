using System.Data;
using SqlSugar;

namespace UniversalData;

public sealed class DataDatabase
{
    private static readonly ConnectionAccess[] openAccess = [ConnectionAccess.Open];
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
            using var db = OfficialSugar.Open(await context.State.OpenSqlAsync(binding, ct).ConfigureAwait(false), ct);
            return CheckResult(await body(db, ct).ConfigureAwait(false));
        }, scope.Token, openAccess).ConfigureAwait(false);
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
            using var owner = new LocalSugarTransaction(await context.State.OpenSqlAsync(binding, ct).ConfigureAwait(false), ct);
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
        return await WithLeaseAsync(async ct =>
        {
            using var database = OfficialSugar.Open(await context.State.OpenSqlAsync(binding, ct).ConfigureAwait(false), ct);
            string query = binding.Definition.Kind == DataSourceKind.Oracle ? "SELECT 1 FROM DUAL" : "SELECT 1";
            return await database.Ado.GetIntAsync(query).ConfigureAwait(false) == 1;
        }, scope.Token, readAccess).ConfigureAwait(false);
    }

    private Task<TResult> WithLeaseAsync<TResult>(Func<CancellationToken, Task<TResult>> callback,
        CancellationToken cancellation, ConnectionAccess[] operations)
    {
        cancellation.ThrowIfCancellationRequested();
        context.State.Authorize(binding, operations);
        return callback(cancellation);
    }

    internal static TResult CheckResult<TResult>(TResult value)
    {
        if (value is ISqlSugarClient or IDbConnection or IDataReader or Task or Delegate ||
            value?.GetType().Assembly == typeof(ISqlSugarClient).Assembly)
            throw new InvalidOperationException("A database callback must return detached data");
        return value;
    }
}
