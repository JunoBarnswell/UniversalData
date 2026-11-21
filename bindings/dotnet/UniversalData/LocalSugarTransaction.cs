using SqlSugar;

namespace UniversalData;

internal sealed class LocalSugarTransaction : IDisposable
{
    private readonly SqlSessionHandle session;

    internal LocalSugarTransaction(SqlSessionHandle session, CancellationToken cancellationToken)
    {
        this.session = session;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var cancel = cancellationToken.UnsafeRegister(static value => SqlNative.Cancel((SqlSessionHandle)value!), session);
            cancellationToken.ThrowIfCancellationRequested();
            Transition(SqlNative.TransactionBegin);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            session.Dispose();
            throw;
        }
    }

    internal ISqlSugarClient Borrow(CancellationToken cancellationToken) =>
        OfficialSugar.Borrow(session, cancellationToken, ownsSession: false);

    internal void Commit() => Transition(SqlNative.TransactionCommit);
    internal void Rollback() => Transition(SqlNative.TransactionRollback);

    private void Transition(int operation)
    {
        Native.ud_context_clear_error(SqlNative.Context(session));
        var status = SqlNative.Transaction(session, operation, 0);
        NativeAdoProvider.Check(session, status);
    }

    public void Dispose() => session.Dispose();
}
