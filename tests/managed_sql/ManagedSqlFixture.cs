using UniversalData;

namespace ManagedSqlAcceptance;

internal sealed class ManagedSqlFixture(Execution execution, SqlSessionHandle session) : IDisposable
{
    internal SqlSessionHandle Session { get; } = session;

    public void Dispose()
    {
        Session.Dispose();
        execution.Dispose();
    }
}
