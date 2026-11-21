using SqlSugar;

namespace UniversalData;

internal sealed class NativeSugarProvider : SqlSugarProvider, ISqlSugarClient
{
    private readonly NativeAdoProvider ado;
    private int disposed;

    internal NativeSugarProvider(ConnectionConfig config, SqlSessionHandle handle, CancellationToken cancellationToken,
        bool ownsSession) : base(config)
    {
        ado = new(this, handle, config.DbType, cancellationToken, ownsSession);
        ContextAdo = ado;
    }

    public override IAdo Ado
    {
        get
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
            return ado;
        }
    }

    public override IInsertable<T> Insertable<T>(T[] insertObjs)
    {
        var insertable = base.Insertable(insertObjs);
        if (insertObjs.Length <= 1 || CurrentConnectionConfig.DbType != DbType.Sqlite ||
            insertable is not InsertableProvider<T> provider)
            return insertable;

        // Official SqlSugar's SQLite batch builder appends LAST_INSERT_ROWID()
        // even for ExecuteCommand. Native execute is deliberately single-statement;
        // keep the official provider and metadata initialization, then replace only
        // the dialect builder that formats the batch SQL.
        var builder = new NativeSqliteInsertBuilder(provider.InsertBuilder);
        provider.InsertBuilder = builder;
        provider.SqlBuilder.InsertBuilder = builder;
        return provider;
    }

    public override void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
            ado.Dispose();
    }

    public override ICodeFirst CodeFirst =>
        throw NativeAdoProvider.Unsupported("Schema operations require explicit Native commands.");

    IFastest<T> ISqlSugarClient.Fastest<T>() => new NativeFastest<T>(ado);

    SqlSugarClient ISqlSugarClient.CopyNew() => throw NativeAdoProvider.Unsupported("A Native session cannot be cloned.");

    ITenant ISqlSugarClient.AsTenant() => throw NativeAdoProvider.Unsupported("A Native session cannot switch resources.");

    T ISqlSugarClient.CreateContext<T>(bool isTran) => throw NativeAdoProvider.CoordinatorOwnsTransaction();

    SugarUnitOfWork ISqlSugarClient.CreateContext(bool isTran) => throw NativeAdoProvider.CoordinatorOwnsTransaction();

    ConnectionConfig ISqlSugarClient.CurrentConnectionConfig
    {
        get => new() { DbType = CurrentConnectionConfig.DbType, ConnectionString = "", InitKeyType = InitKeyType.Attribute };
        set => throw NativeAdoProvider.Unsupported("A Native session cannot replace its connection configuration.");
    }
}
