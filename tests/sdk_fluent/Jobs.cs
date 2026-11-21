using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

public sealed record HeldRead(string Gate, string Source);

[SugarTable("sdk_orders")]
public sealed class FixtureRow
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string Tag { get; set; } = "";
}

public sealed class AtomicProbeRow
{
    public string Name { get; set; } = "";
}

/// <summary>
/// Native package declarations used by the fluent acceptance fixture. The
/// old callbacks intentionally do not appear here: every operation is a real
/// SQL/HTTP component in a Native DataJob package.
/// </summary>
public static class Jobs
{
    public const string Pricing = "pricing";
    public const string HoldReplace = "hold-replace";
    public const string HoldStale = "hold-stale";
    public const string HoldSlow = "hold-slow";
    public const string HoldCancel = "hold-cancel";
    public const string HoldTimeoutSource = "hold-timeout";
    public const string HoldSharedCancelSource = "hold-shared-cancel";
    public const string HoldCrashSource = "hold-crash";
    public const string HoldNodeFirstSource = "hold-node-first";
    public const string HoldNodeSecondSource = "hold-node-second";
    public const string FailEndpoint = "fail-endpoint";

    public static readonly DataJob<object> OverlapOrders = Read("overlap-orders", "orders", Pricing);
    public static readonly DataJob<object> OverlapLedger = Read("overlap-ledger", "ledger", Pricing);
    public static readonly DataJobQuery<object, FixtureRow> Orders = Query("orders-query", "orders", Pricing);
    public static readonly DataJobQuery<object, FixtureRow> Ledger = Query("ledger-query", "ledger", Pricing);
    public static readonly DataJobQuery<object, FixtureRow> Alias = Query("alias-query", "alias", Pricing);
    public static readonly DataJob<object> AliasWork = Read("alias-work", "alias", Pricing);
    public static readonly DataJob<object> HoldOrdersReplace = ReadThenHold("hold-orders-replace", "orders", HoldReplace);
    public static readonly DataJob<object> HoldOrdersStale = Read("hold-orders-stale", "orders", HoldStale);
    public static readonly DataJob<object> HoldOrdersSlow = Read("hold-orders-slow", "orders", HoldSlow);
    public static readonly DataJob<object> HoldLedgerCancel = Read("hold-ledger-cancel", "ledger", HoldCancel);
    public static readonly DataJob<object> HoldTimeout = Read("hold-timeout", "ledger", HoldTimeoutSource);
    public static readonly DataJob<object> HoldSharedCancel = Read("hold-shared-cancel", "orders", HoldSharedCancelSource);
    public static readonly DataJob<object> HoldCrash = Read("hold-crash", "orders", HoldCrashSource);
    public static readonly DataJob<object> HoldNodeFirst = Read("hold-node-first", "orders", HoldNodeFirstSource);
    public static readonly DataJob<object> HoldNodeSecond = Read("hold-node-second", "alias", HoldNodeSecondSource);
    public static readonly DataJob<object> Fail = Signal("fail-job", FailEndpoint);
    public static readonly DataJob<object> Crash = Fail;
    public static readonly DataJob<object> IgnoreCancellation = Signal("timeout-job", HoldTimeoutSource);
    public static readonly DataJob<object> UnsupportedAtomic = Unsupported();
    public static readonly DataJobQuery<object, AtomicProbeRow> AtomicProbe = DataJob.Define("atomic-probe")
        .FromSql<AtomicProbeRow>("orders",
            "SELECT name AS Name FROM sqlite_master WHERE type = 'table' AND name = 'sdk_atomic_admission_probe'")
        .Return();

    /// <summary>Registers HTTP connections whose resources are real fixture URLs.</summary>
    public static void AddHttpSources(DataClientBuilder builder, string origin)
    {
        ArgumentNullException.ThrowIfNull(builder);
        foreach ((string name, string path) in new[]
        {
            (Pricing, ""), (HoldReplace, "hold/replace"), (HoldStale, "hold/stale-borrow"),
            (HoldSlow, "hold/slow"), (HoldCancel, "hold/cancel"), (HoldTimeoutSource, "hold/timeout"),
            (HoldSharedCancelSource, "hold/shared-cancel"), (HoldNodeFirstSource, "hold/node-first"),
            (HoldNodeSecondSource, "hold/node-second"), (HoldCrashSource, "hold/crash"),
            (FailEndpoint, "fail")
        })
        {
            var baseUri = new Uri(origin, UriKind.Absolute);
            var resource = new Uri(baseUri, path).AbsoluteUri;
            builder.AddSource(new DataSourceDefinition(name, DataSourceKind.Http, "",
                resource: resource, baseUri: baseUri));
        }
    }

    public static async Task<string> FirstTag(DataClient client,
        DataJobQuery<object, FixtureRow> query, CancellationToken cancellation)
    {
        await foreach (var row in client.RunAsync(query, cancellation).WithCancellation(cancellation))
            return row.Tag;
        throw new InvalidOperationException("The Native query returned no fixture row.");
    }

    public static async Task<FixtureRow> FirstRow(DataClient client,
        DataJobQuery<object, FixtureRow> query, CancellationToken cancellation)
    {
        await foreach (var row in client.RunAsync(query, cancellation).WithCancellation(cancellation))
            return row;
        throw new InvalidOperationException("The Native query returned no fixture row.");
    }

    public static async Task<bool> AtomicProbeExists(DataClient client, CancellationToken cancellation)
    {
        await foreach (var _ in client.RunAsync(AtomicProbe, cancellation).WithCancellation(cancellation))
            return true;
        return false;
    }

    private static DataJob<object> Read(string id, string database, string http)
    {
        var job = DataJob.Define(id);
        job.Request(new HttpRequestDefinition(http, ExpectedStatus: 200));
        job.FromTable<FixtureRow>(database).Discard();
        return job;
    }

    private static DataJob<object> ReadThenHold(string id, string database, string http)
    {
        var job = DataJob.Define(id);
        job.FromTable<FixtureRow>(database).Discard();
        job.Request(new HttpRequestDefinition(http, ExpectedStatus: 200));
        return job;
    }

    private static DataJobQuery<object, FixtureRow> Query(string id, string database, string http)
    {
        var job = DataJob.Define(id);
        job.Request(new HttpRequestDefinition(http, ExpectedStatus: 200));
        return job.FromTable<FixtureRow>(database).Return();
    }

    private static DataJob<object> Signal(string id, string http)
    {
        var job = DataJob.Define(id);
        job.Request(new HttpRequestDefinition(http, ExpectedStatus: 200));
        return job;
    }

    private static DataJob<object> Unsupported()
    {
        var job = DataJob.Define("unsupported-atomic").Transaction("orders", "ledger");
        job.ExecuteSql("orders", "CREATE TABLE sdk_atomic_admission_probe (id INTEGER PRIMARY KEY)");
        job.ExecuteSql("ledger", "SELECT 1");
        return job;
    }
}
