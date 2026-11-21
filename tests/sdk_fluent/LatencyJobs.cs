using UniversalData;

namespace FluentAcceptance;

internal static class LatencyJobs
{
    internal static readonly DataJobQuery<object, FixtureRow> Fast = DataJob.Define("latency-fast")
        .FromSql<FixtureRow>("fast", "SELECT Id, Tag FROM sdk_orders WHERE Id = 1", stream: true).Return();

    internal static DataJob<object> Slow(string stage, string signal)
    {
        var plan = DataJob.Define("latency-" + stage);
        if (stage == "http") return plan.Request(new("pricing", Url: "/hold/" + signal));
        plan.Request(new("pricing", Url: "/signal/" + signal));
        if (stage == "sql") return plan.ExecuteSql("slow", "UPDATE sdk_orders SET Tag = Tag WHERE Id = 1");
        return plan.FromSql<FixtureRow>("slow", """
            WITH RECURSIVE ticks(value) AS
                (SELECT 1 UNION ALL SELECT value + 1 FROM ticks WHERE value < 2000000000)
            SELECT sum(value) AS Id, 'cpu' AS Tag FROM ticks
            """, stream: true).Discard();
    }
}
