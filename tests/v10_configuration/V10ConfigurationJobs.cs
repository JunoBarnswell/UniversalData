using UniversalData;

public static class V10ConfigurationJobs
{
    public static DataJob<V10RunInput> Configured => DataJob.Define<V10RunInput>("v10-configured")
        .FromTable<V10InputRow>("orders", "input", step: "read")
        .Select((row, input) => new V10OutputRow
        {
            Id = row.Id,
            Value = row.Value + input.Delta
        }, step: "normalize")
        .ToTable("orders", "output", step: "write")
        .UpsertBy(row => row.Id)
        .Request(new HttpRequestDefinition("api", ExpectedStatus: 200, Url: "/probe"), "probe");
}
