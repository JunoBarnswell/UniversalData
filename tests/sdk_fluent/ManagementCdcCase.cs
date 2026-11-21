using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using SqlSugar;
using UniversalData;

namespace FluentAcceptance;

[SugarTable("sdk_fluent_cdc_source")]
public sealed class ManagementCdcRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "id")]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = "";

    [SugarColumn(ColumnName = "value")]
    public long Value { get; set; }
}

internal static class ManagementCdcCase
{
    private const string JobId = "sdk-fluent-management-cdc";
    private const string RequestId = "sdk-fluent-management-cdc-request";

    internal static async Task RunAsync(string policy, string managementAddress, string sourceTable,
        string slot, string publication, string positionFile, string target, string subject, string password)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var cancellation = timeout.Token;
        var handler = new HttpClientHandler
        {
            UseProxy = false,
            CookieContainer = new CookieContainer()
        };
        using var http = new HttpClient(handler)
        {
            BaseAddress = new Uri(managementAddress.TrimEnd('/') + "/", UriKind.Absolute),
            Timeout = TimeSpan.FromSeconds(15)
        };
        string csrf = await LoginAsync(http, subject, password, cancellation).ConfigureAwait(false);
        var management = new EtlManagementClient(http, csrf);
        var position = await File.ReadAllTextAsync(positionFile, cancellation).ConfigureAwait(false);

        await using var data = DataClient.CreateBuilder(policy, "fluent")
            .AddSource(new DataSourceDefinition("source", DataSourceKind.PostgreSql, "",
                resource: "pg-resource", credentialReference: "pg"))
            .AddSource(new DataSourceDefinition("target", DataSourceKind.Sqlite, "",
                resource: "sqlite-resource", credentialReference: "sqlite"))
            .ConfigureWorkers(options => options.MaxConcurrentSessions(2).MaxBranchesPerSession(1)
                .MaxConnectionsPerSource(2).ExecutionTimeout(TimeSpan.FromSeconds(90))
                .CancellationGrace(TimeSpan.FromSeconds(5)))
            .Build();

        var pipeline = data.Etl(JobId)
            .FromChanges<ManagementCdcRow>("source", sourceTable,
                new EtlChangeOptions(Position: position, Slot: slot, Publication: publication,
                    MicroBatchRows: 128, MicroBatchMilliseconds: 100, MaxTransactions: 1))
            .ToTable("target", "output")
            .UpsertBy(row => row.Id);
        var occurrence = await pipeline.SubmitAsync(management, RequestId, cancellation)
            .ConfigureAwait(false);
        if (occurrence.Id.Length == 0 || occurrence.RunId is null || occurrence.State != "Queued")
            throw new InvalidOperationException("Management did not queue the typed CDC occurrence");

        // A retry with the same request identity must address the existing occurrence,
        // even though the client is still alive and the package is already published.
        var repeated = await management.RunJobAsync(JobId, RequestId, cancellation: cancellation)
            .ConfigureAwait(false);
        if (!repeated.IsSuccess || repeated.Value is null ||
            repeated.Value.Id != occurrence.Id || repeated.Value.RunId != occurrence.RunId)
            throw new InvalidOperationException("Management did not make the stable request ID idempotent");

        Console.WriteLine(JsonSerializer.Serialize(new
        {
            Job = JobId,
            Request = RequestId,
            Occurrence = occurrence.Id,
            Run = occurrence.RunId,
            Target = target,
            WorkerClientProcess = Environment.ProcessId
        }));
    }

    private static async Task<string> LoginAsync(HttpClient http, string subject, string password,
        CancellationToken cancellation)
    {
        using var response = await http.PostAsJsonAsync("api/auth/login", new { subject, password },
            cancellation).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellation)
            .ConfigureAwait(false));
        return document.RootElement.GetProperty("csrf").GetString()
            ?? throw new InvalidOperationException("Management login omitted the CSRF token");
    }
}
