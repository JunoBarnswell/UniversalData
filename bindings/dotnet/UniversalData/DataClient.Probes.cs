namespace UniversalData;

public sealed partial class DataClient
{
    private async Task<DataSourceProbe> ProbeAsync(string name, CancellationToken cancellation)
    {
        var registered = DataSourceRegistry.Find(Sources.Snapshot(), name);
        var source = DataSourceRegistry.Describe(registered);
        if (!source.Enabled) throw DataSourceRegistry.SourceError(StatusCode.Auth, "The data source is disabled");
        try
        {
            if (registered.Definition.IsDatabase)
            {
                string sql = source.Kind == DataSourceKind.Oracle
                    ? "SELECT 1 AS \"value\" FROM DUAL" : "SELECT 1 AS value";
                var query = DataJob.Define("probe-sql").FromSql<DataSourceProbeRow>(name, sql, stream: true).Return();
                bool connected = false;
                await foreach (var row in RunAsync(query, cancellation).ConfigureAwait(false))
                    connected = row.Value == 1;
                return new(source, connected, connected ? ["SQL"] : []);
            }
            if (source.Kind == DataSourceKind.Http)
            {
                await RunAsync(DataJob.Define("probe-http").Request(new(name, "HEAD")), cancellation).ConfigureAwait(false);
                return new(source, true, ["HTTP"]);
            }
            if (registered.Definition.IsMessageBroker)
            {
                var definition = registered.Definition;
                var plan = new EtlPackageBuilder("probe-broker")
                    .Connection(name, "ud:" + SdkNativeSources.DriverName(source.Kind), definition.Resource,
                        SdkNativeSources.CredentialReference(registered)).Build();
                if (plan.Value is not { } package)
                    throw new UniversalDataException((int)plan.Code, "The broker definition is invalid", plan.Error);
                await using var worker = await OpenWorkerAsync(cancellation).ConfigureAwait(false);
                var report = await DataClient.InspectAsync(worker, "messageConnection", package, [registered], "{}", name,
                    null, cancellation).ConfigureAwait(false);
                bool connected = report.GetProperty("capabilities").GetUInt64() != 0;
                return new(source, connected, connected ? ["MQ"] : []);
            }
            throw DataSourceRegistry.SourceError(StatusCode.Unsupported, "The data source has no connection probe");
        }
        catch (UniversalDataException error) when (error.Code is StatusCode.Io or StatusCode.Transient or StatusCode.HttpStatus)
        {
            return new(source, false, []);
        }
    }
}
