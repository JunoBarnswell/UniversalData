using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher
{
    private static string RunDocument(RunState run) =>
        JsonSerializer.Serialize(run with { Id = "run/" + run.Id }, HostPolicy.JsonOptions);

    private RunState ClaimRun(Catalog catalog, CatalogLease lease, RunState run, out JsonElement? incremental)
    {
        incremental = null;
        if (run.JobId is null)
        {
            Save(run, lease);
            return run;
        }
        string package = Package(run.PackageId, run.PackageRevision);
        string parameters = run.ParametersReference.Length == 0 ? "{}" :
            new ParameterVault(policy).Resolve(run.ParametersReference);
        if (run.DeploymentId is { } deployment)
        {
            (package, parameters) = catalog.BindPackage(deployment, ulong.Parse(run.DeploymentRevision!), run.PackageId,
                packageParameters: parameters);
        }
        using var definition = new NativePackage(package, policy.MemoryLimit);
        run = run with { Continuous = definition.IsContinuous, RequiresJournal = definition.RequiresJournal };
        var candidate = run with { Incremental = true };
        string? binding = catalog.ClaimIncremental(lease, candidate.Revision, RunDocument(candidate), package, parameters);
        if (binding is not null)
        {
            incremental = JsonSerializer.Deserialize<JsonElement>(binding);
            states[candidate.Id] = candidate;
            return candidate;
        }
        if (run.Incremental)
            throw new UniversalDataException((int)StatusCode.Fenced, "The incremental run lost its frozen definition");
        Save(run, lease);
        return run;
    }

    private void RecordTerminal(Catalog catalog, CatalogLease lease, RunState run,
        JsonElement? confirmation = null, bool beforeStart = false)
    {
        if (run.Incremental && (run.State == "Succeeded" ||
            beforeStart && !run.Resume && (run.State is "Failed" or "Stopped" or "Cancelled")))
        {
            catalog.SettleIncremental(lease, run.Revision, RunDocument(run), confirmation?.GetRawText());
            states[run.Id] = run;
        }
        else
            Save(run, lease);
    }
}
