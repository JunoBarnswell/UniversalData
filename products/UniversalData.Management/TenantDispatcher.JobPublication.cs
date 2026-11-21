using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher
{
    public JobPublicationReceipt PublishJob(HostIdentity identity, JobPublicationRequest request,
        string? suppliedParametersReference = null)
    {
        string packageId = request.Document.GetProperty("id").GetString() ??
            throw new ArgumentException("Package identity is required");
        identity.Require("package.deploy", packageId);
        identity.Require("deployment.write", packageId);
        identity.Require("job.write", packageId);
        identity.Require("package.execute", packageId);
        if (!HostIdentity.ValidId(packageId) || !HostIdentity.ValidId(request.RequestId))
            throw new ArgumentException("Invalid publication identity");
        if (!ulong.TryParse(request.ExpectedPackageRevision, NumberStyles.None, CultureInfo.InvariantCulture,
                out ulong expectedPackageRevision))
            throw new ArgumentException("Invalid expected package revision");

        var schedules = request.Schedules ?? [];
        if (schedules.Length > 64 || schedules.Select(schedule => schedule.Id)
                .Distinct(StringComparer.Ordinal).Count() != schedules.Length)
            throw new ArgumentException("Publication schedules must be unique and bounded");
        foreach (var schedule in schedules)
            NativeJobScheduler.Validate(schedule);

        string packageText;
        using (var package = new NativePackage(request.Document.GetRawText(), policy.MemoryLimit))
            packageText = package.ToJson();
        using var packageDocument = JsonDocument.Parse(packageText);
        RequireScheduledParameters(packageDocument.RootElement, schedules, request.Parameters);
        string parametersReference = suppliedParametersReference ??
            (request.Parameters is null ? "" : ParametersReference(request.Parameters));

        long now = Now;
        var cursors = schedules.Select(schedule =>
            new ScheduleCursor(schedule, NativeJobScheduler.Next(schedule, now - 1))).ToArray();
        string deploymentId = packageId;
        string projectId = "deployment-project/" + deploymentId;
        string deploymentStateId = "deployment/" + deploymentId;
        string jobId = "job/" + packageId;
        ulong packageRevision = checked(expectedPackageRevision + 1);
        ulong deploymentRevision;
        ulong deploymentStateRevision;
        ulong jobRevision;
        ulong projectRevision;
        using var catalog = policy.OpenCatalog();
        lock (gate)
        {
            RefreshDefinitions();
            deploymentRevision = checked(catalog.LatestRevision(CatalogKind.Deployment, deploymentId) + 1);
            projectRevision = deploymentRevision;
            deploymentStateRevision = checked(catalog.LatestRevision(CatalogKind.Audit, deploymentStateId) + 1);
            jobRevision = checked(catalog.LatestRevision(CatalogKind.Audit, jobId) + 1);

            var deployment = new
            {
                version = ProductInfo.Protocol,
                id = deploymentId,
                project = new { id = projectId, revision = projectRevision }
            };
            var project = new
            {
                version = ProductInfo.Protocol,
                id = projectId,
                packages = new[] { new { id = packageId, revision = packageRevision } }
            };
            var deploymentState = new DeploymentState(ProductInfo.Protocol, deploymentId, packageId,
                packageRevision.ToString(CultureInfo.InvariantCulture),
                deploymentRevision.ToString(CultureInfo.InvariantCulture), true, now, deploymentStateRevision);
            var job = new JobState(ProductInfo.Protocol, packageId, packageId,
                packageRevision.ToString(CultureInfo.InvariantCulture), parametersReference, true, cursors,
                identity.Subject, identity.Stamp, TimestampMs: now, Revision: jobRevision,
                DeploymentId: deploymentId,
                DeploymentRevision: deploymentRevision.ToString(CultureInfo.InvariantCulture));

            string? occurrenceId = null;
            string? occurrenceKey = null;
            ulong occurrenceRevision = 0;
            string? occurrenceDocument = null;
            string? runId = null;
            string? runKey = null;
            ulong runRevision = 0;
            string? runDocument = null;
            if (request.Run)
            {
                occurrenceId = "manual-" + OccurrenceId(packageId, request.RequestId);
                occurrenceKey = OccurrenceKey(packageId, occurrenceId);
                occurrenceRevision = checked(catalog.LatestRevision(CatalogKind.Audit, occurrenceKey) + 1);
                runId = OccurrenceId(packageId, occurrenceId);
                runKey = "run/" + runId;
                runRevision = checked(catalog.LatestRevision(CatalogKind.Audit, runKey) + 1);
                using var nativePackage = new NativePackage(packageText, policy.MemoryLimit);
                var parallelism = WorkerParallelism.Read(packageText, policy.MaxConcurrency);
                var run = new RunState(ProductInfo.Protocol, runId, packageId,
                    packageRevision.ToString(CultureInfo.InvariantCulture), parametersReference, "Queued",
                    identity.Subject, identity.Stamp, ControlReference(), TimestampMs: now,
                    Revision: runRevision, JobId: packageId, DeploymentId: deploymentId,
                    DeploymentRevision: deploymentRevision.ToString(CultureInfo.InvariantCulture),
                    Parallelism: parallelism, RequiresJournal: nativePackage.RequiresJournal);
                var occurrence = new JobOccurrence(ProductInfo.Protocol, occurrenceId, packageId, null, now,
                    "Queued", packageId, packageRevision.ToString(CultureInfo.InvariantCulture), deploymentId,
                    deploymentRevision.ToString(CultureInfo.InvariantCulture), runId, occurrenceRevision);
                occurrenceDocument = JsonSerializer.Serialize(occurrence with { Id = occurrenceKey }, HostPolicy.JsonOptions);
                runDocument = JsonSerializer.Serialize(run with { Id = runKey }, HostPolicy.JsonOptions);
            }

            string fingerprint = Fingerprint(packageText, request, schedules);
            string receiptText = catalog.PublishJob(request.RequestId, identity.Subject, fingerprint,
                packageId, packageText, expectedPackageRevision,
                projectId, projectRevision, JsonSerializer.Serialize(project, HostPolicy.JsonOptions),
                deploymentId, deploymentRevision, JsonSerializer.Serialize(deployment, HostPolicy.JsonOptions),
                deploymentStateId, deploymentStateRevision,
                JsonSerializer.Serialize(deploymentState with { Id = deploymentStateId }, HostPolicy.JsonOptions),
                jobId, jobRevision, JsonSerializer.Serialize(job with { Id = jobId }, HostPolicy.JsonOptions),
                occurrenceKey, occurrenceRevision, occurrenceDocument, runKey, runRevision, runDocument);
            using var receipt = JsonDocument.Parse(receiptText);
            var deploymentJson = receipt.RootElement.GetProperty("deployment").Deserialize<DeploymentState>(HostPolicy.JsonOptions)!
                with { Id = deploymentId };
            JobOccurrence? occurrenceJson = receipt.RootElement.GetProperty("occurrence").ValueKind == JsonValueKind.Null
                ? null
                : receipt.RootElement.GetProperty("occurrence").Deserialize<JobOccurrence>(HostPolicy.JsonOptions)!
                    with { Id = occurrenceId! };
            var result = new JobPublicationReceipt(receipt.RootElement.GetProperty("id").GetString()!,
                receipt.RootElement.GetProperty("revision").GetString()!,
                JsonSerializer.SerializeToElement(deploymentJson, HostPolicy.JsonOptions), occurrenceJson);
            /* A replay returns the original receipt. Refresh persisted definitions
             * before updating in-memory projections so a newly generated timestamp,
             * vault reference or run is never mistaken for the committed one. */
            Refresh();
            return result;
        }
    }

    private static string Fingerprint(string packageText, JobPublicationRequest request,
        JobSchedule[] schedules)
    {
        string parameters = request.Parameters ?? "";
        string material = packageText + "\0" + request.ExpectedPackageRevision + "\0" +
            JsonSerializer.Serialize(schedules, HostPolicy.JsonOptions) + "\0" +
            (request.Run ? "1" : "0") + "\0" + parameters;
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(material))).ToLowerInvariant();
    }

    internal static void RequireScheduledParameters(JsonElement package, JobSchedule[] schedules,
        string? parameters)
    {
        if (!schedules.Any(schedule => schedule.Enabled) || parameters is not null ||
            !package.TryGetProperty("parameters", out var definitions) ||
            definitions.ValueKind != JsonValueKind.Object)
            return;
        string[] missing = definitions.EnumerateObject()
            .Where(property => !property.Value.TryGetProperty("default", out _))
            .Select(property => property.Name).ToArray();
        if (missing.Length != 0)
            throw new ArgumentException("Scheduled publication requires parameter values or defaults: " +
                string.Join(", ", missing));
    }
}
