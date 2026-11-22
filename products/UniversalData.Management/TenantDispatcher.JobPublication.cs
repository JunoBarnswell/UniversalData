using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher
{
    private static readonly JsonSerializerOptions DefinitionJson = new(HostPolicy.JsonOptions)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private static string PublicationProject(string packageId) => "deployment-project/" + packageId;

    private static string ProjectDocument(string packageId, ulong revision) => JsonSerializer.Serialize(new
    {
        version = ProductInfo.Protocol,
        id = PublicationProject(packageId),
        packages = new[] { new { id = packageId, revision } }
    }, HostPolicy.JsonOptions);

    public JobEnvironmentReference SaveEnvironment(HostIdentity identity, string id, EnvironmentVersionRequest request)
    {
        if (!HostIdentity.ValidId(id) || !HostIdentity.ValidId(request.PackageId) ||
            request.PackageParameters is { ValueKind: not JsonValueKind.Object } ||
            !ulong.TryParse(request.ExpectedRevision, NumberStyles.None, CultureInfo.InvariantCulture, out var expected))
            throw new ArgumentException("Invalid environment definition");
        identity.Require("environment.write", id);
        identity.Require("deployment.write", request.PackageId);
        var values = request.PackageParameters is { } supplied
            ? new Dictionary<string, JsonElement>(StringComparer.Ordinal) { [request.PackageId] = supplied }
            : null;
        using var catalog = policy.OpenCatalog();
        catalog.CompareExchange(CatalogKind.Environment, id, expected, JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id,
            projectId = PublicationProject(request.PackageId), packageParameters = values
        }, DefinitionJson));
        return new(id, checked(expected + 1).ToString(CultureInfo.InvariantCulture));
    }

    internal (JsonElement Package, string? Parameters) PreparePublication(HostIdentity identity, JobPublicationRequest request)
    {
        if (request.Environment is not { } environment)
            return (request.Document, request.Parameters);
        if (!HostIdentity.ValidId(environment.Id) || !ulong.TryParse(environment.Revision, NumberStyles.None,
            CultureInfo.InvariantCulture, out var environmentRevision) || environmentRevision == 0 ||
            !ulong.TryParse(request.ExpectedPackageRevision, NumberStyles.None, CultureInfo.InvariantCulture,
                out var expectedPackage))
            throw new ArgumentException("Invalid publication environment revision");
        identity.Require("environment.read", environment.Id);
        using var catalog = policy.OpenCatalog();
        string document = catalog.Get(CatalogKind.Environment, environment.Id, environmentRevision) ??
            throw new ArgumentException("Publication environment revision does not exist");
        using var package = new NativePackage(request.Document.GetRawText(), policy.MemoryLimit);
        string packageId = request.Document.GetProperty("id").GetString()!;
        var revision = checked(expectedPackage + 1);
        var bound = package.BindProject(ProjectDocument(packageId, revision), revision, document, request.Parameters ?? "{}");
        return (JsonSerializer.Deserialize<JsonElement>(bound.Package), bound.Parameters);
    }

    private static string RequirePublication(HostIdentity identity, JobPublicationRequest request)
    {
        string packageId = request.Document.GetProperty("id").GetString() ??
            throw new ArgumentException("Package identity is required");
        identity.Require("package.deploy", packageId);
        identity.Require("deployment.write", packageId);
        identity.Require("job.write", packageId);
        identity.Require("package.execute", packageId);
        if (request.Environment is { } environment)
            identity.Require("environment.read", environment.Id);
        if (!HostIdentity.ValidId(packageId) || !HostIdentity.ValidId(request.RequestId))
            throw new ArgumentException("Invalid publication identity");
        return packageId;
    }

    internal JobPublicationReceipt? FindPublication(HostIdentity identity, JobPublicationRequest request)
    {
        string packageId = RequirePublication(identity, request);
        identity.Require("package.read", packageId);
        using var package = new NativePackage(request.Document.GetRawText(), policy.MemoryLimit);
        using var catalog = policy.OpenCatalog();
        string? receipt = catalog.FindPublication(request.RequestId, identity.Subject,
            Fingerprint(package.ToJson(), request, request.Schedules ?? []));
        return receipt is null ? null : ReadReceipt(receipt, packageId);
    }

    public JobPublicationReceipt PublishJob(HostIdentity identity, JobPublicationRequest request,
        string? suppliedParametersReference = null)
    {
        string packageId = RequirePublication(identity, request);
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
        if (request.Environment is null)
            RequireScheduledParameters(packageDocument.RootElement, schedules, request.Parameters);
        string parametersReference = suppliedParametersReference ??
            (request.Parameters is null ? "" : ParametersReference(request.Parameters));

        long now = Now;
        var cursors = schedules.Select(schedule =>
            new ScheduleCursor(schedule, NativeJobScheduler.Next(schedule, now - 1))).ToArray();
        string deploymentId = packageId;
        string projectId = PublicationProject(deploymentId);
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
                project = new { id = projectId, revision = projectRevision },
                environment = request.Environment is { } environment
                    ? new { id = environment.Id, revision = ulong.Parse(environment.Revision, CultureInfo.InvariantCulture) }
                    : null
            };
            var deploymentState = new DeploymentState(ProductInfo.Protocol, deploymentId, packageId,
                packageRevision.ToString(CultureInfo.InvariantCulture),
                deploymentRevision.ToString(CultureInfo.InvariantCulture), true, now, deploymentStateRevision, request.Environment);
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
                    Parallelism: parallelism, RequiresJournal: nativePackage.RequiresJournal, Continuous: nativePackage.IsContinuous);
                var occurrence = new JobOccurrence(ProductInfo.Protocol, occurrenceId, packageId, null, now,
                    "Queued", packageId, packageRevision.ToString(CultureInfo.InvariantCulture), deploymentId,
                    deploymentRevision.ToString(CultureInfo.InvariantCulture), runId, occurrenceRevision);
                occurrenceDocument = JsonSerializer.Serialize(occurrence with { Id = occurrenceKey }, HostPolicy.JsonOptions);
                runDocument = JsonSerializer.Serialize(run with { Id = runKey }, HostPolicy.JsonOptions);
            }

            string fingerprint = Fingerprint(packageText, request, schedules);
            string receiptText = catalog.PublishJob(request.RequestId, identity.Subject, fingerprint,
                packageId, packageText, expectedPackageRevision,
                projectId, projectRevision, ProjectDocument(packageId, packageRevision),
                deploymentId, deploymentRevision, JsonSerializer.Serialize(deployment, DefinitionJson),
                deploymentStateId, deploymentStateRevision,
                JsonSerializer.Serialize(deploymentState with { Id = deploymentStateId }, HostPolicy.JsonOptions),
                jobId, jobRevision, JsonSerializer.Serialize(job with { Id = jobId }, HostPolicy.JsonOptions),
                occurrenceKey, occurrenceRevision, occurrenceDocument, runKey, runRevision, runDocument);
            // The confirmed Catalog receipt is the reply. The dispatcher
            // reloads definitions independently before scheduling.
            return ReadReceipt(receiptText, deploymentId);
        }
    }

    private static JobPublicationReceipt ReadReceipt(string text, string deploymentId)
    {
        using var receipt = JsonDocument.Parse(text);
        var value = receipt.RootElement;
        var deployment = value.GetProperty("deployment").Deserialize<DeploymentState>(HostPolicy.JsonOptions)!
            with { Id = deploymentId };
        var occurrence = value.GetProperty("occurrence").Deserialize<JobOccurrence>(HostPolicy.JsonOptions);
        if (occurrence is not null)
            occurrence = occurrence with { Id = occurrence.Id[(occurrence.Id.LastIndexOf('/') + 1)..] };
        return new(value.GetProperty("id").GetString()!, value.GetProperty("revision").GetString()!,
            JsonSerializer.SerializeToElement(deployment, HostPolicy.JsonOptions), occurrence);
    }

    private static string Fingerprint(string packageText, JobPublicationRequest request,
        JobSchedule[] schedules)
    {
        string parameters = request.Parameters ?? "";
        string material = packageText + "\0" + request.ExpectedPackageRevision + "\0" +
            JsonSerializer.Serialize(schedules, HostPolicy.JsonOptions) + "\0" +
            (request.Run ? "1" : "0") + "\0" + parameters;
        if (request.Environment is { } environment)
            material += "\0" + environment.Id + "\0" + environment.Revision;
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
