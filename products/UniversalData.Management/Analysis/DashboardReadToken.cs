namespace UniversalData.Management;

internal sealed record DashboardReadToken(string Tenant, string Reader, string BoardId, string DatasetId,
    string Revision, string ReleaseId, string Context);
