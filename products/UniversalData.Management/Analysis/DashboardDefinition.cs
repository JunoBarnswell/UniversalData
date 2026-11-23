using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed record DashboardDefinition(string Name, AnalysisDashboardCard[] Cards, AnalysisRelease Release,
    AnalysisSqlSource? Source);
