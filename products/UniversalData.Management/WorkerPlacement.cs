using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record WorkerPlacement(string NodeId, bool Remote, WorkerBudget Budget);
