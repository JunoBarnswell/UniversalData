namespace UniversalData.Management;

public sealed record ProductConfiguration(string WorkerPath,string NodeId,bool Cluster = false);
