namespace UniversalData.Hosting;

public sealed record WorkerSupervisorSettings(int Prewarm = 1, int IdleSeconds = 60)
{
    internal void Validate(uint maxProcesses)
    {
        if (Prewarm < 0 || Prewarm > maxProcesses || IdleSeconds is < 1 or > 86400)
            throw new InvalidDataException("Invalid Worker prewarm settings");
    }
}
