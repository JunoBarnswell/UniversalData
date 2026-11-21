using System.Diagnostics;

namespace UniversalData.Hosting;

internal sealed record WorkerProcessIdentity(int Pid, long Started)
{
    internal bool IsAlive()
    {
        try { return Read(Pid) == this; }
        catch (System.ComponentModel.Win32Exception) { return true; }
        catch (UnauthorizedAccessException) { return true; }
    }

    internal static WorkerProcessIdentity? Read(int pid)
    {
        try
        {
            if (OperatingSystem.IsLinux())
            {
                string stat = File.ReadAllText($"/proc/{pid}/stat");
                string[] fields = stat[(stat.LastIndexOf(')') + 2)..].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (fields.Length <= 19 || fields[0] is "Z" or "X")
                    return null;
                return long.TryParse(fields[19], out long start) ? new(pid, start) : null;
            }
            using var process = Process.GetProcessById(pid);
            return process.HasExited ? null : new(pid, process.StartTime.ToUniversalTime().Ticks);
        }
        catch (FileNotFoundException) { return null; }
        catch (DirectoryNotFoundException) { return null; }
        catch (ArgumentException) { return null; }
        catch (InvalidOperationException) { return null; }
    }
}
