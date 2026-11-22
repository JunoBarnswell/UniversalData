using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;

namespace UniversalData.Management;

public sealed record LocalWorkerProcess(int Id, string Birth)
{
    internal static LocalWorkerProcess? Capture(Process process, int reportedId)
    {
        // A wrapper or a PID reported from a sandbox is not the host process
        // identity. Its exit must be proved by the supervisor instead.
        if (process.Id != reportedId || process.StartInfo.FileName == "/usr/bin/systemd-run")
            return null;
        try
        {
            return new(reportedId, BirthOf(process).Birth);
        }
        catch (Exception error) when (error is Win32Exception or InvalidOperationException or IOException or
                                      InvalidDataException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    internal bool HasExited() => IsRunning() == false;

    internal bool? IsRunning()
    {
        try
        {
            using var process = Process.GetProcessById(Id);
            if (process.HasExited) return false;
            var current = BirthOf(process);
            return !current.Exited && current.Birth == Birth;
        }
        catch (Exception error) when (error is ArgumentException or FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
        catch (Exception error) when (error is Win32Exception or InvalidOperationException or IOException or
                                      InvalidDataException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static (string Birth, bool Exited) BirthOf(Process process)
    {
        if (!OperatingSystem.IsLinux())
            return (process.StartTime.ToFileTimeUtc().ToString(CultureInfo.InvariantCulture), false);

        string record = File.ReadAllText($"/proc/{process.Id}/stat");
        int endOfName = record.LastIndexOf(')');
        string[] fields = endOfName < 0 ? [] : record[(endOfName + 1)..].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        string boot;
        try
        {
            boot = File.ReadAllText("/proc/sys/kernel/random/boot_id").Trim();
        }
        catch (IOException error)
        {
            throw new InvalidDataException("Worker host boot identity is unavailable", error);
        }
        if (fields.Length <= 19 || !ulong.TryParse(fields[19], NumberStyles.None, CultureInfo.InvariantCulture, out _) ||
            !Guid.TryParseExact(boot, "D", out _))
            throw new InvalidDataException("Worker host process identity is unavailable");
        return (boot + "/" + fields[19], fields[0] is "Z" or "X");
    }
}
