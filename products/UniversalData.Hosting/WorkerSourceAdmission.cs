using System.Globalization;

namespace UniversalData.Hosting;

public sealed record WorkerSourceAdmission(string File, uint MaxSessions)
{
    internal static WorkerSourceAdmission Open(string directory, uint maxSessions)
    {
        if (maxSessions is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(maxSessions));
        string limits = Path.Combine(directory, "source-limit");
        HostFiles.NoLinks(limits);
        byte[] value = System.Text.Encoding.ASCII.GetBytes(maxSessions.ToString(CultureInfo.InvariantCulture));
        try { HostFiles.WritePrivate(limits, value); }
        catch (IOException) when (System.IO.File.Exists(limits)) { }
        if (!System.IO.File.ReadAllBytes(limits).AsSpan().SequenceEqual(value))
            throw new InvalidDataException("Worker hosts sharing a node must use the same source connection limit");

        string path = Path.Combine(directory, "source-sessions.lock");
        HostFiles.NoLinks(path);
        var options = new FileStreamOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite,
            Share = FileShare.ReadWrite };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        using var file = new FileStream(path, options);
        return new(path, maxSessions);
    }
}
