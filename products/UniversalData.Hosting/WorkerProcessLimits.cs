using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace UniversalData.Hosting;

internal static class WorkerProcessLimits
{
    private const ushort AllProcessorGroups = 0xffff;
    private const uint CpuRateFull = 10_000;
    private const uint JobMemory = 0x0200;
    private const uint KillOnJobClose = 0x2000;
    private const uint CpuEnable = 0x0001;
    private const uint CpuHardCap = 0x0004;
    private const int JobExtendedLimitInformation = 9;
    private const int JobCpuRateControlInformation = 15;

    // systemd applies ExecStartPre inside the same cgroup as ExecStart. Verify the kernel
    // values before CLR startup in case a user manager lacks a delegated controller.
    private const string VerifyCgroup = """
        IFS=: read -r hierarchy controllers cgroup < /proc/self/cgroup
        [ "$hierarchy" = 0 ] && [ -z "$controllers" ] && [ -n "$cgroup" ] || exit 1
        dir="/sys/fs/cgroup$cgroup"
        read -r quota period < "$dir/cpu.max"
        read -r memory < "$dir/memory.max"
        read -r swap < "$dir/memory.swap.max"
        read -r tasks < "$dir/pids.max"
        [ "$quota" -gt 0 ] && [ "$period" -gt 0 ] && [ "$quota" -le "$((period * $1))" ] &&
        [ "$memory" -gt 0 ] && [ "$memory" -le "$2" ] && [ "$swap" -eq 0 ] &&
        [ "$tasks" -gt 0 ] && [ "$tasks" -le "$3" ]
        """;

    internal static void Validate(WorkerRequest request, WorkerBudget budget)
    {
        if (!Guid.TryParseExact(request.Ticket, "N", out _))
            throw new ArgumentException("Invalid Worker ticket", nameof(request));
        if (budget.CpuSlots == 0 || budget.CpuSlots != request.Policy.MaxConcurrency ||
            budget.CpuSlots > (uint)Environment.ProcessorCount ||
            budget.NativeMemoryBytes == 0 || budget.NativeMemoryBytes != request.Policy.MemoryLimit ||
            budget.ProcessMemoryBytes <= budget.NativeMemoryBytes || budget.ProcessMemoryBytes > long.MaxValue ||
            (OperatingSystem.IsWindows() && budget.ProcessMemoryBytes > (ulong)nuint.MaxValue) ||
            budget.ConsumerLanes == 0 || budget.ConsumerLanes > budget.CpuSlots)
            throw new ArgumentOutOfRangeException(nameof(request), "Invalid Worker resource budget");
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsWindows())
            throw new PlatformNotSupportedException("Worker resource limits require Linux cgroup v2 or Windows Job Objects");
    }

    internal static void AddLinuxProperties(ProcessStartInfo start, WorkerBudget budget)
    {
        var cpu = budget.CpuSlots.ToString(CultureInfo.InvariantCulture);
        var memory = budget.ProcessMemoryBytes.ToString(CultureInfo.InvariantCulture);
        // Leave room for the CLR, GC, native I/O and cgroup verifier in addition to consumer lanes.
        var tasks = (64 + budget.ConsumerLanes).ToString(CultureInfo.InvariantCulture);
        start.ArgumentList.Add("--property=CPUQuota=" + (budget.CpuSlots * 100UL).ToString(CultureInfo.InvariantCulture) + "%");
        start.ArgumentList.Add("--property=CPUQuotaPeriodSec=100ms");
        start.ArgumentList.Add("--property=MemoryMax=" + memory);
        start.ArgumentList.Add("--property=MemorySwapMax=0");
        start.ArgumentList.Add("--property=TasksMax=" + tasks);
        // ':' instructs systemd to leave shell parameter expansions to /bin/sh.
        start.ArgumentList.Add("--property=ExecStartPre=:/bin/sh -ec '" +
            VerifyCgroup.Replace("\n", "; ", StringComparison.Ordinal).Replace("&&; ", "&& ", StringComparison.Ordinal) +
            "' ud-worker-limits " + cpu + " " + memory + " " + tasks);
    }

    [SupportedOSPlatform("windows")]
    internal static Process StartWindows(ProcessStartInfo start, WorkerBudget? budget)
    {
        var job = CreateJobObjectW(IntPtr.Zero, null);
        Process? process = null;
        try
        {
            if (job.IsInvalid)
                throw new Win32Exception(Marshal.GetLastPInvokeError(), "Cannot create Worker Job Object");
            var memory = new JobExtendedLimits
            {
                BasicLimitInformation = new JobBasicLimits { LimitFlags = KillOnJobClose | (budget is null ? 0 : JobMemory) },
                JobMemoryLimit = checked((nuint)(budget?.ProcessMemoryBytes ?? 0))
            };
            if (!SetInformationJobObject(job, JobExtendedLimitInformation, ref memory, (uint)Marshal.SizeOf<JobExtendedLimits>()))
                throw new Win32Exception(Marshal.GetLastPInvokeError(), "Cannot impose Worker memory limit");

            if (budget is not null)
            {
                var processors = GetActiveProcessorCount(AllProcessorGroups);
                if (processors == 0)
                    throw new Win32Exception(Marshal.GetLastPInvokeError(), "Cannot read CPU capacity for the Worker Job Object");
                var cpu = new JobCpuRate { ControlFlags = CpuEnable | CpuHardCap,
                    Rate = (uint)((ulong)budget.CpuSlots * CpuRateFull / processors) };
                if (cpu.Rate is 0 or > CpuRateFull)
                    throw new InvalidOperationException("Worker CPU budget cannot be represented by the Windows Job Object");
                if (!SetInformationJobObject(job, JobCpuRateControlInformation, ref cpu, (uint)Marshal.SizeOf<JobCpuRate>()))
                    throw new Win32Exception(Marshal.GetLastPInvokeError(), "Cannot impose Worker CPU limit");
            }

            // Worker blocks on the private inherited request. The stdin handle is handed to
            // the caller only after assignment succeeds.
            process = Process.Start(start) ?? throw new InvalidOperationException("Worker failed to start");
            if (!AssignProcessToJobObject(job, process.SafeHandle))
                throw new Win32Exception(Marshal.GetLastPInvokeError(), "Cannot attach Worker to resource limits");
            process.Exited += (_, _) => job.Dispose();
            process.EnableRaisingEvents = true;
            return process;
        }
        catch
        {
            job.Dispose();
            if (process is not null)
            {
                using (process)
                {
                    try { if (!process.HasExited) process.Kill(entireProcessTree: true); }
                    catch (InvalidOperationException) { }
                }
            }
            throw;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobBasicLimits
    {
        internal long PerProcessUserTimeLimit, PerJobUserTimeLimit;
        internal uint LimitFlags;
        internal nuint MinimumWorkingSetSize, MaximumWorkingSetSize;
        internal uint ActiveProcessLimit;
        internal nuint Affinity;
        internal uint PriorityClass, SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobExtendedLimits
    {
        internal JobBasicLimits BasicLimitInformation;
        internal ulong ReadOperations, WriteOperations, OtherOperations;
        internal ulong ReadBytes, WriteBytes, OtherBytes;
        internal nuint ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobCpuRate
    {
        internal uint ControlFlags, Rate;
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern SafeFileHandle CreateJobObjectW(IntPtr attributes, string? name);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetInformationJobObject(SafeFileHandle job, int kind, ref JobExtendedLimits limits, uint size);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetInformationJobObject(SafeFileHandle job, int kind, ref JobCpuRate limits, uint size);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool AssignProcessToJobObject(SafeFileHandle job, SafeProcessHandle process);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint GetActiveProcessorCount(ushort groupNumber);
}
