using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FluentAcceptance;

internal static class LatencyArrivalClock
{
    private const int LinuxMonotonicClock = 1;
    private const int LinuxAbsoluteTimer = 1;
    private const int Interrupted = 4;

    internal static void WaitUntil(long scheduled, CancellationToken cancellation)
    {
        if (OperatingSystem.IsLinux() && Environment.Is64BitProcess)
        {
            // Stopwatch uses CLOCK_MONOTONIC on Linux. Sleep to the original deadline without using a CPU to spin.
            var seconds = Math.DivRem(scheduled, Stopwatch.Frequency, out long remainder);
            var deadline = new Timespec(seconds, remainder * 1_000_000_000 / Stopwatch.Frequency);
            int status;
            do
            {
                cancellation.ThrowIfCancellationRequested();
                status = ClockSleep(LinuxMonotonicClock, LinuxAbsoluteTimer, in deadline, IntPtr.Zero);
            } while (status == Interrupted);
            if (status != 0) throw new IOException($"The monotonic arrival timer failed with errno {status}");
            return;
        }

        while (Stopwatch.GetTimestamp() < scheduled)
        {
            cancellation.ThrowIfCancellationRequested();
            if (Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), scheduled).TotalMilliseconds > 1.5)
                Thread.Sleep(1);
            else Thread.SpinWait(64);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private readonly struct Timespec(long seconds, long nanoseconds)
    {
        private readonly long seconds = seconds;
        private readonly long nanoseconds = nanoseconds;
    }

    [DllImport("libc", EntryPoint = "clock_nanosleep")]
    private static extern int ClockSleep(int clock, int flags, in Timespec deadline, IntPtr remainder);
}
