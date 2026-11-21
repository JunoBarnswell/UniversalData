using System.Runtime.InteropServices;

namespace UniversalData;

/// <summary>
/// The Native calendar is the only owner of schedule validation and
/// occurrence calculation.  Management and publication share these exact
/// internal signatures so they cannot drift on timezone or DST behavior.
/// </summary>
internal static class NativeJobScheduler
{
    internal static void Validate(JobSchedule schedule)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        using var strings = new NativeStrings(schedule.Id ?? "", schedule.TimeZone ?? "");
        var value = NativeSchedule(schedule, strings);
        Check(schedule, Native.ud_scheduler_validate(ref value));
    }

    internal static long? Next(JobSchedule schedule, long afterMs)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        using var strings = new NativeStrings(schedule.Id ?? "", schedule.TimeZone ?? "");
        var value = NativeSchedule(schedule, strings);
        int status = Native.ud_scheduler_next(ref value, afterMs, out long next);
        if (status == (int)StatusCode.End)
            return null;
        Check(schedule, status);
        return next;
    }

    internal static (int Index, long Due)? SelectDue(IReadOnlyList<long?> nextDue,
        IReadOnlyList<bool> enabled, long nowMs)
    {
        ArgumentNullException.ThrowIfNull(nextDue);
        ArgumentNullException.ThrowIfNull(enabled);
        if (nextDue.Count is < 1 or > 64 || enabled.Count != nextDue.Count)
            throw new ArgumentException("The schedule cursor count is outside Native limits", nameof(nextDue));
        var due = nextDue.Select(value => value ?? -1L).ToArray();
        var enabledValues = enabled.Select(value => value ? 1 : 0).ToArray();
        var dueHandle = GCHandle.Alloc(due, GCHandleType.Pinned);
        var enabledHandle = GCHandle.Alloc(enabledValues, GCHandleType.Pinned);
        try
        {
            var request = new Native.DueRequest
            {
                Size = (uint)Marshal.SizeOf<Native.DueRequest>(), Abi = ProductVersion.Abi,
                Count = (uint)nextDue.Count, NowMs = nowMs,
                NextDueMs = dueHandle.AddrOfPinnedObject(), Enabled = enabledHandle.AddrOfPinnedObject()
            };
            int status = Native.ud_scheduler_select_due(ref request);
            if (status == (int)StatusCode.End)
                return null;
            if (status != 0)
                throw new UniversalDataException(status,
                    Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native scheduler error");
            return ((int)request.Index, request.DueMs);
        }
        finally
        {
            enabledHandle.Free();
            dueHandle.Free();
        }
    }

    private static void Check(JobSchedule schedule, int status)
    {
        if (status == (int)StatusCode.Invalid || status == (int)StatusCode.Schema ||
            status == (int)StatusCode.Parse)
            throw new ArgumentException("Invalid job schedule", nameof(schedule));
        NativeScheduleStatus.Check(status);
    }

    private static Native.SchedulerSchedule NativeSchedule(JobSchedule schedule, NativeStrings strings)
    {
        if (!Enum.IsDefined(schedule.Frequency))
            throw new ArgumentException("Invalid job schedule frequency", nameof(schedule));
        uint weekdays = 0;
        if (schedule.Weekdays is not null)
            foreach (var day in schedule.Weekdays)
            {
                if (!Enum.IsDefined(day))
                    throw new ArgumentException("Invalid job schedule weekday", nameof(schedule));
                weekdays |= 1u << (int)day;
            }

        return new Native.SchedulerSchedule
        {
            Size = (uint)Marshal.SizeOf<Native.SchedulerSchedule>(),
            Abi = ProductVersion.Abi,
            Id = strings[0],
            Frequency = (int)schedule.Frequency,
            StartMs = schedule.StartAt.ToUnixTimeMilliseconds(),
            Enabled = schedule.Enabled ? 1 : 0,
            IntervalSeconds = schedule.IntervalSeconds ?? 0,
            TimeZone = strings[1],
            HasLocalTime = schedule.LocalTime is not null ? 1 : 0,
            LocalTicks = (ulong)(schedule.LocalTime?.Ticks ?? 0),
            WeekdaysMask = weekdays,
            WeekdaysCount = (uint)(schedule.Weekdays?.Length ?? 0),
            DayOfMonth = schedule.DayOfMonth ?? 0
        };
    }

    private static class NativeScheduleStatus
    {
        internal static void Check(int status)
        {
            if (status == 0)
                return;
            throw new UniversalDataException(status,
                Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native scheduler error");
        }
    }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct SchedulerSchedule
    {
        internal uint Size, Abi;
        internal StringValue Id;
        internal int Frequency;
        internal long StartMs;
        internal int Enabled;
        internal uint IntervalSeconds;
        internal StringValue TimeZone;
        internal int HasLocalTime;
        internal ulong LocalTicks;
        internal uint WeekdaysMask, WeekdaysCount;
        internal int DayOfMonth;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct DueRequest
    {
        internal uint Size, Abi, Count;
        internal long NowMs;
        internal IntPtr NextDueMs, Enabled;
        internal uint Index;
        internal long DueMs;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_scheduler_validate(ref SchedulerSchedule schedule);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_scheduler_next(ref SchedulerSchedule schedule, long afterMs,
        out long nextMs);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_scheduler_select_due(ref DueRequest request);
}
