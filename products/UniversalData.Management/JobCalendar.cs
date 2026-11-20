using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal static class JobCalendar
{
    public static void Validate(JobSchedule schedule)
    {
        if (!HostIdentity.ValidId(schedule.Id) || schedule.StartAt < DateTimeOffset.UnixEpoch ||
            !Enum.IsDefined(schedule.Frequency))
            throw new ArgumentException("Invalid schedule identity, frequency or start time");
        if (schedule.Frequency == JobFrequency.Interval && schedule.IntervalSeconds is null or 0)
            throw new ArgumentException("An interval schedule requires a positive interval");
        if (schedule.Frequency is JobFrequency.Daily or JobFrequency.Weekly or JobFrequency.Monthly)
        {
            _ = Zone(schedule.TimeZone);
            if (schedule.LocalTime is null)
                throw new ArgumentException("A calendar schedule requires a local time");
        }
        if (schedule.Frequency == JobFrequency.Weekly &&
            (schedule.Weekdays is not { Length: > 0 and <= 7 } ||
             schedule.Weekdays.Any(day => !Enum.IsDefined(day))))
            throw new ArgumentException("A weekly schedule requires valid weekdays");
        if (schedule.Frequency == JobFrequency.Monthly && schedule.DayOfMonth is null or < 1 or > 31)
            throw new ArgumentException("A monthly schedule requires a day from 1 through 31");
    }

    public static long? Next(JobSchedule schedule, long afterMs)
    {
        var start = schedule.StartAt.ToUnixTimeMilliseconds();
        if (schedule.Frequency == JobFrequency.Once)
            return start > afterMs ? start : null;
        if (schedule.Frequency == JobFrequency.Interval)
        {
            if (start > afterMs)
                return start;
            var period = schedule.IntervalSeconds!.Value * 1000L;
            var next = start + ((afterMs - start) / period + 1) * period;
            return next <= DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() ? next : null;
        }

        var zone = Zone(schedule.TimeZone);
        var lowerBound = Math.Max(start, afterMs);
        var date = DateOnly.FromDateTime(TimeZoneInfo.ConvertTime(
            DateTimeOffset.FromUnixTimeMilliseconds(lowerBound), zone).Date);
        while (true)
        {
            var matches = schedule.Frequency switch
            {
                JobFrequency.Weekly => schedule.Weekdays!.Contains(date.DayOfWeek),
                JobFrequency.Monthly => date.Day == schedule.DayOfMonth,
                _ => true
            };
            if (matches)
            {
                var local = date.ToDateTime(schedule.LocalTime!.Value);
                if (!zone.IsInvalidTime(local))
                {
                    // The first UTC instant is the sole occurrence of a repeated local time.
                    var offset = zone.IsAmbiguousTime(local) ? zone.GetAmbiguousTimeOffsets(local).Max()
                        : zone.GetUtcOffset(local);
                    var utcTicks = local.Ticks - offset.Ticks;
                    if (utcTicks >= DateTimeOffset.UnixEpoch.Ticks && utcTicks <= DateTimeOffset.MaxValue.Ticks)
                    {
                        var instant = new DateTimeOffset(utcTicks, TimeSpan.Zero).ToUnixTimeMilliseconds();
                        if (instant >= start && instant > afterMs)
                            return instant;
                    }
                }
            }
            if (date == DateOnly.MaxValue)
                return null;
            date = date.AddDays(1);
        }
    }

    private static TimeZoneInfo Zone(string name)
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById(name); }
        catch (Exception error) when (error is TimeZoneNotFoundException or InvalidTimeZoneException)
        { throw new ArgumentException("Unknown schedule time zone", nameof(name), error); }
    }
}
