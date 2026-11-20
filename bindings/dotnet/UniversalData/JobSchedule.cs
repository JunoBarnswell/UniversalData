using System.Text.Json.Serialization;

namespace UniversalData;

[JsonConverter(typeof(JsonStringEnumConverter<JobFrequency>))]
public enum JobFrequency { Once, Interval, Daily, Weekly, Monthly }

public sealed record JobSchedule(string Id, JobFrequency Frequency, DateTimeOffset StartAt,
    bool Enabled = true, uint? IntervalSeconds = null, string TimeZone = "UTC",
    TimeOnly? LocalTime = null, DayOfWeek[]? Weekdays = null, int? DayOfMonth = null);
