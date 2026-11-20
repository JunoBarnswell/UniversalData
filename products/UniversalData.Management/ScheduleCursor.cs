using UniversalData;

namespace UniversalData.Management;

public sealed record ScheduleCursor(JobSchedule Schedule, long? NextDueMs);
