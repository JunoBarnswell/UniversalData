namespace UniversalData;
public enum StatusCode
{
    Ok = 0, End = 1, Invalid = 2, Io = 3, Resource = 4, Schema = 5,
    Parse = 6, Overflow = 7, Cancelled = 8, Corrupt = 9, Incompatible = 10,
    Busy = 11, Unsupported = 12, Auth = 13, Quality = 14, Transient = 15,
    CommitUnknown = 16, Fenced = 17, Timeout = 18
}
