// Generated from include/ud/data.h; Native owns value semantics.
namespace UniversalData;
public enum DataType
{
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Utf8 = 4,
    Decimal128 = 5,
    Date32 = 6,
    Time64 = 7,
    Timestamp = 8,
    Binary = 9,
    UInt64 = 10,
    Duration = 11,
    Interval = 12,
    Struct = 13,
    List = 14,
    Map = 15
}
public readonly record struct Duration(long Microseconds);
public readonly record struct Interval(int Months, int Days, long Microseconds);
