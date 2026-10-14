// Generated from include/ud/data.h; Native owns value semantics.
package org.universaldata;
public enum DataType {
    BOOL(1),
    INT64(2),
    FLOAT64(3),
    UTF8(4),
    DECIMAL128(5),
    DATE32(6),
    TIME64(7),
    TIMESTAMP(8),
    BINARY(9),
    UINT64(10),
    DURATION(11),
    INTERVAL(12),
    STRUCT(13),
    LIST(14),
    MAP(15);
    public final int nativeCode;
    DataType(int code) { nativeCode = code; }
}
