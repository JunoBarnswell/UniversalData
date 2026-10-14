package org.universaldata;
public enum StatusCode {
    OK(0), END(1), INVALID(2), IO(3), RESOURCE(4), SCHEMA(5), PARSE(6), OVERFLOW(7),
    CANCELLED(8), CORRUPT(9), INCOMPATIBLE(10), BUSY(11), UNSUPPORTED(12), AUTH(13),
    QUALITY(14), TRANSIENT(15), COMMIT_UNKNOWN(16), FENCED(17), TIMEOUT(18), MORE(19), TRUNCATED(20), UNKNOWN(-1);
    private final int number;
    StatusCode(int number) { this.number = number; }
    public int number() { return number; }
    public static StatusCode fromNumber(int value) {
        for (var code : values()) if (code.number == value) return code;
        return UNKNOWN;
    }
}
