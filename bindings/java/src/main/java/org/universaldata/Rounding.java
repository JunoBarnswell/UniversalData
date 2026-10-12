package org.universaldata;
public enum Rounding {
    EXACT(0), TOWARD_ZERO(1), HALF_EVEN(2), HALF_AWAY(3), FLOOR(4), CEILING(5);
    final int code;
    Rounding(int code) { this.code = code; }
}
