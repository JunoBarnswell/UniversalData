package org.universaldata;
/** Canonical native schema comparison; JSON policy has one C owner. */
public final class Schema {
    static { ProductVersion.requireNative(); }
    private Schema() {}
    public static String compare(String expected,String actual,String policy) {
        return nativeCompare(Utf8.bytes(expected),Utf8.bytes(actual),Utf8.bytes(policy));
    }
    private static native String nativeCompare(byte[] expected,byte[] actual,byte[] policy);
}
