// Generated from metadata/product.json.
package org.universaldata;
public final class ProductVersion {
    private ProductVersion() {}
    public static final int ABI = 11;
    public static final int PROTOCOL = 11;
    public static final String SDK = "2.2.0";
    static void requireNative() {
        System.loadLibrary("ud_java");
        if (nativeAbi() != ABI) throw new UniversalDataException(10, "Native ABI differs from SDK");
    }
    private static native int nativeAbi();
}
