package org.universaldata;
import java.util.Objects;

/** Owned typed batch; schema and nested JSON use the native SDK contract. */
public final class Batch implements AutoCloseable {
    static { ProductVersion.requireNative(); }
    private long handle;
    public Batch(String schemaJson, String dataJson) { this(schemaJson, dataJson, Rounding.EXACT, 268435456L); }
    public Batch(String schemaJson, String dataJson, Rounding rounding, long memoryLimit) {
        handle = nativeOpen(Utf8.bytes(schemaJson), Utf8.bytes(dataJson), Objects.requireNonNull(rounding).code, memoryLimit);
    }
    private Batch(long pointer) { handle = pointer; }
    public static Batch fromWire(byte[] wire) { return fromWire(wire, 268435456L); }
    public static Batch fromWire(byte[] wire, long memoryLimit) { return new Batch(nativeFromWire(Objects.requireNonNull(wire), memoryLimit)); }
    public synchronized long rows() { ensureOpen(); return nativeCount(handle, false); }
    public synchronized long columns() { ensureOpen(); return nativeCount(handle, true); }
    public synchronized String toJson() { ensureOpen(); return nativeJson(handle, false); }
    public synchronized String schemaJson() { ensureOpen(); return nativeJson(handle, true); }
    public synchronized String rowInfoJson(int row) { ensureOpen(); return nativeRowInfo(handle,row); }
    private static native String nativeRowInfo(long handle,int row);
    public synchronized byte[] toWire() { ensureOpen(); return nativeWire(handle); }
    public synchronized Batch cast(String schemaJson, Rounding rounding, long memoryLimit) {
        ensureOpen(); return new Batch(nativeCast(handle, Utf8.bytes(schemaJson), Objects.requireNonNull(rounding).code, memoryLimit));
    }
    public Batch cast(String schemaJson, Rounding rounding) { return cast(schemaJson, rounding, 268435456L); }
    private void ensureOpen() { if (handle == 0) throw new IllegalStateException("Batch is closed"); }
    @Override public synchronized void close() { if (handle != 0) { long pointer = handle; handle = 0; nativeClose(pointer); } }
    private static native long nativeOpen(byte[] schema, byte[] data, int rounding, long limit);
    private static native long nativeFromWire(byte[] wire, long limit);
    private static native long nativeCount(long handle, boolean columns);
    private static native String nativeJson(long handle, boolean schema);
    private static native byte[] nativeWire(long handle);
    private static native long nativeCast(long handle, byte[] schema, int rounding, long limit);
    private static native void nativeClose(long handle);
}
