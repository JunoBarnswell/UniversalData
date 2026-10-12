package org.universaldata;
/** Immutable DAG contract. Generic execution is under development. */
public final class Dag implements AutoCloseable {
    static { System.loadLibrary("ud_java"); }
    private long handle;
    public Dag(String json) { this(json, 268435456L); }
    public Dag(String json, long memoryLimit) { handle = nativeOpen(Utf8.bytes(json), memoryLimit); }
    private Dag(long pointer) { handle = pointer; }
    public synchronized long nodeCount() { ensureOpen(); return nativeCount(handle, false); }
    public synchronized long edgeCount() { ensureOpen(); return nativeCount(handle, true); }
    public synchronized byte[] fingerprint() { ensureOpen(); return nativeFingerprint(handle); }
    public synchronized String toJson() { ensureOpen(); return nativeJson(handle); }
    public synchronized Dag bind(String valuesJson, long memoryLimit) {
        ensureOpen(); return new Dag(nativeBind(handle, Utf8.bytes(valuesJson), memoryLimit));
    }
    public Dag bind(String valuesJson) { return bind(valuesJson, 268435456L); }
    private void ensureOpen() { if (handle == 0) throw new IllegalStateException("DAG is closed"); }
    @Override public synchronized void close() { if (handle != 0) { long pointer = handle; handle = 0; nativeClose(pointer); } }
    private static native long nativeOpen(byte[] json, long limit);
    private static native long nativeCount(long handle, boolean edges);
    private static native String nativeJson(long handle);
    private static native byte[] nativeFingerprint(long handle);
    private static native long nativeBind(long handle, byte[] values, long limit);
    private static native void nativeClose(long handle);
}
