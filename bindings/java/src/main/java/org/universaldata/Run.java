package org.universaldata;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Independent DAG execution. Native file built-ins require an explicit grant. */
public final class Run implements AutoCloseable {
    static { System.loadLibrary("ud_java"); }
    private static final ThreadLocal<Integer> AUTHORIZING = ThreadLocal.withInitial(() -> 0);
    public enum Access { READ, WRITE }
    @FunctionalInterface public interface Authorizer {
        boolean allow(String tenant, String resource, Access access);
    }
    public record Metrics(int state, StatusCode result, long inputRows, long outputRows,
                          long batches, long queuedBatches, long queueHighWater, long memoryUsed) {}
    private final ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();
    private final Object controller = new Object();
    private long handle;
    public Run(String dagJson, String tenant, Authorizer authorize) {
        this(dagJson, tenant, authorize, null, 2, 4096, 268435456L);
    }
    public Run(String dagJson, String tenant, Authorizer authorize, String parametersJson,
               int edgeCapacity, int maxBatchRows, long memoryLimit) {
        Authorizer guarded = authorize == null ? null : (scope, resource, access) -> {
            AUTHORIZING.set(AUTHORIZING.get() + 1);
            try { return authorize.allow(scope, resource, access); }
            finally { int depth = AUTHORIZING.get() - 1; if (depth == 0) AUTHORIZING.remove(); else AUTHORIZING.set(depth); }
        };
        handle = nativeOpen(Utf8.bytes(dagJson), Utf8.bytes(tenant), guarded,
            parametersJson == null ? null : Utf8.bytes(parametersJson), edgeCapacity, maxBatchRows, memoryLimit);
    }
    public void execute() {
        ensureController();
        synchronized (controller) { lifetime.readLock().lock(); try { ensureOpen(); nativeExecute(handle, false); } finally { lifetime.readLock().unlock(); } }
    }
    public void start() {
        ensureController();
        synchronized (controller) { lifetime.readLock().lock(); try { ensureOpen(); nativeExecute(handle, true); } finally { lifetime.readLock().unlock(); } }
    }
    /** False means timeout; terminal failures throw their native error category. */
    public boolean await(int timeoutMilliseconds) {
        ensureController();
        if (timeoutMilliseconds < 0) throw new IllegalArgumentException("Negative timeout");
        synchronized (controller) { lifetime.readLock().lock(); try { ensureOpen(); return nativeWait(handle, timeoutMilliseconds); } finally { lifetime.readLock().unlock(); } }
    }
    public void cancel() {
        lifetime.readLock().lock(); try { if (handle != 0) nativeCancel(handle); } finally { lifetime.readLock().unlock(); }
    }
    public Metrics metrics() {
        lifetime.readLock().lock();
        try {
            ensureOpen(); long[] m = nativeMetrics(handle);
            return new Metrics((int)m[0], StatusCode.fromNumber((int)m[1]), m[2], m[3], m[4], m[5], m[6], m[7]);
        } finally { lifetime.readLock().unlock(); }
    }
    public int nodeState(int node) {
        if (node < 0) throw new IllegalArgumentException("Negative node index");
        lifetime.readLock().lock(); try { ensureOpen(); return nativeNodeState(handle, node); } finally { lifetime.readLock().unlock(); }
    }
    private void ensureOpen() { if (handle == 0) throw new IllegalStateException("Run is closed"); }
    private static void ensureController() {
        if (AUTHORIZING.get() != 0) throw new IllegalStateException("Authorization callbacks may inspect metrics or cancel, but cannot control or close runs");
    }
    @Override public void close() {
        ensureController();
        cancel(); lifetime.writeLock().lock();
        try { if (handle != 0) { long pointer = handle; handle = 0; nativeClose(pointer); } }
        finally { lifetime.writeLock().unlock(); }
    }
    private static native long nativeOpen(byte[] document, byte[] tenant, Authorizer authorize,
                                         byte[] parameters, int capacity, int maxRows, long limit);
    private static native void nativeExecute(long handle, boolean async);
    private static native boolean nativeWait(long handle, int timeout);
    private static native void nativeCancel(long handle);
    private static native long[] nativeMetrics(long handle);
    private static native int nativeNodeState(long handle, int node);
    private static native void nativeClose(long handle);
}
