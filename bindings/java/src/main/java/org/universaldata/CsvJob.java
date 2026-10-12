package org.universaldata;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class CsvJob implements AutoCloseable {
    static { System.loadLibrary("ud_java"); }
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();
    private final ReentrantLock execution = new ReentrantLock();
    private long handle;
    public CsvJob(Path input, Path state, String schema, String commands) {
        handle = nativeOpen(bytes(input.toAbsolutePath().normalize().toString()), bytes(state.toAbsolutePath().normalize().toString()), bytes(schema), bytes(commands), 268435456L);
    }
    private static byte[] bytes(String value) { return Utf8.bytes(value); }
    private void ensureOpen() { if (handle == 0) throw new IllegalStateException("Job is closed"); }
    public void poll() { execution.lock(); lifecycle.readLock().lock(); try { ensureOpen(); nativePoll(handle, false); } finally { lifecycle.readLock().unlock(); execution.unlock(); } }
    public void run() { execution.lock(); lifecycle.readLock().lock(); try { ensureOpen(); nativePoll(handle, true); } finally { lifecycle.readLock().unlock(); execution.unlock(); } }
    public void cancel() { lifecycle.readLock().lock(); try { if (handle != 0) nativeCancel(handle); } finally { lifecycle.readLock().unlock(); } }
    public String latestSnapshot() { execution.lock(); lifecycle.readLock().lock(); try { ensureOpen(); return nativeSnapshot(handle); } finally { lifecycle.readLock().unlock(); execution.unlock(); } }
    @Override public void close() { cancel(); lifecycle.writeLock().lock(); try { if (handle != 0) { nativeClose(handle); handle = 0; } } finally { lifecycle.writeLock().unlock(); } }
    private static native long nativeOpen(byte[] input, byte[] state, byte[] schema, byte[] commands, long limit);
    private static native void nativePoll(long handle, boolean watch);
    private static native void nativeCancel(long handle);
    private static native String nativeSnapshot(long handle);
    private static native void nativeClose(long handle);
}
