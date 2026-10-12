package org.universaldata;
/** Immutable native Package definition; no Java planning logic. */
public final class Package implements AutoCloseable {
    static { ProductVersion.requireNative(); }
    private long handle;
    public Package(String json) { handle=nativeOpen(Utf8.bytes(json),268435456L); }
    public synchronized String toJson() { ensureOpen();return nativeJson(handle); }
    public Execution execution(String tenant,Execution.Authorizer grant,String parameters,String id) { return new Execution(toJson(),tenant,grant,parameters,id); }
    private void ensureOpen() { if(handle==0)throw new IllegalStateException("Package is closed"); }
    @Override public synchronized void close() { if(handle!=0) { long pointer=handle;handle=0;nativeClose(pointer); } }
    private static native long nativeOpen(byte[] json,long limit);
    private static native String nativeJson(long handle);
    private static native void nativeClose(long handle);
}
