package org.universaldata;
/** Indexed Native Catalog; recovery facts remain in the Native Journal. */
public final class Catalog implements AutoCloseable {
    static { ProductVersion.requireNative(); }
    public enum Kind { PROJECT, PACKAGE, ENVIRONMENT, DEPLOYMENT, EXECUTION, EVENT, AUDIT }
    private long handle;
    private final ThreadLocal<Integer> grants=ThreadLocal.withInitial(()->0);
    public Catalog(String directory,String tenant,Execution.Authorizer authorizer) { this(directory,tenant,authorizer,268435456L); }
    public Catalog(String directory,String tenant,Execution.Authorizer authorizer,long memoryLimit) {
        Execution.Authorizer guarded=(scope,resource,action)-> { Execution.enterCallback();grants.set(grants.get()+1);
            try { return authorizer!=null && authorizer.allow(scope,resource,action); }finally { int depth=grants.get()-1;if(depth==0)grants.remove();else grants.set(depth);Execution.leaveCallback(); } };
        handle=nativeOpen(Utf8.bytes(directory),Utf8.bytes(tenant),guarded,memoryLimit);
    }
    public synchronized void put(Kind kind,String id,long revision,String document) { open();nativePut(handle,kind.ordinal()+1,Utf8.bytes(id),revision,Utf8.bytes(document)); }
    public synchronized String get(Kind kind,String id,long revision) { open();return nativeGet(handle,kind.ordinal()+1,Utf8.bytes(id),revision); }
    public synchronized String query(String nativeQuery) { open();return nativeQuery(handle,Utf8.bytes(nativeQuery)); }
    /** Unsigned revision represented by its exact 64-bit Java bit pattern. */
    public synchronized long latestRevision(Kind kind,String id) { open();return nativeLatest(handle,kind.ordinal()+1,Utf8.bytes(id)); }
    public synchronized long pruneEvents(long beforeMilliseconds,int limit) { open();if(beforeMilliseconds<0)throw new IllegalArgumentException("Invalid retention time");return nativePrune(handle,beforeMilliseconds,limit); }
    public record Binding(String packageJson,String parametersJson) {}
    public synchronized Binding bindPackage(String deployment,long revision,String packageId,String projectParameters,String packageParameters) {
        open();String[] result=nativeBind(handle,Utf8.bytes(deployment),revision,Utf8.bytes(packageId),Utf8.bytes(projectParameters),Utf8.bytes(packageParameters));return new Binding(result[0],result[1]);
    }
    private void open() { if(grants.get()!=0)throw new IllegalStateException("Catalog authorization cannot reenter Catalog");if(handle==0)throw new IllegalStateException("Catalog is closed"); }
    @Override public synchronized void close() { if(grants.get()!=0)throw new IllegalStateException("Catalog authorization cannot close Catalog");if(handle!=0) { nativeClose(handle);handle=0; } }
    private static native long nativeOpen(byte[] directory,byte[] tenant,Execution.Authorizer authorizer,long memoryLimit);
    private static native void nativePut(long handle,int kind,byte[] id,long revision,byte[] document);
    private static native String nativeGet(long handle,int kind,byte[] id,long revision);
    private static native String nativeQuery(long handle,byte[] query);
    private static native long nativeLatest(long handle,int kind,byte[] id);
    private static native long nativePrune(long handle,long before,int limit);
    private static native String[] nativeBind(long handle,byte[] deployment,long revision,byte[] packageId,byte[] projectParameters,byte[] packageParameters);
    private static native void nativeClose(long handle);
}
