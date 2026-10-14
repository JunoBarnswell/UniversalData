package org.universaldata;
/** Indexed Native Catalog; recovery facts remain in the Native Journal. */
public final class Catalog implements AutoCloseable {
    static { ProductVersion.requireNative(); }
    public enum Kind { PROJECT, PACKAGE, ENVIRONMENT, DEPLOYMENT, EXECUTION, EVENT, AUDIT }
    public record Lease(Kind kind,String id,String owner,long generation,long untilMilliseconds) { }
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
    public synchronized Lease tryAcquireLease(Kind kind,String id,String owner,int durationMilliseconds,String pool,int capacity) {
        open();long[] value=nativeLease(handle,1,kind.ordinal()+1,Utf8.bytes(id),Utf8.bytes(owner),0,durationMilliseconds,Utf8.bytes(pool),capacity);
        return value==null ? null : new Lease(kind,id,owner,value[0],value[1]);
    }
    public synchronized Lease renewLease(Lease lease,int durationMilliseconds) {
        open();long[] value=nativeLease(handle,2,lease.kind().ordinal()+1,Utf8.bytes(lease.id()),Utf8.bytes(lease.owner()),lease.generation(),durationMilliseconds,Utf8.bytes(""),0);
        return new Lease(lease.kind(),lease.id(),lease.owner(),lease.generation(),value[1]);
    }
    public synchronized void releaseLease(Lease lease) {
        open();nativeLease(handle,3,lease.kind().ordinal()+1,Utf8.bytes(lease.id()),Utf8.bytes(lease.owner()),lease.generation(),0,Utf8.bytes(""),0);
    }
    public synchronized boolean leaseActive(Kind kind,String id) {
        open();return nativeLease(handle,4,kind.ordinal()+1,Utf8.bytes(id),Utf8.bytes(""),0,0,Utf8.bytes(""),0)[0]!=0;
    }
    public synchronized void compareExchange(Kind kind,String id,long expectedRevision,String document,Lease lease) {
        open();if(expectedRevision==-1L || lease!=null && (lease.kind()!=kind || !lease.id().equals(id)))throw new IllegalArgumentException("Invalid Catalog lease/revision");
        nativeCompareExchange(handle,kind.ordinal()+1,Utf8.bytes(id),expectedRevision,Utf8.bytes(document),Utf8.bytes(lease==null ? "" : lease.owner()),lease==null ? 0 : lease.generation());
    }
    public synchronized void record(Execution.Event event) {
        open();nativeRecord(handle,Utf8.bytes(event.packageId()),Utf8.bytes(event.executionId()),
            Utf8.bytes(event.taskId()),Utf8.bytes(event.pipelineId()),Utf8.bytes(event.componentId()),
            event.state(),event.result(),event.timestampMs(),event.kind(),event.sequence(),Utf8.bytes(event.errorChain()));
    }
    /** Unsigned revision represented by its exact 64-bit Java bit pattern. */
    public synchronized long latestRevision(Kind kind,String id) { open();return nativeLatest(handle,kind.ordinal()+1,Utf8.bytes(id)); }
    public synchronized long pruneEvents(long beforeMilliseconds,int limit) { open();if(beforeMilliseconds<0)throw new IllegalArgumentException("Invalid retention time");return nativePrune(handle,beforeMilliseconds,limit); }
    public record Binding(String packageJson,String parametersJson) {}
    public synchronized Binding bindPackage(String deployment,long revision,String packageId,String projectParameters,String packageParameters) {
        open();String[] result=nativeBind(handle,Utf8.bytes(deployment),revision,Utf8.bytes(packageId),Utf8.bytes(projectParameters),Utf8.bytes(packageParameters));return new Binding(result[0],result[1]);
    }
    public synchronized String bindingSnapshot(String deployment,long revision,String packageId) {
        open();return nativeBindingSnapshot(handle,Utf8.bytes(deployment),revision,Utf8.bytes(packageId));
    }
    public synchronized void protectLegacyParameters(String id,long revision,String reference) {
        open();nativeProtectParameters(handle,Utf8.bytes(id),revision,Utf8.bytes(reference));
    }
    public synchronized void secureCompact() { open();nativeSecureCompact(handle); }
    private static native String nativeBindingSnapshot(long handle,byte[] deployment,long revision,byte[] packageId);
    private static native void nativeProtectParameters(long handle,byte[] id,long revision,byte[] reference);
    private static native void nativeSecureCompact(long handle);
    private void open() { if(grants.get()!=0)throw new IllegalStateException("Catalog authorization cannot reenter Catalog");if(handle==0)throw new IllegalStateException("Catalog is closed"); }
    @Override public synchronized void close() { if(grants.get()!=0)throw new IllegalStateException("Catalog authorization cannot close Catalog");if(handle!=0) { nativeClose(handle);handle=0; } }
    private static native long nativeOpen(byte[] directory,byte[] tenant,Execution.Authorizer authorizer,long memoryLimit);
    private static native void nativePut(long handle,int kind,byte[] id,long revision,byte[] document);
    private static native long[] nativeLease(long handle,int operation,int kind,byte[] id,byte[] owner,long generation,int duration,byte[] pool,int capacity);
    private static native void nativeCompareExchange(long handle,int kind,byte[] id,long expected,byte[] document,byte[] owner,long generation);
    private static native String nativeGet(long handle,int kind,byte[] id,long revision);
    private static native String nativeQuery(long handle,byte[] query);
    private static native void nativeRecord(long handle,byte[] packageId,byte[] executionId,byte[] taskId,byte[] pipelineId,
        byte[] componentId,int state,int result,long timestamp,int kind,long sequence,byte[] errorChain);
    private static native long nativeLatest(long handle,int kind,byte[] id);
    private static native long nativePrune(long handle,long before,int limit);
    private static native String[] nativeBind(long handle,byte[] deployment,long revision,byte[] packageId,byte[] projectParameters,byte[] packageParameters);
    private static native void nativeClose(long handle);
}
