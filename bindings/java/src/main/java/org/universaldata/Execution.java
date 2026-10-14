package org.universaldata;
import java.util.concurrent.locks.ReentrantReadWriteLock;
/** Thin JNI projection of the only native Package execution lifecycle. */
public final class Execution implements AutoCloseable {
    public static final int ACCESS_CHECKPOINT_OPEN=11,ACCESS_CHECKPOINT_READ=12,ACCESS_CHECKPOINT_WRITE=13;
    static { ProductVersion.requireNative(); }
    private static final ThreadLocal<Integer> CALLBACK=ThreadLocal.withInitial(()->0);
    public enum State { CREATED,VALIDATING,READY,RUNNING,DRAINING,SUCCEEDED,FAILED,CANCELLED,SKIPPED,RECOVERY_REQUIRED }
    @FunctionalInterface public interface Authorizer { boolean allow(String tenant,String resource,int action); }
    @FunctionalInterface public interface CredentialResolver { String resolve(String tenant,String resource,String reference); }
    public record Metrics(State state,StatusCode result,long inputRows,long outputRows,long rejectedRows,long batches,long queuedBatches,long queueHighWater,long memoryUsed,long executedTasks,long durableTasks,long recoveryTasks,long resumedTasks,long spillBytes,long pipelineTrees,long treeHighWater,long workerHighWater,long committedRows,long acknowledgedRows,long errorCount,long errorRows,long committedErrorRows,long acknowledgedErrorRows) {}
    private final ReentrantReadWriteLock lifetime=new ReentrantReadWriteLock();
    private final Object controller=new Object();
    private final Registry registry;
    private long handle;
    public Execution(String json,String tenant,Authorizer authorize,String parameters,String id) { this(json,tenant,authorize,parameters,id,null,2,4096,268435456L,0,null,false,1); }
    public Execution(String json,String tenant,Authorizer authorize,String parameters,String id,CredentialResolver resolver,
                     int capacity,int rows,long limit,long timeout,String stateDirectory,boolean resume,int maxConcurrency) {
        this(json,tenant,authorize,parameters,id,resolver,capacity,rows,limit,timeout,stateDirectory,resume,maxConcurrency,null,null,0);
    }
    public record Event(String packageId,String executionId,String taskId,String pipelineId,String componentId,
                        int state,int result,long timestampMs,int kind,long sequence,String errorChain) {}
    @FunctionalInterface public interface Observer { void observe(Event event); }
    public Execution(String json,String tenant,Authorizer authorize,String parameters,String id,CredentialResolver resolver,
                     int capacity,int rows,long limit,long timeout,String stateDirectory,boolean resume,int maxConcurrency,
                     Registry registry,Observer observer,long eventSequenceStart) {
        this.registry=registry;
        Authorizer guarded=authorize==null?null:(scope,resource,action)-> {
            CALLBACK.set(CALLBACK.get()+1);try { return authorize.allow(scope,resource,action); }finally { leaveCallback(); }
        };
        CredentialResolver resolved=resolver==null?null:(scope,resource,reference)-> {
            CALLBACK.set(CALLBACK.get()+1);try { return resolver.resolve(scope,resource,reference); }finally { leaveCallback(); }
        };
        Observer observed=observer==null?null:event -> { enterCallback();try { observer.observe(event); }finally { leaveCallback(); } };
        java.util.function.LongFunction<Long> create=registryHandle -> nativeOpen(Utf8.bytes(json),Utf8.bytes(tenant),guarded,Utf8.bytes(parameters==null?"{}":parameters),Utf8.bytes(id==null?"":id),resolved,
                          capacity,rows,limit,timeout,Utf8.bytes(stateDirectory==null?"":stateDirectory),resume,maxConcurrency,registryHandle,observed,eventSequenceStart);
        handle=registry==null?create.apply(0L):registry.lease(create);
    }
    static void enterCallback() { CALLBACK.set(CALLBACK.get()+1); }
    static void leaveCallback() { int depth=CALLBACK.get()-1;if(depth==0)CALLBACK.remove();else CALLBACK.set(depth); }
    private static void control() { if(CALLBACK.get()!=0)throw new IllegalStateException("Callbacks cannot start, await or close executions"); }
    public void execute() { control();synchronized(controller) { lifetime.readLock().lock();try { open();nativeExecute(handle,false); }finally { lifetime.readLock().unlock(); } } }
    public void start() { control();synchronized(controller) { lifetime.readLock().lock();try { open();nativeExecute(handle,true); }finally { lifetime.readLock().unlock(); } } }
    public boolean await(int timeout) { control();if(timeout<0)throw new IllegalArgumentException("Negative timeout");synchronized(controller) { lifetime.readLock().lock();try { open();return nativeWait(handle,timeout); }finally { lifetime.readLock().unlock(); } } }
    public void cancel() { lifetime.readLock().lock();try { if(handle!=0)nativeCancel(handle); }finally { lifetime.readLock().unlock(); } }
    public String id() { lifetime.readLock().lock(); try { open(); return nativeId(handle); } finally { lifetime.readLock().unlock(); } }
    public Metrics metrics() {
        lifetime.readLock().lock();try { open();long[] m=nativeMetrics(handle);return new Metrics(State.values()[(int)m[0]],StatusCode.fromNumber((int)m[1]),m[2],m[3],m[4],m[5],m[6],m[7],m[8],m[9],m[10],m[11],m[12],m[13],m[14],m[15],m[16],m[17],m[18],m[19],m[20],m[21],m[22]); }finally { lifetime.readLock().unlock(); }
    }
    public String compileExpression(String schema,String source) {
        control();synchronized(controller) {lifetime.readLock().lock();try{open();return nativeExpression(handle,Utf8.bytes(schema),Utf8.bytes(source));}finally{lifetime.readLock().unlock();}}
    }
    private static native String nativeExpression(long handle,byte[] schema,byte[] source);
    public String discoverSchema(String connection,String table) {
        control(); synchronized(controller) { lifetime.readLock().lock();
            try { open(); return nativeDiscover(handle,Utf8.bytes(connection),Utf8.bytes(table)); }
            finally { lifetime.readLock().unlock(); }
        }
    }
    private static native String nativeDiscover(long handle,byte[] connection,byte[] table);
    public String componentPorts(String pipelineId,String componentId,String kind,String configuration) {
        control();synchronized(controller) { lifetime.readLock().lock();
            try { open();return nativePorts(handle,Utf8.bytes(pipelineId),Utf8.bytes(componentId),Utf8.bytes(kind),Utf8.bytes(configuration)); }
            finally { lifetime.readLock().unlock(); }
        }
    }
    private static native String nativePorts(long handle,byte[] pipelineId,byte[] componentId,byte[] kind,byte[] configuration);
    public String componentCatalog() {
        control(); synchronized(controller) { lifetime.readLock().lock();
            try { open(); return nativeCatalog(handle); }
            finally { lifetime.readLock().unlock(); }
        }
    }
    private static native String nativeCatalog(long handle);
    public String explain() { return inspect(false); }
    public String diagnostics() { return inspect(true); }
    private String inspect(boolean diagnostics) {
        control(); synchronized(controller) { lifetime.readLock().lock();
            try { open(); return nativeInspect(handle,diagnostics); }
            finally { lifetime.readLock().unlock(); }
        }
    }
    private static native String nativeInspect(long handle,boolean diagnostics);
    public void configureDebug(String configuration) { debugCommand(configuration,true); }
    public void debugControl(String command) { debugCommand(command,false); }
    private void debugCommand(String command,boolean configure) {
        control();lifetime.readLock().lock();try { open();nativeDebug(handle,Utf8.bytes(command),configure); }
        finally { lifetime.readLock().unlock(); }
    }
    public String debugSnapshot() {
        control();lifetime.readLock().lock();try { open();return nativeDebugSnapshot(handle); }
        finally { lifetime.readLock().unlock(); }
    }
    private static native void nativeDebug(long handle,byte[] command,boolean configure);
    private static native String nativeDebugSnapshot(long handle);
    private void open() { if(handle==0)throw new IllegalStateException("Execution is closed"); }
    @Override public void close() { control();cancel();lifetime.writeLock().lock();try { if(handle!=0) { long p=handle;handle=0;nativeClose(p); } }finally { lifetime.writeLock().unlock(); } }
    private static native long nativeOpen(byte[] json,byte[] tenant,Authorizer authorize,byte[] parameters,byte[] id,CredentialResolver resolve,int capacity,int rows,long limit,long timeout,byte[] stateDirectory,boolean resume,int maxConcurrency,long registry,Observer observer,long eventSequenceStart);
    private static native void nativeExecute(long handle,boolean async);
    private static native boolean nativeWait(long handle,int timeout);
    private static native void nativeCancel(long handle);
    private static native String nativeId(long handle);
    private static native long[] nativeMetrics(long handle);
    private static native void nativeClose(long handle);
}
