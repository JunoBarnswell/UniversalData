package org.universaldata;
import java.util.function.LongFunction;
/** Native Registry owns registrations and leases them for Execution lifetime. */
public final class Registry implements AutoCloseable {
    static { ProductVersion.requireNative(); }
    public static final long FEATURE_SCOPED_JOURNAL=1L,FEATURE_CONTROL_EVENTS=0x0004000000000000L;
    public static long features() { return nativeFeatures(); }
    private static native long nativeFeatures();
    private long handle;
    public Registry() { this(268435456L); }
    public Registry(long memoryLimit) { handle=nativeOpen(memoryLimit); }
    synchronized <T> T lease(LongFunction<T> operation) { open();return operation.apply(handle); }
    public synchronized String describe() { open();return nativeDescribe(handle); }
    public interface Task extends AutoCloseable {
        void invoke(TaskContext context,String configuration) throws Exception;
        default void finish() throws Exception {}
        default void cancel() {}
        @Override default void close() throws Exception {}
    }
    @FunctionalInterface public interface TaskFactory { Task prepare(TaskContext context,String configuration) throws Exception; }
    public record TaskDefinition(String id,String name,String version,String configurationSchema,
                                 boolean replaySafe,boolean threadSafe,int effects,TaskFactory factory) {}
    public static final class TaskContext {
        private long activation;
        private final String taskId,pipelineId,componentId;
        TaskContext(long activation,String taskId,String pipelineId,String componentId) {
            this.activation=activation;this.taskId=taskId;this.pipelineId=pipelineId;this.componentId=componentId;
        }
        void invalidate() { activation=0; }
        private void open() { if(activation==0)throw new IllegalStateException("Native callback has ended"); }
        public String taskId() { open();return taskId; }
        public String pipelineId() { open();return pipelineId; }
        public String componentId() { open();return componentId; }
        public void check() { open();nativeCheck(activation); }
    }
    public synchronized void registerTask(TaskDefinition definition) {
        open();java.util.Objects.requireNonNull(definition.factory());
        nativeRegister(handle,Utf8.bytes(definition.id()),Utf8.bytes(definition.name()),
            Utf8.bytes(definition.version()==null?ProductVersion.SDK:definition.version()),Utf8.bytes(definition.configurationSchema()),
            definition.replaySafe(),definition.threadSafe(),definition.effects(),definition.factory());
    }
    private static Task prepare(TaskFactory factory,long activation,String taskId,String pipelineId,String componentId,String configuration) throws Exception {
        TaskContext context=new TaskContext(activation,taskId,pipelineId,componentId);Execution.enterCallback();
        try { return java.util.Objects.requireNonNull(factory.prepare(context,configuration)); }
        finally { context.invalidate();Execution.leaveCallback(); }
    }
    private static void invoke(Task task,long activation,String taskId,String pipelineId,String componentId,String configuration) throws Exception {
        TaskContext context=new TaskContext(activation,taskId,pipelineId,componentId);Execution.enterCallback();
        try { task.invoke(context,configuration); }finally { context.invalidate();Execution.leaveCallback(); }
    }
    private static void lifecycle(Task task,int action) throws Exception {
        Execution.enterCallback();try { if(action==0)task.finish();else if(action==1)task.cancel();else task.close(); }
        finally { Execution.leaveCallback(); }
    }
    private void open() { if(handle==0)throw new IllegalStateException("Registry is closed"); }
    @Override public synchronized void close() { if(handle!=0) { nativeClose(handle);handle=0; } }
    private static native long nativeOpen(long memoryLimit);
    private static native String nativeDescribe(long handle);
    private static native void nativeRegister(long handle,byte[] id,byte[] name,byte[] version,byte[] schema,boolean replaySafe,boolean threadSafe,int effects,TaskFactory factory);
    private static native void nativeClose(long handle);
    private static native void nativeCheck(long activation);
}
