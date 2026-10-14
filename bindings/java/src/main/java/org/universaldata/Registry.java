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
@FunctionalInterface public interface FunctionBind { String bind(String argumentSchema) throws Exception; }
    @FunctionalInterface public interface FunctionEvaluate { CallbackBatch evaluate(FunctionContext context,CallbackBatch arguments) throws Exception; }
    public record FunctionDefinition(String id,String name,String version,int minimumArguments,int maximumArguments,
        boolean pure,boolean deterministic,boolean threadSafe,FunctionBind bind,FunctionEvaluate evaluate) {}
    public static final class FunctionContext extends CallbackScope {
        private final long resultField;
        FunctionContext(long memory,long field) { super(memory);resultField=field; }
        public void checkCancellation() { check();nativeFunctionCheck(memory); }
        public String resultSchema() { check();return nativeFunctionSchema(memory,resultField); }
        public CallbackBatch result(String rowsJson) { checkCancellation();return create(resultSchema(),rowsJson); }
    }
    public synchronized void registerFunction(FunctionDefinition value) {
        open();java.util.Objects.requireNonNull(value.bind());java.util.Objects.requireNonNull(value.evaluate());
        if(!value.pure()||!value.id().contains(".")||value.minimumArguments()<0||value.maximumArguments()<value.minimumArguments()||value.maximumArguments()>16)
            throw new IllegalArgumentException("Extension functions require a namespace, purity and at most 16 arguments");
        nativeRegisterFunction(handle,Utf8.bytes(value.id()),Utf8.bytes(value.name()),Utf8.bytes(value.version()==null?ProductVersion.SDK:value.version()),value.minimumArguments(),value.maximumArguments(),value.deterministic(),value.threadSafe(),value);
    }
    private static byte[] bindFunction(FunctionDefinition value,String schema) throws Exception {
        Execution.enterCallback();try{return Utf8.bytes(java.util.Objects.requireNonNull(value.bind().bind(schema)));}finally{Execution.leaveCallback();}
    }
    private static void evaluateFunction(FunctionDefinition value,long memory,long arguments,long resultField,long output) throws Exception {
        FunctionContext context=new FunctionContext(memory,resultField);Execution.enterCallback();
        try { CallbackBatch input=context.batch(arguments,false);CallbackBatch result=java.util.Objects.requireNonNull(value.evaluate().evaluate(context,input));
            if(result.scope.memory!=memory)throw new IllegalArgumentException("Function result belongs to another Native context");
            nativeFunctionResult(memory,result.pointer(),resultField,output);
        } finally { context.end();Execution.leaveCallback(); }
    }
    private static native void nativeRegisterFunction(long registry,byte[] id,byte[] name,byte[] version,int minimum,int maximum,boolean deterministic,boolean threadSafe,FunctionDefinition value);
    private static native void nativeFunctionCheck(long memory);
    private static native String nativeFunctionSchema(long memory,long field);
    private static native void nativeFunctionResult(long memory,long batch,long field,long result);
    @FunctionalInterface public interface ComponentConfigure { String[] configure(String configuration,String[] inputs) throws Exception; }
    @FunctionalInterface public interface ComponentFactory { DataFlow create() throws Exception; }
    public record DataFlowDefinition(String id,String name,String version,String configurationSchema,int category,int inputs,int outputs,
        int synchronousInput,int errorOutput,long optionalInputs,long optionalOutputs,boolean replaySafe,boolean threadSafe,boolean stateful,
        boolean blocking,int stateVersion,int effects,boolean handlesControlEvents,boolean finalAcknowledgement,boolean epochAcknowledgement,
        ComponentConfigure configure,ComponentFactory factory) {}
    public synchronized void registerComponent(DataFlowDefinition value) {
        open();java.util.Objects.requireNonNull(value.configure());java.util.Objects.requireNonNull(value.factory());
        if(value.category()<2||value.category()>4||value.inputs()<0||value.outputs()<0||value.inputs()>64||value.outputs()>64||value.stateful()&&value.stateVersion()<=0)
            throw new IllegalArgumentException("Invalid Native component definition");
        nativeRegisterComponent(handle,Utf8.bytes(value.id()),Utf8.bytes(value.name()),Utf8.bytes(value.version()==null?ProductVersion.SDK:value.version()),Utf8.bytes(value.configurationSchema()),
            value.category(),value.inputs(),value.outputs(),value.synchronousInput(),value.errorOutput(),value.optionalInputs(),value.optionalOutputs(),
            value.replaySafe(),value.threadSafe(),value.stateful(),value.blocking(),value.stateVersion(),value.effects(),value.handlesControlEvents(),value.finalAcknowledgement(),value.epochAcknowledgement(),value);
    }
    private static byte[][] configureComponent(DataFlowDefinition definition,String config,String[] inputs) throws Exception {
        Execution.enterCallback();try { String[] schemas=definition.configure().configure(config,inputs);byte[][] result=new byte[schemas.length][];for(int j=0;j<schemas.length;j++)result[j]=Utf8.bytes(schemas[j]);return result; }
        finally { Execution.leaveCallback(); }
    }
    private static DataFlow prepareComponent(DataFlowDefinition definition,long pointer,String config,String[] inputs,String[] outputs) throws Exception {
        DataFlowContext context=new DataFlowContext(pointer);DataFlow component=null;Execution.enterCallback();
        try { component=java.util.Objects.requireNonNull(definition.factory().create());component.open(context,config,inputs,outputs);return component; }
        catch(Exception error){if(component!=null)try{component.close();}catch(Exception ignored){}throw error;}
        finally {context.end();Execution.leaveCallback();}
    }
    private static int stepComponent(DataFlow component,long pointer,int kind,int input,long batch,long epoch,long schemaVersion,long watermark,String position) throws Exception {
        DataFlowContext context=new DataFlowContext(pointer,true);Execution.enterCallback();
        try {return component.step(context,new DataFlow.Event(kind,input,context.batch(batch,false),epoch,schemaVersion,watermark,position));}
        finally {context.end();Execution.leaveCallback();}
    }
    private static void lifecycleComponent(DataFlow component,long pointer,int action) throws Exception {
        DataFlowContext context=new DataFlowContext(pointer);Execution.enterCallback();
        try {if(action==0)component.finish(context);else if(action==1)component.cancel();else if(action==2)component.close();else component.confirm();}
        finally {context.end();Execution.leaveCallback();}
    }
    private static long checkpointComponent(DataFlow component,long pointer) throws Exception {
        DataFlowContext context=new DataFlowContext(pointer);Execution.enterCallback();
        try {CallbackBatch state=java.util.Objects.requireNonNull(component.checkpoint(context));if(state.scope.memory!=context.memory)throw new IllegalArgumentException("Checkpoint belongs to another Native context");return nativeRetainComponentBatch(state.pointer());}
        finally {context.end();Execution.leaveCallback();}
    }
    private static void restoreComponent(DataFlow component,long pointer,long batch) throws Exception {
        DataFlowContext context=new DataFlowContext(pointer);Execution.enterCallback();
        try {component.restore(context,context.batch(batch,false));}finally {context.end();Execution.leaveCallback();}
    }
    private static void confirmEpochComponent(DataFlow component,long epoch,String position) throws Exception {
        Execution.enterCallback();try {component.confirmEpoch(epoch,position);}finally {Execution.leaveCallback();}
    }
    private static native long nativeRetainComponentBatch(long batch);
    private static native void nativeRegisterComponent(long registry,byte[] id,byte[] name,byte[] version,byte[] schema,int category,int inputs,int outputs,
        int synchronous,int error,long optionalInputs,long optionalOutputs,boolean replay,boolean threadSafe,boolean stateful,boolean blocking,
        int stateVersion,int effects,boolean controls,boolean finalAck,boolean epochAck,DataFlowDefinition definition);
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
