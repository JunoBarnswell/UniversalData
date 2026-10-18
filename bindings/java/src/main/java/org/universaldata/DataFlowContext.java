package org.universaldata;
/** Activation and Batch authority borrowed for one Native lifecycle callback. */
public final class DataFlowContext extends CallbackScope {
    private final long context;private final boolean emissions;
    DataFlowContext(long context) { this(context,false); }
    DataFlowContext(long context,boolean emissions) { super(nativeMemory(context));this.context=context;this.emissions=emissions; }
    public boolean isStopping() { check();return nativeNumber(context,3)!=0; }
    public void checkCancellation() { check();nativeCheck(context); }
    public String taskId() { check();return nativeText(context,0,0); }
    public String pipelineId() { check();return nativeText(context,1,0); }
    public String componentId() { check();return nativeText(context,2,0); }
    public String recoveryPosition() { check();return nativeText(context,3,0); }
    public long recoveryEpoch() { check();return nativeNumber(context,0); }
    public boolean recoveryTerminal() { check();return nativeNumber(context,1)!=0; }
    public int recommendedBatchRows() { check();return (int)nativeNumber(context,2); }
    public String outputSchema(int output) { check();return nativeText(context,4,output); }
    public String inputPlan(int input) { check();return nativeText(context,5,input); }
    public CallbackBatch createBatch(String schema,String rows) { checkCancellation();return create(schema,rows); }
    public void emit(int output,CallbackBatch batch) { check();if(!emissions)throw new IllegalStateException("Only step may emit");if(batch.scope.memory!=memory)throw new IllegalArgumentException("Batch belongs to another Native context");nativeEmit(context,output,batch.pointer()); }
    public void emitControl(int output,DataFlow.Event event) { check();if(!emissions)throw new IllegalStateException("Only step may emit");if(event.batch()!=null&&event.batch().scope.memory!=memory)throw new IllegalArgumentException("Event Batch belongs to another Native context");nativeEmitControl(context,output,event.kind(),event.batch()==null?0:event.batch().pointer(),event.epoch(),event.schemaVersion(),event.watermark(),Utf8.bytes(event.sourcePosition())); }
    public void setSourceIdentity(CallbackBatch batch,int row,long position,String source) { check();if(batch.scope.memory!=memory)throw new IllegalArgumentException("Batch belongs to another Native context");nativeSourceIdentity(context,batch.pointer(),row,position,Utf8.bytes(source)); }
    public CallbackBatch errorBatch(int capacity,boolean includeInput) { check();return batch(nativeErrorBatch(context,capacity,includeInput),true); }
    public void appendError(CallbackBatch errors,CallbackBatch input,int row,String rule,int ruleVersion,String fieldIdentity,int status) {
        check();if(errors.scope.memory!=memory||input.scope.memory!=memory)throw new IllegalArgumentException("Error batches belong to another context");
        nativeAppendError(context,errors.pointer(),input.pointer(),row,Utf8.bytes(rule),ruleVersion,Utf8.bytes(fieldIdentity),status);
    }
    private static native long nativeMemory(long context);
    private static native void nativeCheck(long context);
    private static native String nativeText(long context,int kind,int port);
    private static native long nativeNumber(long context,int kind);
    private static native void nativeEmit(long context,int output,long batch);
    private static native void nativeEmitControl(long context,int output,int kind,long batch,long epoch,long schemaVersion,long watermark,byte[] position);
    private static native void nativeSourceIdentity(long context,long batch,int row,long position,byte[] source);
    private static native long nativeErrorBatch(long context,int capacity,boolean includeInput);
    private static native void nativeAppendError(long context,long errors,long input,int row,byte[] rule,int ruleVersion,byte[] field,int status);
}
