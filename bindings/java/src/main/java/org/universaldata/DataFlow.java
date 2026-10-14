package org.universaldata;
/** Native Component callbacks. No managed scheduling or data-flow engine. */
public interface DataFlow extends AutoCloseable {
    default void open(DataFlowContext context,String configuration,String[] inputs,String[] outputs) throws Exception {}
    int step(DataFlowContext context,Event event) throws Exception;
    default void finish(DataFlowContext context) throws Exception {}
    default void cancel() {}
    default void confirm() throws Exception { throw new UnsupportedOperationException("No final source acknowledgement"); }
    default void confirmEpoch(long epoch,String position) throws Exception { throw new UnsupportedOperationException("No source position acknowledgement"); }
    default CallbackBatch checkpoint(DataFlowContext context) throws Exception { throw new UnsupportedOperationException("No checkpoint"); }
    default void restore(DataFlowContext context,CallbackBatch state) throws Exception { throw new UnsupportedOperationException("No restore"); }
    @Override default void close() throws Exception {}
    record Event(int kind,int input,CallbackBatch batch,long epoch,long schemaVersion,long watermark,String sourcePosition) {}
    int DATA=1,INPUT_END=2,POLL=3,WATERMARK=4,BARRIER=5,SCHEMA_CHANGE=6;
}
