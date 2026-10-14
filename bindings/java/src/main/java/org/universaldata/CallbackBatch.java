package org.universaldata;
/** Borrowed/owned Native Batch. Access expires when its callback scope ends. */
public final class CallbackBatch implements AutoCloseable {
    final CallbackScope scope;private long pointer;private final boolean owned;
    CallbackBatch(CallbackScope scope,long pointer,boolean owned) { this.scope=scope;this.pointer=pointer;this.owned=owned; }
    long pointer() { scope.check();if(pointer==0)throw new IllegalStateException("Batch is closed");return pointer; }
    public String schemaJson() { return nativeText(pointer(),0,0); }
    public String dataJson() { return nativeText(pointer(),1,0); }
    public String rowIdentity(int row) { return nativeText(pointer(),2,row); }
    public int rows() { return nativeCount(pointer(),false); }
    public int columns() { return nativeCount(pointer(),true); }
    public boolean isNull(int row,int column) { return nativeNull(pointer(),row,column); }
    public String utf8(int row,int column) { return nativeUtf8(pointer(),row,column); }
    public long int64(int row,int column) { return nativeInt64(pointer(),row,column); }
    public void copyIdentity(int row,CallbackBatch source,int sourceRow) { if(!owned||scope.memory!=source.scope.memory)throw new IllegalArgumentException("Identity needs an owned Batch in the same Native context");nativeCopyIdentity(pointer(),row,source.pointer(),sourceRow); }
    @Override public void close() { if(pointer==0)return;scope.check();if(owned)nativeDestroy(pointer);pointer=0; }
    static native long nativeCreate(long memory,byte[] schema,byte[] rows);
    private static native String nativeText(long batch,int kind,int row);
    private static native int nativeCount(long batch,boolean columns);
    private static native boolean nativeNull(long batch,int row,int column);
    private static native String nativeUtf8(long batch,int row,int column);
    private static native long nativeInt64(long batch,int row,int column);
    private static native void nativeCopyIdentity(long batch,int row,long source,int sourceRow);
    private static native void nativeDestroy(long batch);
}
