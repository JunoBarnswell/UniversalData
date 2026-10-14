package org.universaldata;
import java.util.ArrayList;
/** One Native callback, including ownership of every Batch created inside it. */
abstract class CallbackScope {
    final long memory;
    private final Thread thread=Thread.currentThread();
    private boolean active=true;
    private final ArrayList<CallbackBatch> owned=new ArrayList<>();
    CallbackScope(long memory) { this.memory=memory; }
    final void check() { if(!active||Thread.currentThread()!=thread)throw new IllegalStateException("Native callback has ended or belongs to another thread"); }
    final CallbackBatch batch(long pointer,boolean ownership) { check();if(pointer==0)return null;CallbackBatch result=new CallbackBatch(this,pointer,ownership);if(ownership)owned.add(result);return result; }
    final void end() { if(!active)return;for(var item:owned)item.close();active=false; }
    final CallbackBatch create(String schema,String data) { check();return batch(CallbackBatch.nativeCreate(memory,Utf8.bytes(schema),Utf8.bytes(data)),true); }
}
