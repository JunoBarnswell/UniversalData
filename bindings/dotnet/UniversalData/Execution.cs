using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
namespace UniversalData;
public enum ExecutionState { Created, Validating, Ready, Running, Draining, Succeeded, Failed, Cancelled, Skipped, RecoveryRequired }
public enum ConnectionAccess { Open=1, Discover, Read, Write, Receipt, Delete=8, Execute=9, Network=10 }
public delegate bool PackageAuthorizer(string tenant,string resource,ConnectionAccess access);
public delegate string PackageCredentialResolver(string tenant,string resource,string reference);
public sealed record ExecutionMetrics(ExecutionState State,StatusCode Result,ulong InputRows,ulong OutputRows,ulong RejectedRows,ulong Batches,ulong QueuedBatches,ulong QueueHighWater,ulong MemoryUsed,ulong CompletedTasks,ulong SpillBytes);
/// <summary>Thin projection of the only native Package execution lifecycle.</summary>
public sealed class Execution : IDisposable
{
    [ThreadStatic] private static int callbackDepth;
    private readonly ExecutionHandle execution;
    private readonly object controller=new();
    private bool disposed;
    public Execution(string packageJson,string tenant,PackageAuthorizer? authorize,string parametersJson="{}",string executionId="",PackageCredentialResolver? resolve=null,
        uint edgeCapacity=2,uint maxBatchRows=4096,ulong memoryLimit=268435456,ulong timeoutMilliseconds=0,string? stateDirectory=null,bool resume=false,uint maxConcurrency=1)
    {
        Native.Authorizer? grant=authorize is null?null:(_,scope,resource,action)=>
        {
            callbackDepth++;try { return authorize(Text(scope),Text(resource),(ConnectionAccess)action)?0:13; }catch { return 13; }finally { callbackDepth--; }
        };
        Native.Resolver? resolver=resolve is null?null:delegate(IntPtr user,IntPtr context,Native.StringValue scope,Native.StringValue resource,Native.StringValue reference,out Native.StringValue output)
        {
            output=default;callbackDepth++;
            try
            {
                var data=Utf8.Bytes(resolve(Text(scope),Text(resource),Text(reference)));var pointer=Native.ud_alloc(context,(UIntPtr)checked((uint)data.Length+1));
                if(pointer==IntPtr.Zero)return 4;Marshal.Copy(data,0,pointer,data.Length);Marshal.WriteByte(pointer,data.Length,0);output=new(pointer,checked((uint)data.Length));return 0;
            }
            catch { return 13; }finally { callbackDepth--; }
        };
        var pins=new List<GCHandle>();
        Native.StringValue Pin(string value)
        {
            var data=Utf8.Bytes(value);var pin=GCHandle.Alloc(data.Length==0?new byte[1]:data,GCHandleType.Pinned);pins.Add(pin);return new(pin.AddrOfPinnedObject(),checked((uint)data.Length));
        }
        try
        {
            var document=Pin(packageJson);
            var options=new Native.ExecutionOptions
            {
                Size=(uint)Marshal.SizeOf<Native.ExecutionOptions>(),Abi=4,
                Context=new Native.ContextOptions { Size=(uint)Marshal.SizeOf<Native.ContextOptions>(),Abi=4,Limit=memoryLimit },
                Capacity=edgeCapacity,MaxRows=maxBatchRows,Depth=32,Timeout=timeoutMilliseconds,
                Parameters=Pin(parametersJson),Tenant=Pin(tenant),ExecutionId=Pin(executionId),StateDirectory=Pin(stateDirectory??""),Resume=resume?1:0,MaxConcurrency=maxConcurrency,
                Authorize=grant is null?IntPtr.Zero:Marshal.GetFunctionPointerForDelegate(grant),Resolve=resolver is null?IntPtr.Zero:Marshal.GetFunctionPointerForDelegate(resolver)
            };
            Check(Native.ud_execution_create_json(document,ref options,out var pointer));execution=new ExecutionHandle(pointer,grant,resolver);
        }
        finally { foreach(var pin in pins)pin.Free();GC.KeepAlive(grant);GC.KeepAlive(resolver); }
    }
    private static string Text(Native.StringValue text)=>Marshal.PtrToStringUTF8(text.Data,checked((int)text.Length))??"";
    private static void Check(int status) { if(status!=0)throw new UniversalDataException(status,Marshal.PtrToStringUTF8(Native.ud_status_name(status))??"Native error"); }
    private void Open()=>ObjectDisposedException.ThrowIf(disposed,this);
    private static void Control() { if(callbackDepth!=0)throw new InvalidOperationException("Execution callbacks cannot start, wait or dispose an execution"); }
    public void Execute() { Control();lock(controller) { Open();Check(Native.ud_execution_execute(execution)); } }
    public void Start() { Control();lock(controller) { Open();Check(Native.ud_execution_start(execution)); } }
    public bool Wait(uint timeoutMilliseconds) { Control();lock(controller) { Open();int s=Native.ud_execution_wait(execution,timeoutMilliseconds);if(s==11)return false;Check(s);return true; } }
    public void Cancel() { try { if(!execution.IsClosed)Native.ud_execution_cancel(execution); }catch(ObjectDisposedException) { } }
    public ExecutionMetrics Metrics
    {
        get { Open();var m=new Native.ExecutionMetricsValue { Size=(uint)Marshal.SizeOf<Native.ExecutionMetricsValue>(),Abi=4 };Check(Native.ud_execution_get_metrics(execution,ref m));return new((ExecutionState)m.State,(StatusCode)m.Result,m.Input,m.Output,m.Rejected,m.Batches,m.Queued,m.HighWater,m.Memory,m.Completed,m.Spill); }
    }
    public void Dispose() { Control();Cancel();lock(controller) { if(disposed)return;disposed=true;execution.Dispose(); } }
}
internal sealed class ExecutionHandle:SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly Native.Authorizer? authorizer;private readonly Native.Resolver? resolver;
    internal ExecutionHandle(IntPtr pointer,Native.Authorizer? grant,Native.Resolver? resolve):base(true) { authorizer=grant;resolver=resolve;SetHandle(pointer); }
    protected override bool ReleaseHandle()
    {
        Native.ud_execution_cancel_raw(handle);int result;
        do { result=Native.ud_execution_destroy(handle);if(result==11)Native.ud_execution_wait_raw(handle,100); }while(result==11);
        GC.KeepAlive(authorizer);GC.KeepAlive(resolver);return result==0;
    }
}
internal static partial class Native
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int Authorizer(IntPtr user,StringValue tenant,StringValue resource,int action);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int Resolver(IntPtr user,IntPtr context,StringValue tenant,StringValue resource,StringValue reference,out StringValue value);
    [StructLayout(LayoutKind.Sequential)] internal struct ExecutionOptions
    {
        internal uint Size,Abi;internal ContextOptions Context;internal uint Capacity,MaxRows,Depth;internal ulong Timeout;
        internal StringValue Parameters,Tenant,ExecutionId;internal IntPtr Registry,Authorize,AuthorizeUser,Resolve,ResolveUser,Observe,ObserveUser;internal StringValue StateDirectory;internal int Resume;internal uint MaxConcurrency;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct ExecutionMetricsValue
    { internal uint Size,Abi;internal int State,Result;internal ulong Input,Output,Rejected,Batches,Queued,HighWater,Memory,Completed,Spill; }
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_create_json(StringValue json,ref ExecutionOptions options,out IntPtr execution);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_execute(ExecutionHandle execution);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_start(ExecutionHandle execution);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_wait(ExecutionHandle execution,uint timeout);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern void ud_execution_cancel(ExecutionHandle execution);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_get_metrics(ExecutionHandle execution,ref ExecutionMetricsValue metrics);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl,EntryPoint="ud_execution_cancel")] internal static extern void ud_execution_cancel_raw(IntPtr execution);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl,EntryPoint="ud_execution_wait")] internal static extern int ud_execution_wait_raw(IntPtr execution,uint timeout);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_execution_destroy(IntPtr execution);
}
