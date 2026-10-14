using System.Runtime.InteropServices;
using System.Text;

namespace UniversalData;

/// <summary>Native scalar function. Bind returns a one-field Native schema; evaluate returns one typed value in a Native Batch.</summary>
public sealed record ManagedFunctionDefinition(string Id,string Name,uint MinimumArguments,uint MaximumArguments,
    Func<string,string> Bind,Func<ManagedFunctionContext,ManagedBatch?,ManagedBatch> Evaluate,
    bool Pure = true,bool Deterministic = true,bool ThreadSafe = false,string? Version = null);

public sealed class ManagedFunctionContext
{
    private readonly IntPtr memory,field;private readonly CallbackLifetime lifetime;
    internal ManagedFunctionContext(IntPtr context,IntPtr outputField,CallbackLifetime valid) { memory=context;field=outputField;lifetime=valid; }
    public void CheckCancellation() { lifetime.Open();if(Native.ud_is_cancelled(memory)!=0)throw new UniversalDataException((int)StatusCode.Cancelled,"Function cancelled"); }
    public string ResultSchema {
        get {
            lifetime.Open();Check(Native.ud_batch_create_fields_raw(memory,field,1,0,out var schema));
            try { Check(Native.ud_schema_to_json_raw(schema,out var text,out var length));return DataOwnership.Text(text,length); }
            finally { Native.ud_batch_destroy(schema); }
        }
    }
    public ManagedBatch CreateResult(string rowsJson) {
        CheckCancellation();byte[] schema=Encoding.UTF8.GetBytes(ResultSchema),rows=Encoding.UTF8.GetBytes(rowsJson);
        Check(Native.ud_batch_from_json_schema_raw(memory,schema,(uint)schema.Length,rows,(uint)rows.Length,(int)Rounding.Exact,out var batch));
        return new(memory,batch,lifetime,true);
    }
    private static void Check(int status) { if(status!=0)throw new UniversalDataException(status,"Native function value failed"); }
}
public sealed partial class Registry
{
    public void RegisterFunction(ManagedFunctionDefinition definition) {
        ArgumentNullException.ThrowIfNull(definition);ArgumentNullException.ThrowIfNull(definition.Bind);ArgumentNullException.ThrowIfNull(definition.Evaluate);
        if(!definition.Pure||!definition.Id.Contains('.')||definition.MinimumArguments>definition.MaximumArguments||definition.MaximumArguments>16)
            throw new ArgumentException("Extension functions require a namespace, a pure contract and at most 16 arguments");
        lock(sync) {
            Open();var registration=new FunctionRegistration(definition);using var strings=new NativeStrings();
            var descriptor=new Native.ComponentDescriptor { Size=(uint)Marshal.SizeOf<Native.ComponentDescriptor>(),Abi=ProductVersion.Abi,
                Id=strings.Pin(definition.Id),Name=strings.Pin(definition.Name),Version=strings.Pin(definition.Version??ProductVersion.Sdk),Configuration=strings.Pin("{}"),
                Category=5,SynchronousInput=-1,ErrorOutput=-1,User=registration.Pointer,MinArguments=definition.MinimumArguments,MaxArguments=definition.MaximumArguments,
                Capabilities=0x2000000000000000UL|(definition.Deterministic?0x1000000000000000UL:0)|(definition.ThreadSafe?0x4000000000000000UL:0),
                FunctionBind=Marshal.GetFunctionPointerForDelegate(registration.Bind),FunctionEvaluate=Marshal.GetFunctionPointerForDelegate(registration.Evaluate) };
            try { DataOwnership.Check(context,Native.ud_registry_register_owned(registry,ref descriptor,1,registration.Pointer,registration.Retain,registration.Release)); }
            finally { registration.Drop(); }
        }
    }
}
internal sealed class FunctionRegistration
{
    private readonly ManagedFunctionDefinition definition;private GCHandle root;private int references=1;
    internal IntPtr Pointer=>GCHandle.ToIntPtr(root);
    internal readonly Native.OwnerRetain Retain;internal readonly Native.OwnerRelease Release;
    internal readonly Native.FunctionBindCallback Bind;internal readonly Native.FunctionEvaluateCallback Evaluate;
    internal FunctionRegistration(ManagedFunctionDefinition value) {
        definition=value;root=GCHandle.Alloc(this);Retain=_=>{Interlocked.Increment(ref references);return 0;};Release=_=>Drop();Bind=BindValue;Evaluate=EvaluateValue;
    }
    internal void Drop() { if(Interlocked.Decrement(ref references)==0)root.Free(); }
    private static int Failure(IntPtr memory,Exception error) {
        int code=error is UniversalDataException native?native.Status:(int)StatusCode.Parse;
        using var strings=new NativeStrings();return Native.ud_context_report(memory,code,strings.Pin("Managed function callback failed: "+error.GetType().FullName));
    }
    private int BindValue(IntPtr user,IntPtr memory,IntPtr fields,uint count,out IntPtr output) {
        using var guard=Execution.EnterCallback();output=IntPtr.Zero;IntPtr arguments=IntPtr.Zero;
        try {
            int status=Native.ud_function_argument_schema(memory,fields,count,out arguments);if(status!=0)return status;
            string schema="{\"version\":"+ProductInfo.Protocol+",\"fields\":[]}";
            if(arguments!=IntPtr.Zero){status=Native.ud_schema_to_json_raw(arguments,out var text,out var length);if(status!=0)return status;schema=DataOwnership.Text(text,length);}
            byte[] result=Encoding.UTF8.GetBytes(definition.Bind(schema));
            return Native.ud_schema_from_json_raw(memory,result,(uint)result.Length,default,out output);
        } catch(Exception error){return Failure(memory,error);}finally { if(arguments!=IntPtr.Zero)Native.ud_batch_destroy(arguments); }
    }
    private int EvaluateValue(IntPtr user,IntPtr memory,IntPtr fields,IntPtr values,uint count,IntPtr resultField,out Native.ScalarValue output) {
        using var guard=Execution.EnterCallback();output=default;var lifetime=new CallbackLifetime();IntPtr arguments=IntPtr.Zero;
        try {
            int status=Native.ud_function_arguments(memory,fields,values,count,out arguments);if(status!=0)return status;
            ManagedBatch? batch=arguments==IntPtr.Zero?null:new(memory,arguments,lifetime,true);arguments=IntPtr.Zero;
            var context=new ManagedFunctionContext(memory,resultField,lifetime);var result=definition.Evaluate(context,batch)??throw new InvalidOperationException("Function returned null");
            if(result.Memory!=memory)throw new ArgumentException("Result belongs to another Native context");
            return Native.ud_function_result(memory,result.Pointer,resultField,out output);
        } catch(Exception error){return Failure(memory,error);}finally { if(arguments!=IntPtr.Zero)Native.ud_batch_destroy(arguments);lifetime.Close(); }
    }
}
internal static partial class Native
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]internal delegate int FunctionBindCallback(IntPtr user,IntPtr memory,IntPtr fields,uint count,out IntPtr result);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]internal delegate int FunctionEvaluateCallback(IntPtr user,IntPtr memory,IntPtr fields,IntPtr values,uint count,IntPtr resultField,out ScalarValue result);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)]internal static extern int ud_function_argument_schema(IntPtr memory,IntPtr fields,uint count,out IntPtr batch);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)]internal static extern int ud_function_arguments(IntPtr memory,IntPtr fields,IntPtr values,uint count,out IntPtr batch);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)]internal static extern int ud_function_result(IntPtr memory,IntPtr batch,IntPtr field,out ScalarValue result);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl,EntryPoint="ud_batch_create")]internal static extern int ud_batch_create_fields_raw(IntPtr memory,IntPtr fields,uint columns,uint capacity,out IntPtr batch);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)]internal static extern int ud_is_cancelled(IntPtr memory);
}

public static class ExpressionLanguage
{
    public static string Describe() { using var context=DataOwnership.Context(16777216);DataOwnership.Check(context,Native.ud_expression_language_catalog(context,out var text,out var length));return DataOwnership.Text(text,length); }
}
internal static partial class Native {
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)]internal static extern int ud_expression_language_catalog(ContextHandle context,out IntPtr text,out uint length);
}
