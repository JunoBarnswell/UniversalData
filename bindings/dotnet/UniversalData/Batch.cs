using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using System.Text;

namespace UniversalData;

public enum Rounding
{ Exact = 0, TowardZero = 1, HalfEven = 2, HalfAway = 3, Floor = 4, Ceiling = 5 }

/// <summary>Owned columnar batch. Schema and typed nested JSON follow the C contract.</summary>
public sealed class Batch : IDisposable
{
    private readonly ContextHandle context;
    private readonly BatchHandle batch;
    private readonly object execution = new();
    private bool disposed;

    public Batch(string schemaJson, string dataJson, Rounding rounding = Rounding.Exact, ulong memoryLimit = 268435456)
    {
        var schema = Utf8.Bytes(schemaJson); var data = Utf8.Bytes(dataJson);
        context = DataOwnership.Context(memoryLimit);
        try
        {
            DataOwnership.Check(context, Native.ud_batch_from_json_schema(context, schema, checked((uint)schema.Length),
                data, checked((uint)data.Length), (int)rounding, out var pointer));
            batch = new BatchHandle(pointer, context);
        }
        catch { context.Dispose(); throw; }
    }

    private Batch(ContextHandle owner, IntPtr pointer)
    { context = owner; batch = new BatchHandle(pointer, owner); }

    public static Batch FromWire(byte[] wire, ulong memoryLimit = 268435456)
    {
        ArgumentNullException.ThrowIfNull(wire); var ctx = DataOwnership.Context(memoryLimit);
        try
        {
            DataOwnership.Check(ctx, Native.ud_batch_deserialize(ctx, wire, checked((ulong)wire.LongLength), out var pointer));
            return new Batch(ctx, pointer);
        }
        catch { ctx.Dispose(); throw; }
    }

    public uint Rows
    { get { lock (execution) { EnsureOpen(); return Native.ud_batch_rows(batch); } } }
    public uint Columns
    { get { lock (execution) { EnsureOpen(); return Native.ud_batch_columns(batch); } } }

    public string ToJson()
    {
        lock (execution)
        {
            EnsureOpen(); DataOwnership.Check(context, Native.ud_batch_to_json(batch, out var pointer, out var length));
            return DataOwnership.Text(pointer, length);
        }
    }

    public string SchemaJson
    {
        get
        {
            lock (execution)
            {
                EnsureOpen(); DataOwnership.Check(context, Native.ud_schema_to_json(batch, out var pointer, out var length));
                return DataOwnership.Text(pointer, length);
            }
        }
    }

    public byte[] ToWire()
    {
        lock (execution)
        {
            EnsureOpen(); DataOwnership.Check(context, Native.ud_batch_serialize(batch, out var pointer, out var length));
            try
            {
                if (length > int.MaxValue) throw new UniversalDataException(7, "Managed byte array length overflow");
                var bytes = new byte[(int)length]; Marshal.Copy(pointer, bytes, 0, bytes.Length); return bytes;
            }
            finally { Native.ud_free(pointer); }
        }
    }

    public Batch Cast(string schemaJson, Rounding rounding = Rounding.Exact, ulong memoryLimit = 268435456)
    {
        var schema = Utf8.Bytes(schemaJson);
        lock (execution)
        {
            EnsureOpen(); var ctx = DataOwnership.Context(memoryLimit);
            try
            {
                DataOwnership.Check(ctx, Native.ud_batch_cast_schema(ctx, batch, schema, checked((uint)schema.Length), (int)rounding, out var pointer));
                return new Batch(ctx, pointer);
            }
            catch { ctx.Dispose(); throw; }
        }
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);

    public void Dispose()
    {
        lock (execution) { if (disposed) return; disposed = true; batch.Dispose(); context.Dispose(); }
        GC.SuppressFinalize(this);
    }
}

internal static class Utf8
{
    private static readonly UTF8Encoding Encoding = new(false, true);

    internal static byte[] Bytes(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        try { return Encoding.GetBytes(value); }
        catch (EncoderFallbackException) { throw new UniversalDataException(6, "Invalid UTF16 input"); }
    }
}

internal static class DataOwnership
{
    internal static ContextHandle Context(ulong limit)
    {
        var options = new Native.ContextOptions { Size = (uint)Marshal.SizeOf<Native.ContextOptions>(), Abi = ProductVersion.Abi, Limit = limit };
        int status = Native.ud_context_create(ref options, out var pointer);
        if (status != 0) throw new UniversalDataException(status, "Cannot create native context");
        return new ContextHandle(pointer);
    }

    internal static void Check(ContextHandle ctx, int status)
    {
        _ = ctx;
        if (status != 0) throw new UniversalDataException(status, Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native error");
    }

    internal static string Text(IntPtr pointer, uint length)
    {
        try
        {
            if (length > int.MaxValue) throw new UniversalDataException(7, "Managed string length overflow");
            return Marshal.PtrToStringUTF8(pointer, (int)length)!;
        }
        finally { Native.ud_free(pointer); }
    }
}

internal sealed class BatchHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly ContextHandle owner;

    internal BatchHandle(IntPtr pointer, ContextHandle context) : base(true)
    { owner = context; bool retained = false; context.DangerousAddRef(ref retained); SetHandle(pointer); }

    protected override bool ReleaseHandle()
    { Native.ud_batch_destroy(handle); owner.DangerousRelease(); return true; }
}

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr ud_status_name(int status);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_batch_from_json_schema(ContextHandle ctx, byte[] schema, uint schemaLength, byte[] rows, uint length, int rounding, out IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_batch_cast_schema(ContextHandle ctx, BatchHandle batch, byte[] schema, uint schemaLength, int rounding, out IntPtr output);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint ud_batch_rows(BatchHandle batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint ud_batch_columns(BatchHandle batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_batch_to_json(BatchHandle batch, out IntPtr json, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_schema_to_json(BatchHandle batch, out IntPtr json, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_batch_serialize(BatchHandle batch, out IntPtr wire, out ulong length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_batch_deserialize(ContextHandle ctx, byte[] wire, ulong length, out IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_batch_destroy(IntPtr batch);
}