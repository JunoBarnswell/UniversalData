using System.Runtime.InteropServices;

namespace UniversalData;

/// <summary>Canonical native schema discovery and compatibility policy.</summary>
public static class Schema
{
    public static string Compare(string expectedJson, string actualJson, string policyJson, ulong memoryLimit = 268435456)
    {
        using var context = DataOwnership.Context(memoryLimit);
        using var inputs = new NativeStrings(expectedJson, actualJson, policyJson);
        DataOwnership.Check(context, Native.ud_schema_compare_json(context, inputs[0], inputs[1], inputs[2], out var text, out var length));
        return DataOwnership.Text(text, length);
    }
}

internal sealed class NativeStrings : IDisposable
{
    private readonly List<GCHandle> pins = new();
    private readonly Native.StringValue[] values;
    internal NativeStrings(params string[] strings)
    {
        values = new Native.StringValue[strings.Length];
        try
        {
            for (int i = 0; i < strings.Length; i++)
                values[i] = Pin(strings[i]);
        }
        catch { Dispose(); throw; }
    }
    internal Native.StringValue this[int index] => values[index];
    internal Native.StringValue Pin(string value)
    {
        byte[] bytes = Utf8.Bytes(value);
        var pin = GCHandle.Alloc(bytes.Length == 0 ? new byte[1] : bytes, GCHandleType.Pinned);
        pins.Add(pin); return new(pin.AddrOfPinnedObject(), checked((uint)bytes.Length));
    }
    public void Dispose() { foreach (var pin in pins) pin.Free(); pins.Clear(); }
}

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_schema_compare_json(ContextHandle context, StringValue expected, StringValue actual, StringValue policy, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_execution_discover_schema(ExecutionHandle execution, StringValue connection, StringValue table, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_batch_row_info_json(BatchHandle batch, uint row, out IntPtr text, out uint length);
}
