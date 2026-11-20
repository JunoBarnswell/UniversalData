using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace UniversalData;

internal sealed class ContextHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    internal ContextHandle(IntPtr pointer) : base(true) => SetHandle(pointer);

    protected override bool ReleaseHandle() => Native.ud_context_destroy(handle) == 0;
}

internal static partial class Native
{
    internal const string Library = "ud_data";

    [StructLayout(LayoutKind.Sequential)]
    internal struct ContextOptions
    { internal uint Size, Abi; internal ulong Limit; internal IntPtr User, Allocate, Deallocate; }

    [StructLayout(LayoutKind.Sequential)]
    internal struct StringValue

    { internal IntPtr Data; internal uint Length; internal StringValue(IntPtr data, uint length)
        {
            Data = data; Length = length;
        }
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_context_create(ref ContextOptions options, out IntPtr context);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_context_destroy(IntPtr context);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr ud_error(ContextHandle context);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_free(IntPtr pointer);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr ud_alloc(IntPtr context, UIntPtr bytes);

    internal static string Error(ContextHandle context) => Marshal.PtrToStringUTF8(ud_error(context)) ?? "Native error";
}
