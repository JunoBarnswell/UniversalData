using System.Runtime.InteropServices;
using System.Text;

namespace UniversalData;

internal static class WorkerNativeError
{
    internal static OperationError Create(StatusCode status, IntPtr context = default)
    {
        var detail = Detail();
        if (context != IntPtr.Zero && Native.ud_error_details(context, ref detail) == 0 && detail.Cause != 0)
            return Decode(detail);
        return new(ErrorSource.Native, status, Name((int)status));
    }

    internal static OperationError FromExecution(ExecutionHandle execution, StatusCode status)
    {
        var detail = Detail();
        return Native.ud_execution_error_details(execution, ref detail) == 0 && detail.Cause != 0
            ? Decode(detail) : Create(status);
    }

    private static Native.ErrorDetail Detail() => new()
    {
        Size = (uint)Marshal.SizeOf<Native.ErrorDetail>(), Abi = ProductVersion.Abi, SqlState = new byte[6]
    };

    private static OperationError Decode(Native.ErrorDetail detail)
    {
        string? sqlState = detail.SqlState[0] == 0 ? null : Encoding.ASCII.GetString(detail.SqlState, 0, 5);
        return new((ErrorSource)detail.Source, (StatusCode)detail.Cause, Name(detail.Cause),
            detail.Source is (int)ErrorSource.Database or (int)ErrorSource.Transport ? detail.NativeCode : null,
            sqlState, detail.Source == (int)ErrorSource.Http ? checked((int)detail.NativeCode) : null);
    }

    internal static int ReportComponent(IntPtr context, Exception failure)
    {
        var code = failure is UniversalDataException native && native.Status is > 1 and <= (int)StatusCode.HttpStatus &&
            native.Code != StatusCode.More ? native.Code : StatusCode.ManagedComponent;
        var detail = Detail();
        detail.Source = (int)ErrorSource.Component;
        detail.Cause = (int)code;
        using var message = new NativeStrings("Managed component callback failed");
        return Native.ud_context_report_error(context, (int)code, message[0], ref detail);
    }

    private static string Name(int status)
    {
        try { return Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native operation failed"; }
        catch (DllNotFoundException) { return ((StatusCode)status).ToString(); }
    }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct ErrorDetail
    {
        internal uint Size, Abi;
        internal int Source, Cause;
        internal long NativeCode;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)]
        internal byte[] SqlState;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_error_details(IntPtr context, ref ErrorDetail detail);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_execution_error_details(ExecutionHandle execution, ref ErrorDetail detail);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_context_report_error(IntPtr context, int code, StringValue message, ref ErrorDetail detail);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void ud_context_clear_error(IntPtr context);
}
