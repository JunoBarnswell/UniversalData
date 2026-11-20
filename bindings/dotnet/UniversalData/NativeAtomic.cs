using System.Runtime.InteropServices;

namespace UniversalData;

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_activation_sql_session_open(IntPtr activation, StringValue connection,
        out IntPtr session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_execution_recover_transaction(ExecutionHandle execution, out int decision);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_sql_connection_config(ContextHandle context, int dialect, StringValue input,
        out StringValue config);
}

internal static class NativeSqlConfiguration
{
    internal static string Create(int dialect, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        using var context = DataOwnership.Context(4194304);
        using var input = new NativeStrings(connectionString);
        try
        {
            DataOwnership.Check(context, Native.ud_sql_connection_config(context, dialect, input[0], out var config));
            try { return Native.Text(config); }
            finally
            {
                Clear(config);
                Native.ud_free(config.Data);
            }
        }
        finally { Clear(input[0]); }
    }

    private static void Clear(Native.StringValue value)
    {
        for (var offset = 0; offset < value.Length; ++offset)
            Marshal.WriteByte(value.Data, offset, 0);
    }
}
