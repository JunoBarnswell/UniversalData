using System.Runtime.InteropServices;
using UniversalData;

namespace SqlSugar;

internal static class SqlWire
{
    [DllImport(Native.Library, EntryPoint = "ud_batch_serialize", CallingConvention = CallingConvention.Cdecl)]
    private static extern int Serialize(SqlBatchHandle batch, out IntPtr value, out ulong length);
    [DllImport(Native.Library, EntryPoint = "ud_batch_deserialize", CallingConvention = CallingConvention.Cdecl)]
    private static extern int Deserialize(IntPtr context, byte[] value, ulong length, out IntPtr batch);

    internal static WorkerBatchBuffer Encode(SqlBatchHandle batch)
    {
        SqlNative.Check(Serialize(batch, out var value, out var length));
        try
        {
            if (length > WorkerProtocol.MaximumBatchBytes)
                throw new UniversalDataException((int)StatusCode.Resource, "Worker batch exceeds its bound");
            var buffer = new WorkerBatchBuffer((int)length);
            try
            {
                Marshal.Copy(value, buffer.Bytes, 0, buffer.Length);
                return buffer;
            }
            catch { buffer.Dispose(); throw; }
        }
        finally { Native.ud_free(value); }
    }

    internal static SqlBatchHandle Decode(IntPtr context, WorkerBatchBuffer value)
    {
        SqlNative.Check(Deserialize(context, value.Bytes, (ulong)value.Length, out var batch));
        return new SqlBatchHandle(batch);
    }
}
