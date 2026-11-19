using Microsoft.Win32.SafeHandles;

namespace SqlSugar;

internal sealed class SqlBatchHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    internal SqlBatchHandle(IntPtr pointer) : base(true) => SetHandle(pointer);

    protected override bool ReleaseHandle()
    {
        SqlNative.DestroyBatchRaw(handle);
        return true;
    }

    internal IntPtr Detach()
    {
        var pointer = DangerousGetHandle();
        SetHandleAsInvalid();
        return pointer;
    }
}
