using Microsoft.Win32.SafeHandles;

namespace SqlSugar;

internal sealed class SqlSessionHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    internal SqlSessionHandle(IntPtr pointer) : base(true) => SetHandle(pointer);

    protected override bool ReleaseHandle() => SqlNative.CloseRaw(handle) == 0;
}
