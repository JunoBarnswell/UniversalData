using Microsoft.Win32.SafeHandles;

namespace UniversalData;

internal sealed class SqlSessionHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly ExecutionHandle? execution;

    internal SqlSessionHandle(IntPtr pointer, ExecutionHandle? execution = null) : base(true)
    {
        this.execution = execution;
        SetHandle(pointer);
    }

    protected override bool ReleaseHandle()
    {
        int status = SqlNative.CloseRaw(handle);
        if (status == 0) execution?.DangerousRelease();
        return status == 0;
    }
}
