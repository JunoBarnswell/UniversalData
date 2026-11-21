using System.Runtime.InteropServices;

namespace UniversalData;

internal enum NativeRunState
{
    Queued,
    Claimed,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    RecoveryRequired,
    Stopped
}

internal readonly record struct NativeRetryDecision(NativeRunState State, StatusCode Result,
    bool Retry, long NotBeforeMilliseconds);

/// <summary>Native owns terminal retry/recovery classification.</summary>
internal static class NativeDispatch
{
    internal static NativeRetryDecision Retry(StatusCode result, bool cancelled, bool started,
        bool resumed, bool brokerRestore, uint attempt, uint maxAttempts, uint retryDelaySeconds,
        long nowMilliseconds, bool remote = false)
    {
        var request = new Native.RetryRequest
        {
            Size = (uint)Marshal.SizeOf<Native.RetryRequest>(), Abi = ProductVersion.Abi,
            Result = (int)result, Cancelled = cancelled ? 1 : 0, Started = started ? 1 : 0,
            Resumed = resumed ? 1 : 0, BrokerRestore = brokerRestore ? 1 : 0,
            Remote = remote ? 1 : 0,
            Attempt = attempt, MaxAttempts = maxAttempts, RetryDelaySeconds = retryDelaySeconds,
            NowMs = nowMilliseconds
        };
        var decision = new Native.RetryDecision
        {
            Size = (uint)Marshal.SizeOf<Native.RetryDecision>(), Abi = ProductVersion.Abi
        };
        int status = Native.ud_scheduler_retry(ref request, ref decision);
        if (status != 0)
            throw new UniversalDataException(status,
                Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native dispatch error");
        return new((NativeRunState)decision.State, (StatusCode)decision.Result,
            decision.Retry != 0, decision.NotBeforeMs);
    }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct RetryRequest
    {
        internal uint Size, Abi;
        internal int Result, Cancelled, Started, Resumed, BrokerRestore, Remote;
        internal uint Attempt, MaxAttempts, RetryDelaySeconds;
        internal long NowMs;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct RetryDecision
    {
        internal uint Size, Abi;
        internal int State, Result, Retry;
        internal long NotBeforeMs;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_scheduler_retry(ref RetryRequest request, ref RetryDecision decision);
}
