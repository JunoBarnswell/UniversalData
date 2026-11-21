using System.Text.Json;

namespace UniversalData;

internal sealed record SdkRecoveryInvocation(DataRunReference Reference, SdkSourceBinding[] Sources);
