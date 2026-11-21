using System.Text.Json;

namespace UniversalData;

internal sealed record SdkInvocation(string RunId, SdkMethod Method, JsonElement? Argument,
    SdkSourceBinding[] Sources, SdkModule[] Modules, string? DefaultSource = null);
