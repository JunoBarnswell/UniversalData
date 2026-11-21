using System.Text.Json;

namespace UniversalData;

internal sealed record SdkMethod(string ModuleSha256, string DeclaringType, string Name,
    string[] ParameterTypes, bool HasArgument);
