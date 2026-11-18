using System.Text.Json;

namespace UniversalData.Management;

public sealed record MessageConnectionTestRequest(JsonElement Package, string Connection);
