using System.Text.Json;

namespace UniversalData;

internal sealed record SdkSourceBinding(DataSourceDefinition Definition, ulong Revision, bool Enabled);
