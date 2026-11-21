namespace UniversalData;

internal sealed record DataConfigurationManagement(string? BaseUrl, string? BearerToken, string? Cookie,
    string? Csrf, Dictionary<string, string> Headers);
