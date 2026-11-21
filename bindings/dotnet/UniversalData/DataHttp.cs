using System.Collections.Concurrent;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using SqlSugar;

namespace UniversalData;

public sealed class DataHttp
{
    private static readonly HttpClient client = new(new SocketsHttpHandler { AllowAutoRedirect = false });
    private readonly DataContext context;
    private readonly SdkSourceBinding binding;

    internal DataHttp(DataContext context, SdkSourceBinding binding) { this.context = context; this.binding = binding; }

    internal async Task<bool> ProbeAsync(CancellationToken cancellation)
    {
        using var scope = context.Link(cancellation);
        context.State.Require(binding, ConnectionAccess.Network);
        using var request = new HttpRequestMessage(HttpMethod.Head, binding.Definition.BaseUri);
        try
        {
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, scope.Token)
                .ConfigureAwait(false);
            return true;
        }
        catch (HttpRequestException) { return false; }
    }

    public async Task<T> GetAsync<T>(string relativePath, CancellationToken cancellation = default)
    {
        using var scope = context.Link(cancellation);
        context.State.Require(binding, ConnectionAccess.Network);
        var configured = binding.Definition.BaseUri!;
        var root = new Uri(configured.AbsoluteUri.TrimEnd('/') + "/", UriKind.Absolute);
        if (!Uri.TryCreate(relativePath, UriKind.Relative, out var relative) || !Uri.TryCreate(root, relative, out var target) ||
            !root.IsBaseOf(target) || target.UserInfo.Length != 0)
            throw new ArgumentException("The HTTP path must remain under the registered base URI", nameof(relativePath));
        using var response = await client.GetAsync(target, HttpCompletionOption.ResponseHeadersRead, scope.Token).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new UniversalDataException((int)StatusCode.HttpStatus, "The HTTP source rejected the request",
                new(ErrorSource.Http, StatusCode.HttpStatus, "Unexpected HTTP status", HttpStatus: (int)response.StatusCode));
        await using var content = await response.Content.ReadAsStreamAsync(scope.Token).ConfigureAwait(false);
        return (await JsonSerializer.DeserializeAsync<T>(content, WorkerProtocol.Json, scope.Token).ConfigureAwait(false))!;
    }
}
