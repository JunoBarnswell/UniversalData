using System.Net.WebSockets;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal static class WorkerNodeEndpoint
{
    internal static void Map(WebApplication app, IReadOnlyDictionary<string, WorkerNodeTrust> trust)
    {
        if (trust.Count == 0)
            return;
        app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(5), KeepAliveTimeout = TimeSpan.FromSeconds(10) });
        app.MapGet(WorkerNodeChannel.RegistrationPath, (HttpContext ctx, WorkerNodeDirectory directory) =>
            Accept(ctx, directory, trust, true)).AllowAnonymous();
        app.MapGet(WorkerNodeChannel.ExecutionPath, (HttpContext ctx, WorkerNodeDirectory directory) =>
            Accept(ctx, directory, trust, false)).AllowAnonymous();
    }

    private static async Task Accept(HttpContext context, WorkerNodeDirectory directory,
        IReadOnlyDictionary<string, WorkerNodeTrust> trust, bool registration)
    {
        var stopping = context.RequestServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, stopping);
        var certificate = await context.Connection.GetClientCertificateAsync(lifetime.Token);
        if (!context.Request.IsHttps || certificate is null || !context.WebSockets.IsWebSocketRequest ||
            !trust.Values.Any(entry => WorkerNodeCertificate.Matches(certificate, entry.CertificateSha256)))
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        using var channel = new WorkerNodeChannel(await context.WebSockets.AcceptWebSocketAsync());
        try
        {
            using var handshake = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
            handshake.CancelAfter(TimeSpan.FromSeconds(8));
            var frame = await channel.ReadAsync(handshake.Token, 262144);
            if (frame is null || !trust.TryGetValue(frame.NodeId, out var node) ||
                !WorkerNodeCertificate.Matches(certificate, node.CertificateSha256))
                throw new InvalidDataException("Worker node certificate does not match its identity");
            if (registration && frame.Kind == "register" && frame is { MemberId.Length: 0, Generation: 0, Lease.Length: 0, LeadershipGeneration: 0 } && Guid.TryParseExact(frame.AgentBootId,"N",out _))
            {
                var capacity = frame.Data?.Deserialize<WorkerNodeCapacity>(HostPolicy.JsonOptions);
                if (capacity is not { Valid: true })
                    throw new InvalidDataException("Invalid Worker node capacity");
                string[] tenants = node.Tenants.Intersect(capacity.Tenants, StringComparer.Ordinal).ToArray();
                if (tenants.Length == 0)
                    throw new InvalidDataException("Worker node has no shared tenant scope");
                await directory.RegisterAsync(new RemoteWorkerNode(frame.NodeId, capacity.MaxCpu,
                    capacity.MaxProcesses, capacity.MaxMemoryBytes, tenants,
                    tenants.ToDictionary(tenant => tenant, tenant => capacity.Inventory[tenant], StringComparer.Ordinal)), frame.AgentBootId, channel, lifetime.Token);
            }
            else if (!registration && frame.Kind == "attach" && frame.ValidLease)
                await directory.AttachAsync(frame, channel, lifetime.Token);
            else
                throw new InvalidDataException("Invalid Worker node handshake");
        }
        catch (Exception error) when (error is IOException or WebSocketException or JsonException or OperationCanceledException or InvalidOperationException) { }
    }
}
