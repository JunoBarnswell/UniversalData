using System.IO.Pipes;
using System.Text;
using System.Text.Json;

namespace UniversalData.Hosting;

public static class WorkerControl
{
    private static string Name(string ticket)
    {
        if (!Guid.TryParseExact(ticket, "N", out _)) throw new ArgumentException("Invalid worker ticket");
        return "ud-v" + ProductInfo.Protocol + "-" + ticket;
    }
    public static async Task<JsonElement> Send(string ticket, string action, JsonElement? command = null,
        CancellationToken cancellation = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        await using var pipe = new NamedPipeClientStream(".", Name(ticket), PipeDirection.InOut, PipeOptions.Asynchronous);
        await pipe.ConnectAsync(deadline.Token);
        using var writer = new StreamWriter(pipe, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
        using var reader = new StreamReader(pipe, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync(JsonSerializer.Serialize(new { version = ProductInfo.Protocol, action, command }, HostPolicy.JsonOptions));
        var text = await reader.ReadLineAsync(deadline.Token) ?? throw new IOException("Worker control response missing");
        using var result = JsonDocument.Parse(text);
        int status = result.RootElement.GetProperty("result").GetInt32();
        if (status != 0) throw new UniversalDataException(status, "Worker control failed");
        return result.RootElement.GetProperty("value").Clone();
    }
    public static async Task Serve(string ticket, Execution execution, CancellationToken cancellation)
    {
        while (!cancellation.IsCancellationRequested) {
            await using var pipe = new NamedPipeServerStream(Name(ticket), PipeDirection.InOut, 1,
                PipeTransmissionMode.Byte, PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);
            try {
                await pipe.WaitForConnectionAsync(cancellation);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                using var reader = new StreamReader(pipe, Encoding.UTF8, leaveOpen: true);
                using var writer = new StreamWriter(pipe, new UTF8Encoding(false), leaveOpen: true) { AutoFlush = true };
                object value = new { }; int status = 0;
                try {
                    var text = await reader.ReadLineAsync(timeout.Token) ?? throw new ArgumentException("Missing command");
                    if (text.Length > 65536) throw new ArgumentException("Control request exceeds limit");
                    using var request = JsonDocument.Parse(text);
                    var root = request.RootElement;
                    if (root.GetProperty("version").GetInt32() != ProductInfo.Protocol) throw new ArgumentException("Incompatible control request");
                    switch (root.GetProperty("action").GetString()) {
                        case "cancel": execution.Cancel(); break;
                        case "snapshot": using (var snapshot = JsonDocument.Parse(execution.DebugSnapshot())) value = snapshot.RootElement.Clone(); break;
                        case "debug": execution.DebugControl(root.GetProperty("command").GetRawText()); break;
                        default: throw new ArgumentException("Unknown control action");
                    }
                } catch (UniversalDataException error) { status = error.Status; }
                  catch (Exception error) when (error is ArgumentException or JsonException or InvalidOperationException) { status = (int)StatusCode.Invalid; }
                await writer.WriteLineAsync(JsonSerializer.Serialize(new { result = status, value }, HostPolicy.JsonOptions));
            } catch (Exception error) when (error is IOException or OperationCanceledException) {
                if (cancellation.IsCancellationRequested) break;
            }
        }
    }
}
