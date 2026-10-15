using System.IO.Pipes;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Security.Cryptography;

namespace UniversalData.Hosting;

/// <summary>Execution-scoped control capability. Unix sockets remain reachable inside the same bounded state-directory mount.</summary>
public static class WorkerControl
{
    private static string Name(string ticket) {
        if (!Guid.TryParseExact(ticket,"N",out _)) throw new ArgumentException("Invalid Worker ticket");
        return "ud-v" + ProductInfo.Protocol + "-" + ticket;
    }
    private static string SocketPath(string directory,string ticket) {
        _ = Name(ticket); HostFiles.NoLinks(directory); var path = Path.Combine(directory,"control.sock");
        if (Encoding.UTF8.GetByteCount(path) >= 104) throw new ArgumentException("Execution control path exceeds Unix socket limit");
        return path;
    }
    public static async Task<JsonElement> Send(string directory,string ticket,string token,string action,JsonElement? command = null,CancellationToken cancellation = default)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation); deadline.CancelAfter(TimeSpan.FromSeconds(5));
        await using Stream stream = await Connect(directory,ticket,deadline.Token);
        using var writer = new StreamWriter(stream,new UTF8Encoding(false),leaveOpen:true) { AutoFlush = true };
        using var reader = new StreamReader(stream,Encoding.UTF8,leaveOpen:true);
        await writer.WriteLineAsync(JsonSerializer.Serialize(new { version = ProductInfo.Protocol,token,action,command },HostPolicy.JsonOptions));
        string text = await WorkerProtocol.ReadLine(reader,8388608,deadline.Token) ?? throw new IOException("Worker control response missing");
        using var result = JsonDocument.Parse(text); int status = result.RootElement.GetProperty("result").GetInt32();
        if (status != 0) throw new UniversalDataException(status,"Worker control failed"); return result.RootElement.GetProperty("value").Clone();
    }
    private static async Task<Stream> Connect(string directory,string ticket,CancellationToken cancellation)
    {
        if (OperatingSystem.IsWindows()) {
            var pipe = new NamedPipeClientStream(".",Name(ticket),PipeDirection.InOut,PipeOptions.Asynchronous);
            try { await pipe.ConnectAsync(cancellation); return pipe; } catch { pipe.Dispose(); throw; }
        }
        var socket = new Socket(AddressFamily.Unix,SocketType.Stream,ProtocolType.Unspecified);
        try { await socket.ConnectAsync(new UnixDomainSocketEndPoint(SocketPath(directory,ticket)),cancellation); return new NetworkStream(socket,ownsSocket:true); }
        catch (SocketException error) { socket.Dispose(); throw new IOException("Worker control endpoint is unavailable",error); }
        catch { socket.Dispose(); throw; }
    }
    // Binding is synchronous: returning from Serve guarantees the endpoint is
    // accepting connections. Worker publishes ready only after this boundary.
    public static Task Serve(string directory,string ticket,string token,Execution execution,CancellationToken cancellation,SecretFilter? filter = null)
    {
        cancellation.ThrowIfCancellationRequested();
        Socket? listener = null; string? address = null; NamedPipeServerStream? firstPipe = null;
        try {
            if (!OperatingSystem.IsWindows()) {
                Directory.CreateDirectory(directory); File.SetUnixFileMode(directory,UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
                address = SocketPath(directory,ticket); if (File.Exists(address)) File.Delete(address);
                listener = new Socket(AddressFamily.Unix,SocketType.Stream,ProtocolType.Unspecified);
                listener.Bind(new UnixDomainSocketEndPoint(address)); File.SetUnixFileMode(address,UnixFileMode.UserRead | UnixFileMode.UserWrite); listener.Listen(4);
            } else firstPipe = Pipe(ticket);
            var serving = ServeOpened(ticket,token,execution,cancellation,filter,listener,address,firstPipe);
            if (serving.IsCompleted) serving.GetAwaiter().GetResult();
            return serving;
        } catch {
            listener?.Dispose(); firstPipe?.Dispose();
            if (address is not null && File.Exists(address)) File.Delete(address);
            throw;
        }
    }
    private static NamedPipeServerStream Pipe(string ticket) => new(Name(ticket),PipeDirection.InOut,1,
        PipeTransmissionMode.Byte,PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);
    private static async Task ServeOpened(string ticket,string token,Execution execution,CancellationToken cancellation,SecretFilter? filter,
                                          Socket? listener,string? address,NamedPipeServerStream? firstPipe)
    {
        try {
            while(!cancellation.IsCancellationRequested) {
                Stream? stream = null;
                try {
                    if (listener is not null) stream = new NetworkStream(await listener.AcceptAsync(cancellation),ownsSocket:true);
                    else {
                        var pipe = firstPipe ?? Pipe(ticket); firstPipe = null;
                        stream = pipe; await pipe.WaitForConnectionAsync(cancellation);
                    }
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation); timeout.CancelAfter(TimeSpan.FromSeconds(5));
                    using var reader = new StreamReader(stream,Encoding.UTF8,leaveOpen:true);
                    using var writer = new StreamWriter(stream,new UTF8Encoding(false),leaveOpen:true) { AutoFlush = true };
                    object value = new { }; int status = 0;
                    try {
                        var text = await WorkerProtocol.ReadLine(reader,65536,timeout.Token) ?? throw new ArgumentException("Missing command");
                        using var request = JsonDocument.Parse(text); var root = request.RootElement;
                        if (root.GetProperty("version").GetInt32() != ProductInfo.Protocol) throw new ArgumentException("Incompatible control request");
                        var supplied = root.GetProperty("token").GetString() ?? "";
                        if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(token),Encoding.UTF8.GetBytes(supplied))) throw new UniversalDataException((int)StatusCode.Auth,"Invalid control capability");
                        switch(root.GetProperty("action").GetString()) {
                            case "cancel": execution.Cancel(); break;
                            case "metrics": using(var progress=JsonDocument.Parse(execution.Progress())) value=new {metrics=execution.Metrics,progress=progress.RootElement.Clone()};break;
                            case "snapshot": using(var snapshot = JsonDocument.Parse(filter?.Json(execution.DebugSnapshot()) ?? execution.DebugSnapshot())) value = snapshot.RootElement.Clone(); break;
                            case "debug": execution.DebugControl(root.GetProperty("command").GetRawText()); break;
                            default: throw new ArgumentException("Unknown control action");
                        }
                    } catch(UniversalDataException error) { status = error.Status; }
                      catch(Exception error) when(error is ArgumentException or JsonException or InvalidOperationException) { status = (int)StatusCode.Invalid; }
                    await writer.WriteLineAsync(JsonSerializer.Serialize(new { result = status,value },HostPolicy.JsonOptions));
                } catch(Exception error) when(error is IOException or SocketException or OperationCanceledException) { if (cancellation.IsCancellationRequested) break; }
                finally { if (stream is not null) await stream.DisposeAsync(); }
            }
        } finally { firstPipe?.Dispose(); listener?.Dispose(); if (address is not null && File.Exists(address)) File.Delete(address); }
    }
}
