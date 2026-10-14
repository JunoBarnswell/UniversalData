using System.Runtime.InteropServices;
using System.Text.Json;
using UniversalData;

NativeLibrary.SetDllImportResolver(typeof(Execution).Assembly, (name, _, _) =>
    name == "ud_data" ? NativeLibrary.Load(Path.GetFullPath(args[0])) : IntPtr.Zero);
var root = Path.Combine(Path.GetTempPath(), "ud-managed-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
static void Require(bool value, string message) { if (!value) throw new Exception(message); }
static void Throws<T>(Action action) where T : Exception
{ try { action(); } catch (T) { return; } throw new Exception("Expected " + typeof(T).Name); }
static bool Allow(string tenant, string resource, ConnectionAccess access) => true;
static string TaskPackage(string kind, object? extensions = null) => JsonSerializer.Serialize(new {
    version = 10, id = "managed-product", extensions,
    controlFlow = new { tasks = new[] { new { id = "task", kind, config = new { } } }, constraints = Array.Empty<object>() },
    dataFlows = Array.Empty<object>() });
// Leave absent extension lists absent, rather than introducing a null protocol value.
static string PlainPackage(string kind) => TaskPackage(kind).Replace(",\"extensions\":null", "");
try {
    var counts = new Counts();
    using (var registry = new Registry()) {
        registry.RegisterTask(new("managed.task", "Managed task", "{}", () => new CountingTask(counts), ThreadSafe: true));
        using (var execution = new Execution(PlainPackage("managed.task"), "local", Allow, registry: registry,
            observe: value => { Require(value.ExecutionId.Length != 0 && value.Sequence > 0, "Native event identity");
                Require(Math.Abs((long)value.TimestampMilliseconds - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) < 10000, "Persistent events use UTC wall time"); counts.Events++; })) {
            Require(Guid.TryParse(execution.Id, out _), "Native generated execution ID");
            Throws<UniversalDataException>(registry.Dispose);
            GC.Collect(); GC.WaitForPendingFinalizers(); execution.Execute();
            Require(execution.Metrics.ExecutedTasks == 1, "Native executed managed task");
        }
        Require(counts.Executed == 1 && counts.Finished == 1 && counts.Disposed == 1 && counts.Events > 2,
            "Managed task callback ownership and lifecycle");
        Throws<ObjectDisposedException>(() => _ = counts.Borrowed!.TaskId);
    }
    const string empty = "{\"version\":10,\"id\":\"observer\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
    using (var execution = new Execution(empty, "local", Allow, observe: _ => throw new Exception("observer failure"))) {
        Throws<UniversalDataException>(execution.Execute);
        Require(execution.Metrics.Result == StatusCode.Io, "Observer failure reaches Native error lifecycle");
    }
    var source = Path.Combine(root, "source.ndjson"); var destination = Path.Combine(root, "destination.ndjson");
    File.WriteAllText(source, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n{\"id\":5}\n");
    var document = JsonSerializer.Serialize(new {
        version = 10, id = "managed-flow", connections = new[] { new { id = "source", driver = "file", resource = source }, new { id = "target", driver = "file", resource = destination } },
        controlFlow = new { tasks = new[] { new { id = "flow", kind = "dataFlow", config = new { pipeline = "rows" } } }, constraints = Array.Empty<object>() },
        dataFlows = new[] { new { id = "rows", components = new object[] {
            new { id = "read", component = "file.ndjson.source", config = new { connection = "source", schema = new { version = 10, fields = new[] { new { name = "id", type = "int64", nullable = false } } } } },
            new { id = "managed", component = "managed.pass", config = new { } },
            new { id = "write", component = "file.ndjson.destination", config = new { connection = "target" } } },
            paths = new[] { new { from = "read", output = 0, to = "managed", input = 0 }, new { from = "managed", output = 0, to = "write", input = 0 } } } } });
    var transformed = new Counts();
    using (var registry = new Registry()) {
        registry.RegisterComponent(new("managed.pass", "Managed pass", ComponentCategory.Transform, 1, 1, "{}",
            (_, inputs) => [inputs[0]], () => new Pass(transformed), SynchronousInput: 0, ThreadSafe: true));
        using var execution = new Execution(document, "local", Allow, registry: registry, maxBatchRows: 2, maxConcurrency: 4);
        execution.ConfigureDebug("{\"previewRows\":2,\"breakpoints\":[\"/4:flow;\"]}");
        execution.Start();
        static JsonDocument Paused(Execution execution) {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline) {
                var snapshot = JsonDocument.Parse(execution.DebugSnapshot());
                if (snapshot.RootElement.GetProperty("paused").GetBoolean()) return snapshot;
                snapshot.Dispose(); Thread.Sleep(5);
            }
            throw new Exception("Native safe pause timed out");
        }
        using (var paused = Paused(execution)) Require(paused.RootElement.GetProperty("maxConcurrency").GetInt32() == 1, "Debug Native worker budget");
        execution.DebugControl("{\"action\":\"step\"}");
        using (var paused = Paused(execution)) Require(paused.RootElement.GetProperty("preview").GetArrayLength() > 0, "Bounded Native edge preview after step");
        execution.DebugControl("{\"action\":\"continue\"}");
        while (!execution.Wait(100)) { }
        Require(execution.Metrics.State == ExecutionState.Succeeded && File.ReadAllLines(destination).Length == 5, "Managed transform and resumed debug delivery");
        Throws<ObjectDisposedException>(() => _ = transformed.Batch!.Rows);
    }
    Require(transformed.Rows == 5 && transformed.Disposed == 1, "Managed component row ownership");
    using (var registry = new Registry()) {
        using var entered = new ManualResetEventSlim();
        registry.RegisterTask(new("managed.cancel", "Cancelable", "{}", () => new BlockingTask(entered), ThreadSafe: true));
        using var execution = new Execution(PlainPackage("managed.cancel"), "local", Allow, registry: registry);
        execution.Start(); Require(entered.Wait(5000), "Managed task started"); execution.Cancel();
        Throws<UniversalDataException>(() => { while (!execution.Wait(100)) { } });
        Require(execution.Metrics.State == ExecutionState.Cancelled, "Cancellation remains Native");
    }
    const string script = """
        using UniversalData;
        public sealed class Module : IManagedModule {
            public void Register(Registry registry) => registry.RegisterTask(new("script.task", "Compiled script", "{}", () => new Script(), ThreadSafe: true));
        }
        public sealed class Script : IManagedTask {
            public void Execute(ManagedTaskContext context, string json) => context.CheckCancellation();
            public void Dispose() { }
        }
        """;
    var assembly = CSharpDeployment.Compile(script, Path.Combine(root, "assemblies"), "local", Allow);
    var fixedPackage = TaskPackage("script.task", new[] { new { kind = assembly.Kind, path = assembly.Path, sha256 = assembly.Sha256 } });
    using (var execution = new Execution(fixedPackage, "local", Allow)) {
        execution.Execute(); Require(execution.Metrics.ExecutedTasks == 1, "Fixed script assembly uses Native Registry");
    }
    using (var catalog = new Catalog(Path.Combine(root, "catalog"), "local", Allow)) {
        using var execution = new Execution(empty, "local", Allow, observe: catalog.Record);
        execution.Execute();
        var latest = catalog.LatestRevision(CatalogKind.Event, execution.Id);
        Require(latest > 2, "Persistent observer events");
        var removed = catalog.PruneEvents(ulong.MaxValue >> 1);
        Require(removed > 0 && catalog.LatestRevision(CatalogKind.Event, execution.Id) == latest, "Retention preserves event sequence watermark");
    }
    Console.WriteLine("Managed Registry/task/component ownership, expired borrows, GC, observer fault, cancellation, Native debug/step, deployment compilation and Catalog retention passed");
} finally { Directory.Delete(root, recursive: true); }

sealed class Counts { public int Executed, Finished, Disposed, Events, Rows; public ManagedTaskContext? Borrowed; public ManagedBatch? Batch; }
sealed class CountingTask(Counts counts) : IManagedTask {
    public void Execute(ManagedTaskContext context, string json) { context.CheckCancellation(); counts.Borrowed = context; counts.Executed++; GC.Collect(); }
    public void Finish() => counts.Finished++;
    public void Dispose() => counts.Disposed++;
}
sealed class Pass(Counts counts) : IManagedComponent {
    public StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input) {
        if (input.Kind == ComponentEventKind.Poll) return StatusCode.End;
        if (input.Kind != ComponentEventKind.Data) return StatusCode.Ok;
        counts.Batch = input.Batch!; counts.Rows += (int)input.Batch!.Rows; context.Emit(0, input.Batch); GC.Collect(); return StatusCode.Ok;
    }
    public void Dispose() => counts.Disposed++;
}
sealed class BlockingTask(ManualResetEventSlim entered) : IManagedTask {
    public void Execute(ManagedTaskContext context, string json) { entered.Set(); while (true) { context.CheckCancellation(); Thread.Sleep(2); } }
    public void Dispose() { }
}
