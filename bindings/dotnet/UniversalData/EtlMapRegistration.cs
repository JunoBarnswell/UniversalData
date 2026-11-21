using System.Reflection;
using System.Text.Json;

namespace UniversalData;

internal static class EtlMapRegistration
{
    internal const string Component = "sdk.map";
    private const string Configuration = """
        {"type":"object","properties":{"method":{"type":"object"}},"required":["method"],"additionalProperties":false}
        """;

    internal static bool UsedBy(JsonElement package)
    {
        return package.TryGetProperty("dataFlows", out var flows) && flows.EnumerateArray().Any(flow =>
            flow.GetProperty("components").EnumerateArray().Any(node => node.GetProperty("component").GetString() == Component));
    }

    internal static void Register(Registry registry, Func<SdkMethod, MethodInfo> resolve)
    {
        registry.RegisterComponent(new ManagedComponentDefinition(Component, "C# mapped rows", ComponentCategory.Transform,
            1, 1, Configuration, (configuration, _) => [EntityMapping.For(Resolve(configuration, resolve).ReturnType).SchemaJson()],
            () => new MapComponent(resolve), SynchronousInput: 0, ThreadSafe: true, ReplaySafe: true,
            HandlesControlEvents: false, StreamSafe: true));
    }

    private static MethodInfo Resolve(string configuration, Func<SdkMethod, MethodInfo> resolve)
    {
        using var json = JsonDocument.Parse(configuration);
        var method = json.RootElement.GetProperty("method").Deserialize<SdkMethod>(WorkerProtocol.Json)
            ?? throw new ArgumentException("A registered C# mapping method is required");
        var entry = resolve(method);
        if (entry.GetParameters().Length != 1 || entry.ReturnType == typeof(void) || entry.ReturnType.IsByRef ||
            entry.GetParameters()[0].ParameterType.IsByRef)
            throw new ArgumentException("A C# mapping method must accept one row and return one row");
        return entry;
    }

    private sealed class MapComponent(Func<SdkMethod, MethodInfo> resolve) : IManagedComponent
    {
        private IMapBatch? body;

        public void Open(ManagedComponentContext context, string configuration, string[] inputs, string[] outputs)
        {
            var entry = Resolve(configuration, resolve);
            var type = typeof(MapBatch<,>).MakeGenericType(entry.GetParameters()[0].ParameterType, entry.ReturnType);
            body = (IMapBatch)Activator.CreateInstance(type, entry, inputs[0], outputs[0])!;
        }

        public StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input)
        {
            if (input.Kind == ComponentEventKind.Poll) return StatusCode.End;
            if (input.Batch is not { } batch) return StatusCode.Ok;
            context.CheckCancellation();
            using var output = body!.Map(context, batch);
            output.CopyRowIdentitiesFrom(batch);
            context.Emit(0, output);
            return StatusCode.Ok;
        }

        public void Dispose() { }
    }

    private interface IMapBatch
    {
        ManagedBatch Map(ManagedComponentContext context, ManagedBatch input);
    }

    private sealed class MapBatch<TInput, TOutput> : IMapBatch
    {
        private readonly Func<TInput, TOutput> map;
        private readonly IReadOnlyList<(EntityMember Member, uint Ordinal)> inputProjection;
        private readonly byte[] inputFingerprint;
        private readonly IReadOnlyList<(EntityMember Member, int? Precision, int? Scale)> outputProjection;
        private readonly byte[] outputFingerprint;

        public MapBatch(MethodInfo method, string inputSchema, string outputSchema)
        {
            map = method.CreateDelegate<Func<TInput, TOutput>>();
            inputProjection = ManagedBatch.BuildEntityProjection<TInput>(inputSchema);
            inputFingerprint = ManagedBatch.SchemaFingerprint(inputSchema);
            outputProjection = ManagedBatch.BuildOutputProjection<TOutput>(outputSchema);
            outputFingerprint = ManagedBatch.SchemaFingerprint(outputSchema);
        }

        public ManagedBatch Map(ManagedComponentContext context, ManagedBatch input)
        {
            var entities = input.HasSchemaFingerprint(inputFingerprint)
                ? input.EnumerateEntities<TInput>(inputProjection)
                : input.EnumerateEntities<TInput>();
            var mapped = entities.Select(map);
            return context.OutputSchemaMatches(outputFingerprint)
                ? context.CreateBatch(mapped, outputProjection)
                : context.CreateBatch(mapped);
        }
    }
}
