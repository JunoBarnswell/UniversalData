using UniversalData;

[assembly: UniversalDataModule(ProductInfo.Sdk, ProductInfo.Protocol)]

namespace FluentAcceptance;

public sealed class MqBurstModule : IManagedModule
{
    internal const string Component = "demo.mq-business-delay";

    public void Register(Registry registry) => registry.RegisterComponent(new(Component, "Simulated MQ business work",
        ComponentCategory.Transform, 1, 1,
        """{"type":"object","properties":{"timingFile":{"type":"string"}},"required":["timingFile"],"additionalProperties":false}""",
        (_, inputs) => [inputs[0]], () => new MqBurstDelay(), SynchronousInput: 0,
        ThreadSafe: true, ReplaySafe: true, HandlesControlEvents: false, StreamSafe: true));
}
