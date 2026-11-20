using UniversalData;

[assembly: UniversalDataModule(ProductInfo.Sdk, ProductInfo.Protocol)]

namespace ManagedSqlAcceptance;

public sealed class ManagedSqlModule : IManagedModule
{
    public void Register(Registry registry) => registry.RegisterComponent(new ManagedComponentDefinition(
        "managed.uppercase", "Uppercase order label", ComponentCategory.Transform, 1, 1, "{}",
        (_, inputs) => [inputs[0]], () => new ManagedRowTransform(),
        SynchronousInput: 0, ThreadSafe: true, StreamSafe: true));
}
