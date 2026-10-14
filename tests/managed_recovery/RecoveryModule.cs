using UniversalData;

[assembly: UniversalDataModule(ProductInfo.Sdk, ProductInfo.Protocol)]

/// <summary>The same actual module is exercised through SDK registration and the product Worker loader.</summary>
public sealed class RecoveryModule : IManagedModule
{
    public void Register(Registry registry)
    {
        registry.RegisterComponent(new("managed.source","Durable managed Source",ComponentCategory.Source,0,1,"{}",
            (_,_) => [Source.Schema],() => new Source(""),ThreadSafe:true,Stateful:true,StateVersion:1,ReplaySafe:true,Acknowledgement:SourceAcknowledgement.Epoch));
        registry.RegisterComponent(new("managed.pass","Managed control-aware transform",ComponentCategory.Transform,1,1,"{}",
            (_,input) => [input[0]],() => new Pass(),SynchronousInput:0,ThreadSafe:true,ReplaySafe:true,
            BindPorts:_ => new([new("input")],[new("rows")]),
            DescribePlan:(_,_,_) => new(EstimatedRows:12,RowWidth:8,EstimatedCost:96)));
        registry.RegisterComponent(new("managed.destination","Idempotent managed Destination",ComponentCategory.Destination,1,0,"{}",
            (_,_) => [],() => new Destination(""),ThreadSafe:true,Stateful:true,StateVersion:1,ReplaySafe:true,
            Effects:ComponentEffects.ExternalWrite | ComponentEffects.Idempotent));
    }
}
