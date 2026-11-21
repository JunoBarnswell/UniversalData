using UniversalData;

namespace ManagedSqlAcceptance;

internal sealed class ManagedRowTransform : IManagedComponent
{
    public StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input)
    {
        if (input.Kind == ComponentEventKind.Poll)
            return StatusCode.End;
        if (input.Batch is not { } source)
            return StatusCode.Ok;

        var rows = source.ToEntities<ManagedEtlRow>();
        foreach (var row in rows)
            row.Label = row.Label.ToUpperInvariant();
        using var output = context.CreateBatch(rows);
        output.CopyRowIdentitiesFrom(source);
        for (uint row = 0; row < output.Rows; row++)
            if (output.RowIdentity(row) != source.RowIdentity(row))
                throw new InvalidOperationException("The managed transform changed the Native row identity.");
        context.Emit(0, output);
        return StatusCode.Ok;
    }

    public void Dispose() { }
}
