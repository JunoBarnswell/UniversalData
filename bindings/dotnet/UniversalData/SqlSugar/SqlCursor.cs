using System.Data;
using UniversalData;

namespace SqlSugar;

public sealed class SqlCursor<T> : IAsyncDisposable
{
    private readonly NativeSqlReader reader;
    private readonly Func<IDataRecord, T> materialize;
    private readonly SemaphoreSlim use = new(1, 1);
    private bool disposed;

    internal SqlCursor(NativeSqlReader reader)
    {
        this.reader = reader;
        try
        {
            materialize = typeof(T) == typeof(SqlRow)
                ? record => (T)(object)new SqlRow(Enumerable.Range(0, record.FieldCount).ToDictionary(
                    record.GetName, index => record.IsDBNull(index) ? null : record.GetValue(index),
                    StringComparer.OrdinalIgnoreCase))
                : EntityMaterializerPlan<T>.Create(reader).Read;
        }
        catch { reader.Dispose(); throw; }
    }

    public async ValueTask<Result<T>> ReadAsync(CancellationToken cancellation = default)
    {
        try { await use.WaitAsync(cancellation).ConfigureAwait(false); }
        catch (OperationCanceledException) { return new(StatusCode.Cancelled, default); }
        try
        {
            if (disposed)
                return new(StatusCode.Invalid, default);
            var step = await reader.ReadAsync(cancellation).ConfigureAwait(false);
            return step.IsSuccess ? Result<T>.Success(materialize(reader)) : Result<T>.Failure(step);
        }
        catch (Exception error) when (SqlResult.Expected(error)) { return Result<T>.Failure(SqlResult.Error(error)); }
        finally { use.Release(); }
    }

    public async ValueTask<Result<IReadOnlyList<T>>> ReadBatchAsync(CancellationToken cancellation = default)
    {
        try { await use.WaitAsync(cancellation).ConfigureAwait(false); }
        catch (OperationCanceledException) { return new(StatusCode.Cancelled, null); }
        try
        {
            if (disposed)
                return new(StatusCode.Invalid, null);
            var first = await reader.ReadAsync(cancellation).ConfigureAwait(false);
            if (!first.IsSuccess)
                return Result<IReadOnlyList<T>>.Failure(first);
            var rows = new T[checked(reader.RemainingInBatch + 1)];
            rows[0] = materialize(reader);
            for (int index = 1; index < rows.Length; ++index)
            {
                cancellation.ThrowIfCancellationRequested();
                reader.AdvanceInBatch();
                rows[index] = materialize(reader);
            }
            return Result<IReadOnlyList<T>>.Success(rows);
        }
        catch (Exception error) when (SqlResult.Expected(error))
        { return Result<IReadOnlyList<T>>.Failure(SqlResult.Error(error)); }
        finally { use.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await use.WaitAsync().ConfigureAwait(false);
        try
        {
            if (disposed)
                return;
            disposed = true;
            reader.Dispose();
        }
        finally { use.Release(); }
    }
}
