using SqlSugar;
using UniversalData;

namespace ManagedSqlAcceptance;

internal static class SqlAcceptance
{
    internal static T Require<T>(this Result<T> result)
    {
        Require(new Result(result.Code, result.Error));
        return result.Value!;
    }

    internal static void Require(this Result result)
    {
        if (result.Code == StatusCode.Cancelled)
            throw new OperationCanceledException();
        if (!result.IsSuccess)
            throw new UniversalDataException((int)result.Code, result.Error?.Message ?? result.Code.ToString(),
                result.Error);
    }

    internal static SqlSugarClient OpenClient(ConnectionConfig config) =>
        SqlSugarClient.OpenAsync(config).GetAwaiter().GetResult().Require();

    internal static IEnumerable<T> ToStream<T>(this ISugarQueryable<T> query, CancellationToken cancellation = default)
    {
        var cursor = query.OpenCursorAsync(cancellation: cancellation).GetAwaiter().GetResult().Require();
        try
        {
            while (true)
            {
                var result = cursor.ReadAsync(cancellation).GetAwaiter().GetResult();
                if (result.Code == StatusCode.End)
                    yield break;
                yield return result.Require();
            }
        }
        finally { cursor.DisposeAsync().GetAwaiter().GetResult(); }
    }

    internal static IEnumerable<IReadOnlyList<T>> ToBatches<T>(this ISugarQueryable<T> query, int size)
    {
        var cursor = query.OpenCursorAsync(size).GetAwaiter().GetResult().Require();
        try
        {
            while (true)
            {
                var result = cursor.ReadBatchAsync().GetAwaiter().GetResult();
                if (result.Code == StatusCode.End)
                    yield break;
                yield return result.Require();
            }
        }
        finally { cursor.DisposeAsync().GetAwaiter().GetResult(); }
    }
}
