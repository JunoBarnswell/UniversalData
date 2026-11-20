using System.Runtime.InteropServices;
using SqlSugar;
using UniversalData;

namespace ManagedSqlAcceptance;

internal static class SqlAcceptance
{
    internal static int Execute(SqlSessionHandle session, string sql, params SugarParameter[] parameters)
    {
        using var text = new NativeStrings(sql);
        using var bound = parameters.Length == 0 ? null : SqlBatchBuilder.BuildParameters(SqlNative.Context(session), parameters);
        SqlNative.Check(SqlNative.Execute(session, text[0], bound?.DangerousGetHandle() ?? IntPtr.Zero, out var affected));
        return checked((int)affected);
    }

    internal static T Scalar<T>(SqlSessionHandle session, string sql, params SugarParameter[] parameters)
    {
        using var text = new NativeStrings(sql, "", "");
        using var bound = parameters.Length == 0 ? null : SqlBatchBuilder.BuildParameters(SqlNative.Context(session), parameters);
        var status = SqlNative.ReadOpen(session, text[0], text[1], text[2], bound?.DangerousGetHandle() ?? IntPtr.Zero,
            1, SqlNative.ReadScalar, out var schema);
        using (schema)
            SqlNative.Check(status);
        try
        {
            status = SqlNative.ReadNext(session, out var batch);
            using (batch)
            {
                SqlNative.Check(status);
                SqlNative.Check(SqlNative.BatchGet(batch!, 0, 0, out var value));
                return (T)SqlValueCodec.ReadValue(value, typeof(T), "scalar")!;
            }
        }
        finally { SqlNative.Check(SqlNative.ReadClose(session)); }
    }

    internal static IEnumerable<T[]> ReadBatches<T>(SqlSessionHandle session, string sql, int rows,
        CancellationToken cancellation = default) where T : new()
    {
        cancellation.ThrowIfCancellationRequested();
        var mapping = EntityMapping.For<T>();
        using var text = new NativeStrings(sql, "", mapping.SchemaJson());
        var status = SqlNative.ReadOpen(session, text[0], text[1], text[2], IntPtr.Zero, (uint)rows, SqlNative.ReadTyped, out var schema);
        using (schema)
            SqlNative.Check(status);
        try
        {
            while (true)
            {
                cancellation.ThrowIfCancellationRequested();
                status = SqlNative.ReadNext(session, out var batch);
                using (batch)
                {
                    if ((StatusCode)status == StatusCode.End) yield break;
                    SqlNative.Check(status);
                    var result = new T[SqlNative.BatchRows(batch!)];
                    for (uint row = 0; row < result.Length; row++)
                    {
                        var entity = new T();
                        for (uint column = 0; column < mapping.Members.Count; column++)
                        {
                            var member = mapping.Members[(int)column];
                            SqlNative.Check(SqlNative.BatchGet(batch!, row, column, out var value));
                            member.Set(entity!, SqlValueCodec.ReadValue(value, member.ValueType, member.ColumnName));
                        }
                        result[row] = entity;
                    }
                    yield return result;
                }
            }
        }
        finally { SqlNative.Check(SqlNative.ReadClose(session)); }
    }

    internal static int WriteRows<T>(SqlSessionHandle session, int mode, IEnumerable<T> rows, int chunkRows,
        CancellationToken cancellation = default, bool bulk = false)
    {
        var mapping = EntityMapping.For<T>();
        var columns = mode == SqlNative.WriteDelete ? mapping.Keys : mapping.Members;
        uint[] keys = mode == SqlNative.WriteAppend ? [] : columns.Select((member, index) => (member, index))
            .Where(entry => entry.member.IsPrimaryKey).Select(entry => (uint)entry.index).ToArray();
        using var enumerator = rows.GetEnumerator();
        if (!enumerator.MoveNext()) return 0;
        bool more = true;
        using var table = new NativeStrings(mapping.TableName);
        var context = SqlNative.Context(session);
        using var schema = SqlNative.SchemaFromJson(context, mapping.SchemaJson(columns));
        using var values = new SqlBatchBuilder.PinnedValues(columns.Count);
        var pinned = keys.Length == 0 ? default : GCHandle.Alloc(keys, GCHandleType.Pinned);
        SqlBatchHandle? current = null;
        try
        {
            SqlNative.Check(SqlNative.WriteStream(session, mode, bulk ? SqlNative.LoadAuto : SqlNative.LoadParameters, table[0],
                pinned.IsAllocated ? pinned.AddrOfPinnedObject() : IntPtr.Zero, (uint)keys.Length, (uint)chunkRows,
                Next, IntPtr.Zero, out var count));
            return checked((int)count);
        }
        finally
        {
            current?.Dispose();
            if (pinned.IsAllocated) pinned.Free();
        }

        int Next(IntPtr _, uint capacity, out IntPtr pointer, out int last)
        {
            pointer = IntPtr.Zero;
            last = 0;
            try
            {
                current?.Dispose();
                if (!more) return (int)StatusCode.Protocol;
                current = SqlBatchBuilder.BuildChunk(context, schema, columns, enumerator, values,
                    capacity, cancellation, out more);
                pointer = current.DangerousGetHandle();
                last = more ? 0 : 1;
                return 0;
            }
            catch (UniversalDataException error) { return error.Status; }
            catch (OperationCanceledException) { return (int)StatusCode.Cancelled; }
            catch (OverflowException) { return (int)StatusCode.Overflow; }
            catch (Exception) { return (int)StatusCode.ManagedComponent; }
        }
    }

    internal static void Transaction(SqlSessionHandle session, int operation, int isolation = 0) =>
        SqlNative.Check(SqlNative.Transaction(session, operation, isolation));

    internal static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
