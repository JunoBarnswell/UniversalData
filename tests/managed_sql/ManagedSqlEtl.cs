using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using SqlSugar;
using UniversalData;

namespace ManagedSqlAcceptance;

internal static class ManagedSqlEtl
{
    private const string PackageId = "managed-sql-etl";

    internal static void Run(string plugin)
    {
        var directory = Directory.CreateTempSubdirectory("ud-managed-sql-etl-");
        try
        {
            var source = Path.Combine(directory.FullName, "source.db");
            var target = Path.Combine(directory.FullName, "target.db");
            using (var writer = Open(plugin, source))
            {
                writer.Ado.ExecuteCommand("CREATE TABLE etl_rows(id INTEGER NOT NULL PRIMARY KEY,label TEXT NOT NULL)").Require();
                var rows = Enumerable.Range(1, 35)
                    .Select(id => new ManagedEtlRow { Id = id, Label = $"数据-{id}-order" })
                    .ToList();
                Require(writer.Insertable(rows).ExecuteCommand().Require() == rows.Count, "Source preparation");
            }
            using (var writer = Open(plugin, target))
                writer.Ado.ExecuteCommand("CREATE TABLE etl_rows(id INTEGER NOT NULL PRIMARY KEY,label TEXT NOT NULL)").Require();

            var sourceReadable = true;
            var schema = Discover(plugin, source, Authorize, Resolve);
            var document = Definition(plugin, source, target, schema);
            using (var package = new NativePackage(document))
                Require(!package.RequiresJournal, "Live SQL work must use response delivery");
            VerifyPreview(document, Authorize, Resolve);
            using (var before = Open(plugin, target))
                Require(Convert.ToInt64(before.Ado.GetScalar("SELECT COUNT(*) FROM etl_rows").Require(),
                    CultureInfo.InvariantCulture) == 0, "Preview left the destination empty");
            using var registry = new Registry();
            new ManagedSqlModule().Register(registry);
            using (var execution = new Execution(document, "sql-etl", Authorize, resolve: Resolve,
                       registry: registry, maxBatchRows: 4))
            {
                using (var hosted = execution.OpenSqlClient("source"))
                {
                    Require(hosted.CurrentConnectionConfig.DbType == DbType.Sqlite, "Hosted driver identification");
                    Require(Convert.ToInt64(hosted.Ado.GetScalar("SELECT COUNT(*) FROM etl_rows").Require(),
                        CultureInfo.InvariantCulture) == 35, "Hosted authorized read");
                    try
                    {
                        hosted.Ado.ExecuteCommand("DELETE FROM etl_rows WHERE id=1").Require();
                        throw new InvalidOperationException("The source write should have been denied.");
                    }
                    catch (UniversalDataException denied) when (denied.Code == StatusCode.Auth) { }

                    sourceReadable = false;
                    try
                    {
                        hosted.Ado.GetScalar("SELECT COUNT(*) FROM etl_rows").Require();
                        throw new InvalidOperationException("The revoked read should have been denied.");
                    }
                    catch (UniversalDataException denied) when (denied.Code == StatusCode.Auth) { }
                    finally { sourceReadable = true; }
                    Require(Convert.ToInt64(hosted.Ado.GetScalar("SELECT COUNT(*) FROM etl_rows").Require(),
                        CultureInfo.InvariantCulture) == 35, "Read after restoring the host grant");
                }
                execution.Execute();
                Require(execution.Metrics.State == ExecutionState.Succeeded && execution.Metrics.CommittedRows == 35,
                    "C source, managed transform and C target delivery");
            }
            using var reader = Open(plugin, target);
            var result = reader.Queryable<ManagedEtlRow>().OrderBy(row => row.Id).ToList().Require();
            Require(result.Select(row => (row.Id, row.Label)).SequenceEqual(
                Enumerable.Range(1, 35).Select(id => ((long)id, $"数据-{id}-ORDER"))), "Target readback");
            Console.WriteLine("PASS SQL Source, managed typed transform, SQL Destination, 35 rows and identities");

            bool Authorize(string tenant, string resource, ConnectionAccess action) =>
                tenant == "sql-etl" &&
                ((resource == plugin && action is ConnectionAccess.Open or ConnectionAccess.Execute) ||
                 (resource == source && (action is ConnectionAccess.Open or ConnectionAccess.Discover ||
                                         (action == ConnectionAccess.Read && sourceReadable))) ||
                 (resource == target && action is ConnectionAccess.Open or ConnectionAccess.Discover or ConnectionAccess.Read or ConnectionAccess.Write));

            string Resolve(string tenant, string resource, string reference) =>
                tenant == "sql-etl" && ((resource == source && reference == "source") ||
                                        (resource == target && reference == "target"))
                    ? resource
                    : throw new InvalidOperationException("Unexpected isolated connection reference.");
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static SqlSugarClient Open(string plugin, string file) => SqlAcceptance.OpenClient(new ConnectionConfig
    {
        DbType = DbType.Sqlite,
        ConnectionString = "Data Source=" + file,
        PluginPath = plugin
    });

    private static void VerifyPreview(string document, PackageAuthorizer authorize,
        PackageCredentialResolver resolve)
    {
        var design = JsonNode.Parse(document)!;
        var flow = design["dataFlows"]![0]!;
        var nodes = flow["components"]!.AsArray();
        nodes.RemoveAt(1);
        flow["paths"] = new JsonArray(new JsonObject
        {
            ["from"] = "read", ["output"] = 0, ["to"] = "write", ["input"] = 0
        });
        using var preview = new Execution(design.ToJsonString(), "sql-etl", authorize, resolve: resolve,
            maxBatchRows: 4);
        using var facts = JsonDocument.Parse(preview.Preview(flow.ToJsonString(), "read",
            sourceRows: 3, sampleRows: 3));
        var result = facts.RootElement;
        var samples = result.GetProperty("snapshot").GetProperty("preview");
        var rows = samples.EnumerateArray().Where(sample => sample.GetProperty("component").GetString() == "read")
            .Sum(sample => sample.GetProperty("totalRows").GetInt32());
        var status = result.GetProperty("result").GetInt32();
        Require(status == 0 && rows == 3, $"Bounded Native SQL preview status {status}, rows {rows}. " +
            result.GetProperty("diagnostics").GetProperty("schemas").GetRawText());
    }

    private static JsonElement Discover(string plugin, string source, PackageAuthorizer authorize,
        PackageCredentialResolver resolve)
    {
        var document = JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id = PackageId,
            connections = new[]
            {
                new { id = "source", driver = plugin, resource = source, credentialReference = "source" }
            },
            controlFlow = new { tasks = Array.Empty<object>(), constraints = Array.Empty<object>() },
            dataFlows = Array.Empty<object>()
        });
        using var execution = new Execution(document, "sql-etl", authorize, resolve: resolve);
        using var schema = JsonDocument.Parse(execution.DiscoverSchema("source", "etl_rows"));
        return schema.RootElement.Clone();
    }

    private static string Definition(string plugin, string source, string target, JsonElement schema)
    {
        return JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id = PackageId,
            connections = new[]
            {
                new { id = "source", driver = plugin, resource = source, credentialReference = "source" },
                new { id = "target", driver = plugin, resource = target, credentialReference = "target" }
            },
            controlFlow = new
            {
                tasks = new[] { new { id = "copy", kind = "dataFlow", config = new { pipeline = "rows" } } },
                constraints = Array.Empty<object>()
            },
            dataFlows = new[]
            {
                new
                {
                    id = "rows",
                    components = new object[]
                    {
                        new { id = "read", component = "sql.source", config = new { connection = "source", table = "etl_rows", readMode = "stream", batchSize = 4, schema } },
                        new { id = "transform", component = "managed.uppercase", config = new { } },
                        new { id = "write", component = "sql.destination", config = new { connection = "target", table = "etl_rows", mode = "append", delivery = "response" } }
                    },
                    paths = new[]
                    {
                        new { from = "read", output = 0, to = "transform", input = 0 },
                        new { from = "transform", output = 0, to = "write", input = 0 }
                    }
                }
            }
        });
    }

    private static void Require(bool result, string context)
    {
        if (!result)
            throw new InvalidOperationException($"Managed SQL ETL validation failed. {context}");
    }
}
