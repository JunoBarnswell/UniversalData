using UniversalData;

if (args.Length == 5 && args[0] == "--compare")
{
    File.WriteAllText(args[4], Schema.Compare(File.ReadAllText(args[1]), File.ReadAllText(args[2]), File.ReadAllText(args[3])));
    return;
}
if (args.Length == 3 && args[0] == "--row-info")
{
    using var restored = Batch.FromWire(File.ReadAllBytes(args[1]));
    File.WriteAllText(args[2], restored.RowInfoJson(0));
    return;
}
if (args.Length == 5 && args[0] == "--discover")
{
    using var query = new Execution(File.ReadAllText(args[1]), "fixture", (_, _, _) => true,
        resolve: (_, resource, reference) => reference == "fixture" ? resource : throw new UnauthorizedAccessException());
    File.WriteAllText(args[4], query.DiscoverSchema(args[2], args[3]));
    return;
}
if (args.Length != 4) throw new ArgumentException("schema.json data.json output.json output.wire");
using var batch = new Batch(File.ReadAllText(args[0]), File.ReadAllText(args[1]));
File.WriteAllText(args[2], batch.ToJson()); File.WriteAllBytes(args[3], batch.ToWire());
