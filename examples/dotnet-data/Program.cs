using UniversalData;

if (args.Length != 6) throw new ArgumentException("schema.json data.json dag.json parameters.json output.json output.wire");
using var batch = new Batch(File.ReadAllText(args[0]), File.ReadAllText(args[1]));
File.WriteAllText(args[4], batch.ToJson());
File.WriteAllBytes(args[5], batch.ToWire());
using var roundtrip = Batch.FromWire(batch.ToWire());
if (batch.ToJson() != roundtrip.ToJson()) throw new InvalidOperationException("Typed wire roundtrip differs");
using var dag = new Dag(File.ReadAllText(args[2]));
using var bound = dag.Bind(File.ReadAllText(args[3]));
File.WriteAllText(args[4] + ".dag", bound.ToJson());
Console.WriteLine($"Batch rows={batch.Rows}, DAG nodes={dag.NodeCount}; contract consumption only");
