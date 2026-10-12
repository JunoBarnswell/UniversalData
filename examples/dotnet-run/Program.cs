using UniversalData;

if (args.Length != 4) throw new ArgumentException("dag.json parameters.json metrics.json expected-status");
string root = Path.GetDirectoryName(Path.GetFullPath(args[0]))! + Path.DirectorySeparatorChar;
int expected = int.Parse(args[3]), actual = 0;
try
{
    using var run = new Run(File.ReadAllText(args[0]), "tenant1",
        (tenant, resource, access) => tenant == "tenant1" && Path.GetFullPath(resource).StartsWith(root, StringComparison.Ordinal),
        File.ReadAllText(args[1]), edgeCapacity: 2, maxBatchRows: 32, memoryLimit: 524288);
    run.Start(); while (!run.Wait(100)) { }
    var m = run.Metrics;
    File.WriteAllText(args[2], System.Text.Json.JsonSerializer.Serialize(new
    {
        state = (int)m.State, result = (int)m.Result, inputRows = m.InputRows, outputRows = m.OutputRows,
        queueHighWater = m.QueueHighWater
    }));
}
catch (UniversalDataException exception) { actual = exception.Status; }
if (actual != expected) throw new InvalidOperationException($"Actual status {actual}, expected {expected}");
Console.WriteLine($".NET DAG execution status={actual}");
