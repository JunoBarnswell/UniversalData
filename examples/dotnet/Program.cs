using System.Text.Json;
using UniversalData;

if (args.Length != 4) throw new ArgumentException("package.json parameters.json execution-id authorized-root");
string root = Path.GetFullPath(args[3]).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
using var package = new Package(File.ReadAllText(args[0]));
using var execution = package.CreateExecution("fixture", (_, resource, _) => Path.IsPathFullyQualified(resource) && Path.GetFullPath(resource).StartsWith(root, StringComparison.Ordinal), File.ReadAllText(args[1]), args[2]);
execution.Start(); while (!execution.Wait(100)) { }
var m = execution.Metrics;
Console.WriteLine(JsonSerializer.Serialize(new { state = (int)m.State, result = (int)m.Result, inputRows = m.InputRows, outputRows = m.OutputRows, rejectedRows = m.RejectedRows, completedTasks = m.CompletedTasks }));