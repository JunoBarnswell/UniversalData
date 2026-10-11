using UniversalData;
if (args.Length != 2) throw new ArgumentException("input-directory state-directory required");
const string schema = "team utf8 required\namount int64 nullable\n";
const string commands = "filter amount ge 0\nproject team amount\ngroup team\nsum amount total\ncount * rows\n";
using var job = new CsvJob(args[0], args[1], schema, commands);
job.Poll();
Console.WriteLine(CsvJob.LatestSnapshot(args[1]));
