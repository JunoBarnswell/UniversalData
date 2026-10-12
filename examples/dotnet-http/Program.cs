using UniversalData;
using System.Text.Json;
if(args.Length!=4)throw new ArgumentException("package.json exact-url ca-file permit|deny|resolver-deny");
int resolutions=0,networkGrants=0;
using var package=new Package(File.ReadAllText(args[0]));
using var execution=package.CreateExecution("fixture",(tenant,resource,access)=>
{
    if(tenant!="fixture")return false;
    if(resource==args[1] && (access==ConnectionAccess.Open || access==ConnectionAccess.Network))
    {
        if(access==ConnectionAccess.Network) { networkGrants++;return args[3]!="deny"; }
        return true;
    }
    return resource==args[2] && (access==ConnectionAccess.Open || access==ConnectionAccess.Read);
},executionId:"http-dotnet",resolve:(tenant,resource,reference)=>
{
    if(args[3]=="resolver-deny" || tenant!="fixture" || resource!=args[1] || reference!="token")
        throw new InvalidOperationException("Denied credential scope");
    resolutions++;
    return "{\"headers\":{\"Authorization\":\"Bearer fixture-token\"}}";
});
int status=0;
try { execution.Start();while(!execution.Wait(100)) { } }
catch(UniversalDataException error) { status=error.Status; }
var m=execution.Metrics;
Console.WriteLine(JsonSerializer.Serialize(new { result=status,state=(int)m.State,completedTasks=m.CompletedTasks,resolutions,networkGrants }));
