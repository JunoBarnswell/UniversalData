import java.nio.file.Files;
import java.nio.file.Path;
import org.universaldata.Execution;
import org.universaldata.Package;
import org.universaldata.UniversalDataException;
public final class HttpConsumer {
    public static void main(String[] args) throws Exception {
        if(args.length!=4)throw new IllegalArgumentException("package.json exact-url ca-file permit|deny|resolver-deny");
        int[] resolutions={0},networkGrants={0};
        try(Package definition=new Package(Files.readString(Path.of(args[0])));
            Execution execution=new Execution(definition.toJson(),"fixture",(tenant,resource,action)-> {
                if(!tenant.equals("fixture"))return false;
                if(resource.equals(args[1]) && (action==1 || action==10)) {
                    if(action==10) { networkGrants[0]++;return !args[3].equals("deny"); }
                    return true;
                }
                return resource.equals(args[2]) && (action==1 || action==3);
            },"{}","http-java",(tenant,resource,reference)-> {
                if(args[3].equals("resolver-deny") || !tenant.equals("fixture") || !resource.equals(args[1]) || !reference.equals("token"))
                    throw new IllegalStateException("Denied credential scope");
                resolutions[0]++;
                return "{\"headers\":{\"Authorization\":\"Bearer fixture-token\"}}";
            },2,4096,268435456L,0,null,false,1)) {
            int status=0;
            try { execution.start();while(!execution.await(100)) { } }
            catch(UniversalDataException error) { status=error.status(); }
            Execution.Metrics m=execution.metrics();
            System.out.printf("{\"result\":%d,\"state\":%d,\"completedTasks\":%d,\"resolutions\":%d,\"networkGrants\":%d}%n",status,m.state().ordinal(),m.completedTasks(),resolutions[0],networkGrants[0]);
        }
    }
}
