import java.nio.file.Files;
import java.nio.file.Path;
import org.universaldata.Execution;
import org.universaldata.Package;
public final class Consumer {
    public static void main(String[] args) throws Exception {
        if(args.length!=4)throw new IllegalArgumentException("package.json parameters.json execution-id authorized-root");
        Path root=Path.of(args[3]).toAbsolutePath().normalize();
        try(Package definition=new Package(Files.readString(Path.of(args[0])));
            Execution execution=definition.execution("fixture",(tenant,resource,action)->Path.of(resource).isAbsolute()&&Path.of(resource).normalize().startsWith(root),Files.readString(Path.of(args[1])),args[2])) {
            execution.start();while(!execution.await(100)) { }
            Execution.Metrics m=execution.metrics();
            System.out.printf("{\"state\":%d,\"result\":%d,\"inputRows\":%d,\"outputRows\":%d,\"rejectedRows\":%d,\"completedTasks\":%d}%n",m.state().ordinal(),m.result().number(),m.inputRows(),m.outputRows(),m.rejectedRows(),m.completedTasks());
        }
    }
}
