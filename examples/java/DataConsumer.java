import java.nio.file.Files;
import java.nio.file.Path;
import org.universaldata.Batch;
import org.universaldata.Schema;
import org.universaldata.Execution;
public final class DataConsumer {
    public static void main(String[] args) throws Exception {
        if (args.length == 5 && args[0].equals("--compare")) {
            Files.writeString(Path.of(args[4]),Schema.compare(Files.readString(Path.of(args[1])),Files.readString(Path.of(args[2])),Files.readString(Path.of(args[3]))));
            return;
        }
        if (args.length == 3 && args[0].equals("--row-info")) {
            try(Batch batch=Batch.fromWire(Files.readAllBytes(Path.of(args[1])))) {
                Files.writeString(Path.of(args[2]),batch.rowInfoJson(0));
            }
            return;
        }
        if (args.length == 5 && args[0].equals("--discover")) {
            try(Execution query=new Execution(Files.readString(Path.of(args[1])),"fixture",(t,r,a)->true,"{}","",
                (t,r,ref)->ref.equals("fixture")?r:null,2,4096,268435456L,0,null,false,1)) {
                Files.writeString(Path.of(args[4]),query.discoverSchema(args[2],args[3]));
            }
            return;
        }
        try(Batch batch=new Batch(Files.readString(Path.of(args[0])),Files.readString(Path.of(args[1])))) {
            Files.writeString(Path.of(args[2]),batch.toJson());Files.write(Path.of(args[3]),batch.toWire());
        }
    }
}
