import java.nio.file.Files;
import java.nio.file.Path;
import org.universaldata.Batch;
public final class DataConsumer {
    public static void main(String[] args) throws Exception {
        try(Batch batch=new Batch(Files.readString(Path.of(args[0])),Files.readString(Path.of(args[1])))) {
            Files.writeString(Path.of(args[2]),batch.toJson());Files.write(Path.of(args[3]),batch.toWire());
        }
    }
}
