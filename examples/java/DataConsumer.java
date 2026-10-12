import org.universaldata.Batch;
import org.universaldata.Dag;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class DataConsumer {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("schema.json data.json dag.json parameters.json output.json output.wire");
        try (var batch = new Batch(Files.readString(Path.of(args[0])), Files.readString(Path.of(args[1])));
             var roundtrip = Batch.fromWire(batch.toWire());
             var dag = new Dag(Files.readString(Path.of(args[2])));
             var bound = dag.bind(Files.readString(Path.of(args[3])))) {
            if (!batch.toJson().equals(roundtrip.toJson())) throw new IllegalStateException("Typed wire roundtrip differs");
            Files.writeString(Path.of(args[4]), batch.toJson(), StandardCharsets.UTF_8);
            Files.write(Path.of(args[5]), batch.toWire());
            Files.writeString(Path.of(args[4] + ".dag"), bound.toJson(), StandardCharsets.UTF_8);
            System.out.println("Batch rows=" + batch.rows() + ", DAG nodes=" + dag.nodeCount() + "; contract consumption only");
        }
    }
}
