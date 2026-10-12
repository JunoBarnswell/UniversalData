import org.universaldata.Run;
import org.universaldata.UniversalDataException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RunConsumer {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("dag.json parameters.json metrics.json expected-status");
        Path root = Path.of(args[0]).toAbsolutePath().normalize().getParent();
        int actual = 0, expected = Integer.parseInt(args[3]);
        try (var run = new Run(Files.readString(Path.of(args[0])), "tenant1",
                (tenant, resource, access) -> tenant.equals("tenant1") && Path.of(resource).toAbsolutePath().normalize().startsWith(root),
                Files.readString(Path.of(args[1])), 2, 32, 524288)) {
            run.start(); while (!run.await(100)) { }
            var m = run.metrics();
            Files.writeString(Path.of(args[2]), "{\"state\":" + m.state() + ",\"result\":" + m.result().number()
                + ",\"inputRows\":" + m.inputRows() + ",\"outputRows\":" + m.outputRows()
                + ",\"queueHighWater\":" + m.queueHighWater() + "}");
        } catch (UniversalDataException exception) { actual = exception.status(); }
        if (actual != expected) throw new IllegalStateException("Actual status " + actual + ", expected " + expected);
        System.out.println("Java DAG execution status=" + actual);
    }
}
