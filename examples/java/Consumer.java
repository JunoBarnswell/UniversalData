import org.universaldata.CsvJob;
import java.nio.file.Path;
public final class Consumer {
    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("input-directory state-directory required");
        String schema = "ud-schema 2\nteam utf8 required\namount int64 nullable\n";
        String commands = "ud-plan 2\nfilter amount ge 0\nproject team amount\ngroup team\nsum amount total\ncount * rows\n";
        try (var job = new CsvJob(Path.of(args[0]), Path.of(args[1]), schema, commands)) { job.poll(); System.out.println(job.latestSnapshot()); }
    }
}
