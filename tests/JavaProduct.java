import org.universaldata.Catalog;
import org.universaldata.Execution;
import org.universaldata.Registry;
import org.universaldata.UniversalDataException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class JavaProduct {
    static final String EMPTY="{\"version\":10,\"id\":\"java-product\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
    static String task(String kind) { return EMPTY.replace("\"tasks\":[]", "\"tasks\":[{\"id\":\"task\",\"kind\":\""+kind+"\",\"config\":{}}]"); }
    static void require(boolean value) { if(!value)throw new AssertionError(); }
    static void status(int expected,Runnable action) {
        try { action.run(); } catch(UniversalDataException failure) { require(failure.status()==expected);return; }
        throw new AssertionError("Expected Native status "+expected);
    }
    static Execution execution(String packageJson,Registry registry,Execution.Observer observer) {
        return new Execution(packageJson,"local",(tenant,resource,action)->true,"{}","",null,2,4,
                             67108864,0,null,false,4,registry,observer,0);
    }
    public static void main(String[] args) throws Exception {
        AtomicInteger calls=new AtomicInteger(),finished=new AtomicInteger(),closed=new AtomicInteger(),events=new AtomicInteger();
        Registry.TaskContext[] borrowed=new Registry.TaskContext[1];
        try(Registry registry=new Registry()) {
            registry.registerTask(new Registry.TaskDefinition("java.task","Java task",null,"{}",false,true,0,
                (context,json)->new Registry.Task() {
                    public void invoke(Registry.TaskContext active,String configuration) { active.check();borrowed[0]=active;calls.incrementAndGet();System.gc(); }
                    public void finish() { finished.incrementAndGet(); }
                    public void close() { closed.incrementAndGet(); }
                }));
            try(Execution execution=execution(task("java.task"),registry,event->{
                require(!event.executionId().isEmpty() && event.sequence()!=0);events.incrementAndGet();
            })) {
                require(java.util.UUID.fromString(execution.id()).version()==4);
                status(11,registry::close);System.gc();execution.execute();require(execution.metrics().executedTasks()==1);
            }
            require(calls.get()==1 && finished.get()==1 && closed.get()==1 && events.get()>2);
            try { borrowed[0].check();throw new AssertionError(); }catch(IllegalStateException expected) { }
        }
        try(Execution execution=execution(EMPTY,null,event->{throw new IllegalArgumentException("observer failure");})) {
            status(3,execution::execute);require(execution.metrics().result().number()==3);
        }
        try(Registry registry=new Registry()) {
            CountDownLatch entered=new CountDownLatch(1);
            registry.registerTask(new Registry.TaskDefinition("java.cancel","Cancelable",null,"{}",false,true,0,
                (context,json)->(active,configuration)->{ entered.countDown();while(true) { active.check();Thread.sleep(2); } }));
            try(Execution execution=execution(task("java.cancel"),registry,null)) {
                execution.start();require(entered.await(5,TimeUnit.SECONDS));execution.cancel();
                status(8,()-> { while(!execution.await(100)) { } });
            }
        }
        Path root=Files.createTempDirectory("ud-java-catalog-");
        try(Catalog catalog=new Catalog(root.toString(),"local",(tenant,resource,action)->true)) {
            catalog.put(Catalog.Kind.PACKAGE,"java-product",-1L,EMPTY);
            require(catalog.latestRevision(Catalog.Kind.PACKAGE,"java-product")==-1L);
            require(catalog.get(Catalog.Kind.PACKAGE,"java-product",-1L).contains("java-product"));
            require(catalog.query("{\"kind\":2,\"id\":\"java-product\"}").contains("18446744073709551615"));
            Catalog.Lease lease=catalog.tryAcquireLease(Catalog.Kind.AUDIT,"run/sdk","java-node",30000,"sdk-pool",1);
            require(lease!=null && lease.generation()==1 && catalog.leaseActive(Catalog.Kind.AUDIT,"run/sdk"));
            require(catalog.tryAcquireLease(Catalog.Kind.AUDIT,"run/sdk","other-node",30000,"sdk-pool",1)==null);
            catalog.compareExchange(Catalog.Kind.AUDIT,"run/sdk",0,"{\"version\":10,\"id\":\"run/sdk\",\"state\":\"Queued\"}",lease);
            status(17,()->catalog.compareExchange(Catalog.Kind.AUDIT,"run/sdk",0,"{\"version\":10,\"id\":\"run/sdk\"}",lease));
            catalog.renewLease(lease,30000);catalog.releaseLease(lease);
            require(!catalog.leaseActive(Catalog.Kind.AUDIT,"run/sdk"));
            status(17,()->catalog.renewLease(lease,30000));
            try(Execution execution=execution(EMPTY,null,catalog::record)) {
                execution.execute();require(catalog.latestRevision(Catalog.Kind.EVENT,execution.id())>2);
                long latest=catalog.latestRevision(Catalog.Kind.EVENT,execution.id());
                require(catalog.pruneEvents(Long.MAX_VALUE,1000)>0);
                require(catalog.latestRevision(Catalog.Kind.EVENT,execution.id())==latest);
            }
        } finally {
            try(var paths=Files.walk(root)) { for(Path path:paths.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path); }
        }
        System.out.println("Java Registry/task ownership, expired borrows, GC, observer fault, cancellation and unsigned Catalog revisions passed");
    }
}
