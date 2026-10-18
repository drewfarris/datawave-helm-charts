package local.floci;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.*;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;

/** Deterministic, lab-only storage lifecycle and recovery probe. */
public final class StorageSemantics {
    static final String S3 = "s3a://dwv-accumulo-lab/accumulo";
    static String row(int n) { return String.format("row%06d", n); }
    static String value(int n) { return String.format("value%06d:", n) + "abcdefgh01234567".repeat(64); }
    static void require(boolean ok, String message) { if (!ok) throw new AssertionError(message); }
    static String hex(byte[] bytes) { StringBuilder s=new StringBuilder(); for(byte b:bytes)s.append(String.format("%02x",b&255)); return s.toString(); }
    public static void main(String[] args) throws Exception {
        String action=args[0], table=args[1];
        require(table.startsWith("phase4_"), "Only phase4_ lab tables are permitted");
        Properties props=new Properties();
        try(var in=java.nio.file.Files.newInputStream(java.nio.file.Paths.get("/opt/accumulo/conf/accumulo-client.properties"))){props.load(in);}
        props.setProperty("rpc.timeout", "10s");
        props.setProperty("auth.type", "password");
        props.setProperty("auth.principal", "root");
        require("floci-lab".equals(props.getProperty("instance.name")), "Only isolated lab is permitted");
        // Waiting forbids eventual scans silently falling back to tablet servers.
        props.setProperty("scan.server.selector.opts.profiles", "[{\"isDefault\":true,\"maxBusyTimeout\":\"5m\",\"busyTimeoutMultiplier\":8,\"timeToWaitForScanServers\":\"1h\",\"attemptPlans\":[{\"servers\":\"100%\",\"busyTimeout\":\"1s\"}]}]");
        try(AccumuloClient c=Accumulo.newClient().from(props).build()) {
            TableOperations t=c.tableOperations();
            switch(action) {
            case "create":
                t.create(table); t.setProperty(table,"table.custom.volume.preferred",S3);
                t.setProperty(table,"table.compaction.major.ratio","1000");
                System.out.println("CREATED id="+t.tableIdMap().get(table)); break;
            case "create-default":
                t.create(table);
                require(!t.getTableProperties(table).containsKey("table.custom.volume.preferred"),"Unexpected table override");
                require(S3.equals(c.instanceOperations().getSystemConfiguration().get("general.custom.volume.preferred.default")),"User default was not promoted");
                System.out.println("CREATED_DEFAULT id="+t.tableIdMap().get(table)+" explicitVolumeOverride=false default="+S3);break;
            case "write":
                int start=Integer.parseInt(args[2]), count=Integer.parseInt(args[3]);
                try(BatchWriter w=c.createBatchWriter(table,new BatchWriterConfig().setDurability(Durability.SYNC).setTimeout(60,TimeUnit.SECONDS))) {
                    for(int i=start;i<start+count;i++){Mutation m=new Mutation(row(i));m.put("cf","cq",value(i));w.addMutation(m);}
                }
                System.out.println("SYNC_WAL_ACKNOWLEDGED start="+start+" count="+count); break;
            case "flush": t.flush(table,null,null,true);System.out.println("FLUSHED");break;
            case "compact": t.compact(table,new CompactionConfig().setFlush(true).setWait(true));System.out.println("COMPACTED");break;
            case "external": t.setProperty(table,"table.compaction.dispatcher.opts.service","phase4");System.out.println("EXTERNAL_POLICY");break;
            case "split":
                TreeSet<Text> splits=new TreeSet<>(); for(int i=500;i<2000;i+=500)splits.add(new Text(row(i)));
                t.addSplits(table,splits); require(t.listSplits(table).size()==3,"Split count mismatch");System.out.println("SPLITS=3");break;
            case "scan": case "eventual":
                int expected=Integer.parseInt(args[2]); MessageDigest hash=MessageDigest.getInstance("SHA-256");
                if(action.equals("eventual"))require(!c.instanceOperations().getScanServers().isEmpty(),"No scan server registered");
                try(org.apache.accumulo.core.client.Scanner s=c.createScanner(table,Authorizations.EMPTY)) {
                    s.setTimeout(90,TimeUnit.SECONDS);
                    if(action.equals("eventual"))s.setConsistencyLevel(ScannerBase.ConsistencyLevel.EVENTUAL);
                    int n=0; for(var e:s){require(e.getKey().getRow().toString().equals(row(n)),"Row mismatch "+n);require(e.getValue().toString().equals(value(n)),"Value mismatch "+n);hash.update((row(n)+"="+value(n)+"\n").getBytes(StandardCharsets.UTF_8));n++;}
                    require(n==expected,"Count "+n+" != "+expected);
                    Random r=new Random(42);for(int j=0;j<40;j++){int i=r.nextInt(expected);s.setRange(new Range(row(i)));var it=s.iterator();require(it.hasNext()&&it.next().getValue().toString().equals(value(i))&&!it.hasNext(),"Point read "+i);}
                    System.out.println("SCAN_PASS consistency="+s.getConsistencyLevel()+" count="+n+" randomReads=40 sha256="+hex(hash.digest()));
                } break;
            case "files":
                String id=t.tableIdMap().get(table); require(id!=null,"Table missing");
                Configuration conf=new Configuration(); conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"));
                Set<String> paths=new TreeSet<>();
                try(org.apache.accumulo.core.client.Scanner s=c.createScanner("accumulo.metadata",Authorizations.EMPTY)) {
                    s.fetchColumnFamily("file"); for(var e:s)if(e.getKey().getRow().toString().startsWith(id+";")||e.getKey().getRow().toString().equals(id+"<"))paths.add(e.getKey().getColumnQualifier().toString());
                }
                require(!paths.isEmpty(),"No RFile metadata references");
                for(String uri:paths){require(uri.startsWith(S3+"/tables/"+id+"/"),"Unexpected placement "+uri);Path file=new Path(uri);MessageDigest h=MessageDigest.getInstance("SHA-256");long size=0;try(var in=file.getFileSystem(conf).open(file)){byte[] b=new byte[8192];int n;while((n=in.read(b))!=-1){h.update(b,0,n);size+=n;}}require(size>0,"Empty RFile");System.out.println("RFILE "+uri+" bytes="+size+" sha256="+hex(h.digest()));}
                break;
            case "absent":
                Configuration gcConf=new Configuration();gcConf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"));
                for(int i=2;i<args.length;i++){require(args[i].startsWith(S3+"/tables/"),"Unsafe GC probe path");Path old=new Path(args[i]);require(!old.getFileSystem(gcConf).exists(old),"Obsolete object still exists "+old);System.out.println("GC_ABSENT "+old);}
                break;
            case "delete": t.delete(table);System.out.println("DELETED");break;
            default: throw new IllegalArgumentException(action);
            }
        }
    }
}
