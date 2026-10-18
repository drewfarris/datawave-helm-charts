package local.floci;

import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Read a real immutable lab RFile through each application's actual client libraries. */
public final class RFileReadProbe {
    private static void require(boolean ok, String message) { if (!ok) throw new AssertionError(message); }
    private static String hex(byte[] data) { StringBuilder s=new StringBuilder(); for(byte b:data)s.append(String.format("%02x",b&255)); return s.toString(); }
    public static void main(String[] args) throws Exception {
        require(args.length==2 && args[0].startsWith("s3a://dwv-accumulo-lab/accumulo/tables/"),"Only a lab S3 RFile may be read");
        Configuration conf=new Configuration();conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"));
        require(conf.get("fs.s3a.endpoint","").contains("floci.aws-emulation"),"Unexpected endpoint");
        System.out.println("RFILE_API_SOURCE="+RFile.class.getProtectionDomain().getCodeSource().getLocation());
        System.out.println("HADOOP_SOURCE="+Configuration.class.getProtectionDomain().getCodeSource().getLocation());
        try(FileSystem fs=FileSystem.newInstance(URI.create(args[0]),conf)) {
            MessageDigest bytes=MessageDigest.getInstance("SHA-256");
            try(var stream=fs.open(new Path(args[0]))){byte[] buffer=new byte[8192];int n;while((n=stream.read(buffer))!=-1)bytes.update(buffer,0,n);}
            String fileHash=hex(bytes.digest());require(fileHash.equals(args[1]),"Object byte checksum differs from server retrieval");
            List<Key> keys=new ArrayList<>();List<Value> values=new ArrayList<>();
            try(Scanner scanner=RFile.newScanner().from(args[0]).withFileSystem(fs).withoutSystemIterators().build()) {
                TreeMap<Key,Value> unique=new TreeMap<>();
                for(var entry:scanner)unique.putIfAbsent(new Key(entry.getKey()),new Value(entry.getValue()));
                keys.addAll(unique.keySet());values.addAll(unique.values());
                require(!keys.isEmpty(),"RFile contains no entries");
                Random random=new Random(42);
                for(int j=0;j<25;j++){
                    int i=random.nextInt(keys.size());scanner.setRange(new Range(keys.get(i),true,null,true));
                    var it=scanner.iterator();require(it.hasNext(),"Random seek returned no entry");var actual=it.next();
                    require(actual.getKey().equals(keys.get(i))&&actual.getValue().equals(values.get(i)),"Random seek mismatch");
                }
            }
            System.out.println("DIRECT_RFILE_READ=PASS entries="+keys.size()+" randomSeeks=25 sha256="+fileHash);
        }
    }
}
