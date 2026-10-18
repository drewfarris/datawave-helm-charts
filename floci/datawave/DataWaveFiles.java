package local.floci;

import java.security.MessageDigest;
import java.util.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** Flush only isolated DataWave tables and retrieve every metadata-referenced S3 RFile. */
public final class DataWaveFiles {
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static String hex(byte[] bytes){StringBuilder b=new StringBuilder();for(byte x:bytes)b.append(String.format("%02x",x&255));return b.toString();}
    public static void main(String[] args) throws Exception {
        Properties props=new Properties();try(var in=java.nio.file.Files.newInputStream(java.nio.file.Paths.get("/opt/accumulo/conf/accumulo-client.properties"))){props.load(in);}
        require("floci-lab".equals(props.getProperty("instance.name")),"Only isolated lab is permitted");
        props.setProperty("auth.type","password");props.setProperty("auth.principal","root");props.setProperty("rpc.timeout","10s");
        try(AccumuloClient client=Accumulo.newClient().from(props).build()) {
            require(client.instanceOperations().getInstanceID().equals("2f824244-c68b-4e29-8f9b-38bf74c5f054"),"Instance UUID differs");
            Map<String,String> ids=new TreeMap<>();
            client.tableOperations().tableIdMap().forEach((name,id)->{if(name.startsWith("datawave."))ids.put(id,name);});
            require(ids.size()>=10,"DataWave tables were not initialized");
            if(args.length>0&&args[0].equals("flush"))for(String table:ids.values())client.tableOperations().flush(table,null,null,true);
            if(args.length>0&&args[0].equals("compact"))for(String table:ids.values())client.tableOperations().compact(table,null,null,true,true);
            Configuration conf=new Configuration();conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"));
            Map<String,Set<String>> references=new TreeMap<>();
            try(org.apache.accumulo.core.client.Scanner scanner=client.createScanner("accumulo.metadata",Authorizations.EMPTY)) {
                scanner.fetchColumnFamily("file");
                for(var entry:scanner){String row=entry.getKey().getRow().toString();String id=row.split("[;<]",2)[0];if(ids.containsKey(id))references.computeIfAbsent(ids.get(id),x->new TreeSet<>()).add(entry.getKey().getColumnQualifier().toString());}
            }
            for(String expected:List.of("datawave.shard","datawave.shardIndex","datawave.shardReverseIndex","datawave.metadata","datawave.annotation"))require(references.containsKey(expected),"Missing flushed files for "+expected);
            for(var table:references.entrySet())for(String uri:table.getValue()) {
                require(uri.startsWith("s3a://dwv-accumulo-lab/accumulo/tables/"),"DataWave file has unexpected placement: "+uri);
                Path path=new Path(uri);MessageDigest hash=MessageDigest.getInstance("SHA-256");long size=0;
                try(var in=path.getFileSystem(conf).open(path)){byte[] bytes=new byte[8192];int n;while((n=in.read(bytes))!=-1){hash.update(bytes,0,n);size+=n;}}
                require(size>0,"Empty RFile");System.out.println("DWFILE table="+table.getKey()+" uri="+uri+" bytes="+size+" sha256="+hex(hash.digest()));
            }
            System.out.println("DATAWAVE_FILES=PASS populatedTables="+references.size());
        }
    }
}
