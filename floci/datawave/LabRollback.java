package local.floci;

import java.security.MessageDigest;
import java.util.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.ImportConfiguration;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/** Export an offline S3 canary, restore it on HDFS, then restore the lab's S3 default. */
public final class LabRollback {
    static final String S3="s3a://dwv-accumulo-lab/accumulo";
    static final String HDFS="hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo-s3-lab";
    static final String SRC="phase5_backup_source", DST="phase5_backup_restored", AFTER="phase5_after_rollback";
    static void require(boolean ok,String message){if(!ok)throw new AssertionError(message);}
    static List<String> files(AccumuloClient c,String table) throws Exception {
        String id=c.tableOperations().tableIdMap().get(table);List<String> refs=new ArrayList<>();
        try(org.apache.accumulo.core.client.Scanner s=c.createScanner("accumulo.metadata",Authorizations.EMPTY)){
            s.fetchColumnFamily("file");for(var e:s){String row=e.getKey().getRow().toString();if(row.startsWith(id+";")||row.equals(id+"<"))refs.add(e.getKey().getColumnQualifier().toString());}
        }require(!refs.isEmpty(),"No files for "+table);return refs;
    }
    static void write(AccumuloClient c,String table) throws Exception {
        try(BatchWriter w=c.createBatchWriter(table)){for(int i=0;i<128;i++){Mutation m=new Mutation(String.format("row%04d",i));m.put("cf","cq","backup-value-"+i);w.addMutation(m);}}
        c.tableOperations().flush(table,null,null,true);
    }
    static void scan(AccumuloClient c,String table) throws Exception {
        try(org.apache.accumulo.core.client.Scanner s=c.createScanner(table,Authorizations.EMPTY)){int n=0;for(var e:s){require(e.getKey().getRow().toString().equals(String.format("row%04d",n))&&e.getValue().toString().equals("backup-value-"+n),"Canary differs at "+n);n++;}require(n==128,"Expected 128 restored rows");}
        System.out.println("CANARY_SCAN_PASS table="+table+" rows=128");
    }
    static String hash(FileSystem fs,Path path) throws Exception {
        MessageDigest h=MessageDigest.getInstance("SHA-256");try(var in=fs.open(path)){byte[] b=new byte[8192];int n;while((n=in.read(b))!=-1)h.update(b,0,n);}StringBuilder s=new StringBuilder();for(byte b:h.digest())s.append(String.format("%02x",b&255));return s.toString();
    }
    public static void main(String[] args) throws Exception {
        Properties props=new Properties();try(var in=java.nio.file.Files.newInputStream(java.nio.file.Paths.get("/opt/accumulo/conf/accumulo-client.properties"))){props.load(in);}
        require("floci-lab".equals(props.getProperty("instance.name")),"Only isolated lab is permitted");props.setProperty("auth.type","password");props.setProperty("auth.principal","root");props.setProperty("rpc.timeout","10s");
        try(AccumuloClient c=Accumulo.newClient().from(props).build()){
            require(c.instanceOperations().getInstanceID().equals("2f824244-c68b-4e29-8f9b-38bf74c5f054"),"Wrong UUID");
            require(S3.equals(c.instanceOperations().getSystemConfiguration().get("general.custom.volume.preferred.default")),"Expected S3 default");
            for(String table:List.of(DST,AFTER))require(!c.tableOperations().exists(table),"Prior rollback run exists: "+table);
            if(!c.tableOperations().exists(SRC)){c.tableOperations().create(SRC);write(c,SRC);}else{c.tableOperations().online(SRC,true);scan(c,SRC);c.tableOperations().flush(SRC,null,null,true);}
            scan(c,SRC);List<String> original=files(c,SRC);
            for(String uri:original)require(uri.startsWith(S3+"/tables/"),"Source is not S3");
            Configuration conf=new Configuration();conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/core-site.xml"));
            Path export=new Path(HDFS+"/phase5-backup/verified-canary");FileSystem backup=export.getFileSystem(conf);require(!backup.exists(export),"Backup path already exists");
            c.tableOperations().offline(SRC,true);c.tableOperations().exportTable(SRC,export.toString());
            for(String uri:original){Path from=new Path(uri),to=new Path(export,from.getName());FileSystem source=from.getFileSystem(conf);String expected=hash(source,from);require(FileUtil.copy(source,from,backup,to,false,conf),"S3-to-HDFS backup copy failed");require(expected.equals(hash(backup,to)),"Backup checksum differs");System.out.println("BACKUP_FILE source="+from+" backup="+to+" sha256="+expected);}
            Map<String,String> policies=new TreeMap<>();
            for(String table:c.tableOperations().list())if(table.startsWith("datawave.")){
                policies.put(table,c.tableOperations().getTableProperties(table).get("table.custom.volume.preferred"));
                c.tableOperations().setProperty(table,"table.custom.volume.preferred",S3);
            }
            try {
                c.instanceOperations().setProperty("general.custom.volume.preferred.default",HDFS);Thread.sleep(3000);
                Path work=new Path(HDFS+"/phase5-backup/import-work");require(!backup.exists(work),"Import work path exists");
                require(FileUtil.copy(backup,export,backup,work,false,conf),"Could not create independent import working copy");
                c.tableOperations().importTable(DST,Set.of(work.toString()),ImportConfiguration.builder().setKeepOffline(false).build());
                scan(c,DST);for(String uri:files(c,DST)){require(uri.startsWith(HDFS+"/tables/"),"Restored file is not HDFS");System.out.println("RESTORED_HDFS_FILE="+uri);}
                c.tableOperations().setProperty(DST,"table.custom.volume.preferred",HDFS);
                c.tableOperations().online(SRC,true);scan(c,SRC);require(files(c,SRC).equals(original),"Original S3 references changed");
            } finally {
                c.instanceOperations().setProperty("general.custom.volume.preferred.default",S3);Thread.sleep(3000);
                for(var policy:policies.entrySet())if(policy.getValue()==null)c.tableOperations().removeProperty(policy.getKey(),"table.custom.volume.preferred");else c.tableOperations().setProperty(policy.getKey(),"table.custom.volume.preferred",policy.getValue());
            }
            Thread.sleep(3000);c.tableOperations().create(AFTER);write(c,AFTER);scan(c,AFTER);
            for(String uri:files(c,AFTER)){require(uri.startsWith(S3+"/tables/"),"Restored S3 default not effective");System.out.println("RESTORED_S3_DEFAULT_FILE="+uri);}
            System.out.println("LAB_ROLLBACK_RESTORE=PASS scope=128-row-offline-table-export;not-whole-instance-backup");
        }
    }
}
