package local.floci;

import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** Acceptance probe for the disposable fresh-instance test; never targets existing dev tables. */
public final class VerifyHybrid {
    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !args[0].contains("driver-fresh-test") || !args[1].contains("driver-fresh-test"))
            throw new IllegalArgumentException("Probe is restricted to disposable driver-fresh-test volumes");
        java.util.Properties props = new java.util.Properties();
        try (InputStream in = java.nio.file.Files.newInputStream(Path.of(System.getenv("ACCUMULO_CONF_DIR"), "accumulo-client.properties"))) { props.load(in); }
        props.setProperty("auth.type", "password");
        props.setProperty("rpc.timeout", "15s");
        try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
            for (String table : new String[]{"accumulo.root", "accumulo.metadata"}) {
                if (!args[0].equals(client.tableOperations().getConfiguration(table).get("table.custom.volume.preferred")))
                    throw new AssertionError("System table is not pinned to HDFS");
            }
            String table = "driver_storage_canary";
            boolean created = !client.tableOperations().exists(table);
            if (created) client.tableOperations().create(table);
            if (created) try (var writer = client.createBatchWriter(table)) {
                for (int i = 0; i < 128; i++) { Mutation m = new Mutation(String.format("r%04d", i)); m.put("cf", "cq", "v" + i); writer.addMutation(m); }
            }
            client.tableOperations().flush(table, null, null, true);
            Map<String,String> rows = new TreeMap<>();
            try (var scanner = client.createScanner(table, Authorizations.EMPTY)) {
                for (var e : scanner) rows.put(e.getKey().getRow().toString(), e.getValue().toString());
            }
            if (rows.size() != 128) throw new AssertionError("Wrong canary count");
            for (int i = 0; i < 128; i++) if (!("v" + i).equals(rows.get(String.format("r%04d", i)))) throw new AssertionError("Record mismatch");
            String id = client.tableOperations().tableIdMap().get(table);
            int files = 0;
            Configuration config = new Configuration();
            config.addResource(new org.apache.hadoop.fs.Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
            config.addResource(new org.apache.hadoop.fs.Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));
            try (var scanner = client.createScanner("accumulo.metadata", Authorizations.EMPTY)) {
                scanner.fetchColumnFamily("file");
                for (var e : scanner) {
                    String row = e.getKey().getRow().toString();
                    if (!row.startsWith(id + ";") && !row.equals(id + "<")) continue;
                    String uri = e.getKey().getColumnQualifier().toString();
                    if (!uri.startsWith(args[1] + "/tables/")) throw new AssertionError("New user RFile is not on S3: " + uri);
                    var p = new org.apache.hadoop.fs.Path(uri);
                    try (FileSystem fs = FileSystem.newInstance(p.toUri(), config); InputStream in = fs.open(p)) {
                        byte[] bytes = in.readAllBytes();
                        String sha = new java.math.BigInteger(1, MessageDigest.getInstance("SHA-256").digest(bytes)).toString(16);
                        System.out.println("USER_FILE=" + uri + " BYTES=" + bytes.length + " SHA256=" + sha);
                    }
                    files++;
                }
            }
            if (files == 0) throw new AssertionError("No metadata-referenced user RFiles");
            var hp = new org.apache.hadoop.fs.Path(args[0]);
            try (FileSystem fs = FileSystem.newInstance(hp.toUri(), config)) {
                for (String systemId : new String[]{"+r", "!0"}) {
                    var found = fs.listFiles(new org.apache.hadoop.fs.Path(args[0] + "/tables/" + systemId), true);
                    boolean rfile = false;
                    while (found.hasNext()) rfile |= found.next().getPath().getName().endsWith(".rf");
                    if (!rfile) throw new AssertionError("No HDFS system RFile: " + systemId);
                }
                if (!fs.listFiles(new org.apache.hadoop.fs.Path(args[0] + "/wal"), true).hasNext()) throw new AssertionError("No HDFS WAL");
            }
            var sp = new org.apache.hadoop.fs.Path(args[1]);
            try (FileSystem fs = FileSystem.newInstance(sp.toUri(), config)) {
                for (String path : new String[]{"/tables/+r", "/tables/!0", "/wal"})
                    if (fs.exists(new org.apache.hadoop.fs.Path(args[1] + path))) throw new AssertionError("System data unexpectedly on S3");
            }
            System.out.println("FRESH_HYBRID_VERIFY=PASS ROWS=128 SYSTEM_TABLES=HDFS USER_FILES=S3");
        }
    }
}
