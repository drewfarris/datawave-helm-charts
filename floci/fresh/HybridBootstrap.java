package local.floci;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/** Fresh-only, serialized HDFS initialization and S3 volume registration, before manager startup. */
public final class HybridBootstrap {
    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
    static boolean empty(FileSystem fs, Path path) throws Exception {
        return !fs.exists(path) || fs.listStatus(path).length == 0;
    }
    static String marker(FileSystem fs, Path path) throws Exception {
        Path markers = new Path(path, "instance_id");
        if (!fs.exists(markers)) return null;
        FileStatus[] entries = fs.listStatus(markers);
        require(entries.length == 1 && entries[0].isFile(), "Partial instance markers: " + markers);
        return entries[0].getPath().getName();
    }
    static Set<String> versions(FileSystem fs, Path path) throws Exception {
        Set<String> result = new TreeSet<>();
        Path base = new Path(path, "version");
        if (fs.exists(base)) for (FileStatus entry : fs.listStatus(base)) result.add(entry.getPath().getName());
        return result;
    }
    static void volumes(String value) throws Exception {
        java.nio.file.Path config = java.nio.file.Path.of(System.getenv("ACCUMULO_CONF_DIR"), "accumulo.properties");
        List<String> lines = Files.readAllLines(config);
        boolean found = false;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith("instance.volumes=")) {
                lines.set(i, "instance.volumes=" + value);
                found = true;
            }
        }
        require(found, "Missing instance.volumes property");
        Files.write(config, lines);
    }
    static void init(String... args) throws Exception {
        List<String> command = new java.util.ArrayList<>(Arrays.asList("accumulo", "init"));
        command.addAll(Arrays.asList(args));
        require(new ProcessBuilder(command).inheritIO().start().waitFor() == 0,
                "Accumulo initialization/registration failed; inspect state, never clear volumes automatically");
    }
    public static void main(String[] args) throws Exception {
        require(args.length == 4, "Usage: HybridBootstrap instance zookeepers hdfsURI s3URI");
        String name = args[0];
        require(name.matches("[a-zA-Z0-9_-]+"), "Invalid instance name");
        Path hdfs = new Path(args[2]), s3 = new Path(args[3]);
        require("hdfs".equals(hdfs.toUri().getScheme()) && hdfs.toUri().getPath().length() > 1, "Dedicated HDFS directory required");
        require("s3a".equals(s3.toUri().getScheme()) && s3.toUri().getPath().length() > 1, "Dedicated S3 prefix required");
        Configuration configuration = new Configuration();
        configuration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        configuration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));
        CountDownLatch connected = new CountDownLatch(1);
        try (ZooKeeper zk = new ZooKeeper(args[1], 120000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) connected.countDown();
        }); FileSystem hf = FileSystem.newInstance(hdfs.toUri(), configuration);
             FileSystem sf = FileSystem.newInstance(s3.toUri(), configuration)) {
            require(connected.await(60, TimeUnit.SECONDS), "ZooKeeper unavailable");
            String parent = "/datawave-driver-hybrid";
            try { zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); }
            catch (org.apache.zookeeper.KeeperException.NodeExistsException expected) { }
            String owner = parent + "/" + name;
            byte[] plan = (args[2] + "\n" + args[3]).getBytes(StandardCharsets.UTF_8);
            String existingId = zk.exists("/accumulo/instances/" + name, false) == null ? null
                    : new String(zk.getData("/accumulo/instances/" + name, false, null), StandardCharsets.UTF_8);
            if (zk.exists(owner, false) == null) {
                require(existingId == null && empty(hf, hdfs) && empty(sf, s3),
                        "Refusing existing/unowned Accumulo data. Fresh instances only; no migration is provided.");
                zk.create(owner, plan, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            require(Arrays.equals(plan, zk.getData(owner, false, null)), "Owned instance volume configuration changed");
            // An ephemeral lock serializes concurrent starts and disappears if this JVM dies.
            zk.create(owner + "/lock", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            String hid = marker(hf, hdfs), sid = marker(sf, s3);
            String registered = owner + "/registered";
            if (zk.exists(registered, false) != null) {
                String recorded = new String(zk.getData(registered, false, null), StandardCharsets.UTF_8);
                require(recorded.equals(existingId) && recorded.equals(hid) && recorded.equals(sid),
                        "Previously registered instance data is missing or mismatched; refusing reinitialization or volume replacement");
            }
            if (existingId == null) {
                require(hid == null && sid == null && empty(hf, hdfs) && empty(sf, s3), "Partial initialization; inspect before retry");
                volumes(args[2]);
                hf.mkdirs(hdfs);
                hf.setPermission(hdfs, new org.apache.hadoop.fs.permission.FsPermission((short)0700));
                init("--instance-name", name, "--password", System.getenv("ACCUMULO_ROOT_PASSWORD"));
                existingId = new String(zk.getData("/accumulo/instances/" + name, false, null), StandardCharsets.UTF_8);
                hid = marker(hf, hdfs);
            }
            require(existingId.equals(hid) && !versions(hf, hdfs).isEmpty(), "HDFS/ZooKeeper identity or version mismatch");
            volumes(args[2] + "," + args[3]);
            if (sid == null) {
                require(empty(sf, s3), "Partial S3 volume registration; inspect before retry");
                init("--add-volumes");
            }
            require(existingId.equals(marker(sf, s3)) && versions(hf, hdfs).equals(versions(sf, s3)), "Registered volume mismatch");
            try (ServerContext context = new ServerContext(SiteConfiguration.auto())) {
                for (String table : List.of("+r", "!0")) {
                    var key = TablePropKey.of(context, TableId.of(table));
                    context.getPropStore().putAll(key, Map.of("table.custom.volume.preferred", args[2]));
                    require(args[2].equals(context.getPropStore().get(key).asMap().get("table.custom.volume.preferred")),
                            "System-table HDFS policy did not persist");
                }
            }
            if (zk.exists(registered, false) == null)
                zk.create(registered, existingId.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("HYBRID_BOOTSTRAP=PASS INSTANCE_ID=" + existingId);
        }
    }
}
