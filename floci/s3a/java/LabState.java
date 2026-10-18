package local.floci;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/** Read-only consistency check used before any lab initialization or volume registration. */
public final class LabState {
    private static void require(boolean value, String message) { if (!value) throw new IllegalStateException(message); }
    private static String marker(FileSystem fs, Path base) throws Exception {
        Path path = new Path(base, "instance_id");
        if (!fs.exists(path)) return null;
        FileStatus[] children = fs.listStatus(path);
        if (children.length == 0) return null;
        require(children.length == 1 && children[0].isFile(), "Ambiguous instance markers: " + path);
        return children[0].getPath().getName();
    }
    private static Set<String> versions(FileSystem fs, Path base) throws Exception {
        Set<String> result = new TreeSet<>();
        Path path = new Path(base, "version");
        if (fs.exists(path)) for (FileStatus file : fs.listStatus(path)) result.add(file.getPath().getName());
        return result;
    }
    private static boolean empty(FileSystem fs, Path path) throws Exception { return !fs.exists(path) || fs.listStatus(path).length == 0; }
    public static void main(String[] args) throws Exception {
        require(args.length == 4, "Usage: LabState instanceName zookeepers hdfsURI s3URI");
        require(!"dev".equals(args[0]), "Refusing to bootstrap the development instance");
        Configuration conf = new Configuration();
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        Path hdfs = new Path(args[2]), s3 = new Path(args[3]);
        require(hdfs.toUri().getPath().equals("/accumulo-s3-lab"), "Unexpected HDFS lab path");
        require(s3.toString().equals("s3a://dwv-accumulo-lab/accumulo"), "Unexpected S3 lab path");
        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(args[1], 30000, event -> { if (event.getState() == Watcher.Event.KeeperState.SyncConnected) connected.countDown(); });
        try (FileSystem hf = FileSystem.newInstance(hdfs.toUri(), conf); FileSystem sf = FileSystem.newInstance(s3.toUri(), conf)) {
            require(connected.await(30, TimeUnit.SECONDS), "ZooKeeper connection timeout");
            String namePath = "/accumulo/instances/" + args[0];
            String zkId = zk.exists(namePath, false) == null ? null : new String(zk.getData(namePath, false, null), StandardCharsets.UTF_8);
            String hdfsId = marker(hf, hdfs), s3Id = marker(sf, s3);
            if (zkId == null && hdfsId == null) {
                require(empty(hf, hdfs) && empty(sf, s3), "Partial initialization found; inspect before retry");
                System.out.println("STATE=ABSENT");
            } else {
                require(zkId != null && zkId.equals(hdfsId), "ZooKeeper and HDFS identity mismatch; refusing reinitialization");
                require(!versions(hf, hdfs).isEmpty(), "HDFS data version missing");
                System.out.println("INSTANCE_ID=" + zkId);
                if (s3Id == null) {
                    require(empty(sf, s3), "Partial S3 registration found; inspect before retry");
                    System.out.println("STATE=INITIALIZED");
                } else {
                    require(zkId.equals(s3Id), "S3 instance identity mismatch");
                    require(versions(hf, hdfs).equals(versions(sf, s3)), "Volume data-version mismatch");
                    System.out.println("DATA_VERSIONS=" + versions(hf, hdfs));
                    System.out.println("STATE=REGISTERED");
                }
            }
        } finally { zk.close(); }
    }
}
