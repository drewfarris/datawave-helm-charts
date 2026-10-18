package local.floci;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;

/** Exercises real S3A streams, multipart, seek, rename and change detection. */
public final class S3ASmoke {
    private static final int SIZE = 11 * 1024 * 1024 + 123;
    private static byte expected(long offset) { return (byte) (offset % 251); }
    private static void require(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) out.append(String.format("%02x", b & 255));
        return out.toString();
    }
    public static void main(String[] args) throws Exception {
        String dir = System.getenv("HADOOP_CONF_DIR");
        require(dir != null, "HADOOP_CONF_DIR missing");
        Configuration conf = new Configuration();
        require(conf.get("fs.s3a.endpoint", "").contains("floci.aws-emulation"), "Mounted core-site.xml is not visible through the actual classloader");
        conf.addResource(new Path(dir + "/core-site.xml"));
        if (Files.exists(Paths.get(dir, "hdfs-site.xml"))) conf.addResource(new Path(dir + "/hdfs-site.xml"));
        require(conf.get("fs.defaultFS").startsWith("hdfs://"), "HDFS default was changed");
        require(conf.getPassword("fs.s3a.access.key") != null, "JCEKS access-key alias missing");
        require(conf.getPassword("fs.s3a.secret.key") != null, "JCEKS secret-key alias missing");
        require("server".equals(conf.get("fs.s3a.change.detection.mode")), "Server ETag detection required");
        conf.setLong("fs.s3a.multipart.size", 5 * 1024 * 1024);
        conf.setLong("fs.s3a.multipart.threshold", 5 * 1024 * 1024);
        conf.set("fs.s3a.fast.upload.buffer", "disk");
        String name = args.length == 0 ? "runtime" : args[0];
        Path base = new Path("s3a://dwv-s3-smoke/phase2/" + name);
        try (FileSystem fs = FileSystem.newInstance(URI.create("s3a://dwv-s3-smoke"), conf)) {
            System.out.println("hadoop.version=" + VersionInfo.getVersion());
            System.out.println("defaultFS=" + conf.get("fs.defaultFS"));
            System.out.println("filesystem.class=" + fs.getClass().getName());
            System.out.println("filesystem.source=" + fs.getClass().getProtectionDomain().getCodeSource().getLocation());
            Class<?> sdk = Class.forName("software.amazon.awssdk.services.s3.S3Client", true, fs.getClass().getClassLoader());
            System.out.println("sdk.source=" + sdk.getProtectionDomain().getCodeSource().getLocation());
            System.out.println("credentials=JCEKS aliases resolved; values omitted");
            fs.mkdirs(base);
            Path original = new Path(base, "multipart.bin");
            MessageDigest written = MessageDigest.getInstance("SHA-256");
            byte[] block = new byte[65536];
            try (FSDataOutputStream stream = fs.create(original, true)) {
                for (int position = 0; position < SIZE;) {
                    int length = Math.min(block.length, SIZE - position);
                    for (int i = 0; i < length; i++) block[i] = expected(position + i);
                    stream.write(block, 0, length);
                    written.update(block, 0, length);
                    position += length;
                }
            }
            require(fs.getFileStatus(original).getLen() == SIZE, "Multipart length mismatch");
            byte[] hash = written.digest();
            MessageDigest readHash = MessageDigest.getInstance("SHA-256");
            try (FSDataInputStream stream = fs.open(original)) {
                int n;
                while ((n = stream.read(block)) >= 0) readHash.update(block, 0, n);
                for (long offset : new long[] {0, 5 * 1024 * 1024 - 16, SIZE - 64, 123456}) {
                    stream.seek(offset);
                    byte[] range = new byte[64];
                    stream.readFully(range);
                    for (int i = 0; i < range.length; i++) require(range[i] == expected(offset + i), "Seek mismatch at " + offset);
                }
            }
            require(Arrays.equals(hash, readHash.digest()), "SHA-256 mismatch");
            System.out.println("multipart-read-seek=PASS bytes=" + SIZE + " sha256=" + hex(hash));
            Path renamed = new Path(base, "renamed.bin");
            require(fs.rename(original, renamed), "Rename failed");
            require(!fs.exists(original) && fs.getFileStatus(renamed).getLen() == SIZE, "Copy/delete rename mismatch");
            try (FSDataInputStream stream = fs.open(renamed)) {
                MessageDigest copied = MessageDigest.getInstance("SHA-256");
                int n;
                while ((n = stream.read(block)) >= 0) copied.update(block, 0, n);
                require(Arrays.equals(hash, copied.digest()), "Rename changed contents");
            }
            Path empty = new Path(base, "empty");
            try (FSDataOutputStream ignored = fs.create(empty, true)) { }
            require(fs.getFileStatus(empty).getLen() == 0 && fs.listStatus(base).length == 2, "Zero-byte/list mismatch");
            System.out.println("rename-list-zero-byte=PASS");
            try (FSDataInputStream stale = fs.open(renamed)) {
                stale.read();
                try (FSDataOutputStream overwrite = fs.create(renamed, true)) { overwrite.write(42); }
                boolean detected = false;
                try { stale.seek(0); stale.read(); }
                catch (IOException changed) {
                    detected = changed.getClass().getName().contains("RemoteFileChanged");
                    if (!detected) throw changed;
                    System.out.println("etag-change-detection=PASS " + changed.getClass().getSimpleName());
                }
                require(detected, "Stale ETag was not rejected");
            }
            require(fs.delete(base, true) && !fs.exists(base), "Recursive delete failed");
            System.out.println("S3A_SMOKE=PASS role=" + name);
        }
    }
}
