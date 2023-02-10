package cn.ityege.hadoop.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;

public class HDFSRead {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://bigdata1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream in = fileSystem.open(new Path("/test/java/abc"));
        FileOutputStream out = new FileOutputStream("D:\\abc.txt");
        IOUtils.copy(in, out);
        fileSystem.close();
    }
}
