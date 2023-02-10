package cn.ityege.hadoop.hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;

public class HDFSWrite {
    public static void main(String[] args)  throws Exception{
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://bigdata1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FileInputStream in = new FileInputStream("D:\\软件安装\\7zip\\7z2106-x64.exe");
        Path path = new Path("/test/java/7zip.exe");
        FSDataOutputStream out = fileSystem.create(path);
        IOUtils.copy(in,out);
        fileSystem.close();
    }
}
