package cn.ityege.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 将HADOOP_USER_NAME已经设置在环境变量中了
 */
public class HDFSClient {
    private FileSystem fileSystem;

    @Before
    public void init() throws Exception {
        URI uri = new URI("hdfs://bigdata1:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");

        String user = "bigdata";
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws Exception {
        fileSystem.close();
    }

    // 创建目录
    @Test
    public void testMkdir() throws Exception {
        fileSystem.mkdirs(new Path("/test/java"));
    }

    /**
     * 写入文件
     *
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/test/java/abc"));
        fsDataOutputStream.write("hello word!!!!!".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testPut2() throws Exception {
        fileSystem.copyFromLocalFile(false, true, new Path("D:\\code\\bigdata-lean\\.gitignore"), new Path("hdfs://bigdata1/test/java"));

    }


    @Test
    public void testGet() throws Exception {
        fileSystem.copyToLocalFile(false, new Path("/directory/app-20221120105557-0000"), new Path("D:\\"));

    }

    @Test
    public void testRm() throws Exception {
        fileSystem.delete(new Path("/test/java/.gitignore"), true);
    }

    //获取文件详细情况
    @Test
    public void testDetail() throws Exception {

        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("==========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }


    @Test
    public void testFile() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }

    }

}
