package cn.ityege.hadoop.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

public class SequenceFileWrite {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws Exception {
        //设置客户端运行身份 以root去操作访问HDFS
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        //Configuration 用于指定相关参数属性
        Configuration conf = new Configuration();
        //sequence file key、value
        IntWritable key = new IntWritable();
        Text value = new Text();

        //构造Writer参数属性
        SequenceFile.Writer writer = null;
        CompressionCodec codec = new GzipCodec();

//        SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path("hdfs://bigdata1:8020/test/java/seq.out"));
        SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path("D:\\code\\bigdata-lean\\data\\output\\seq.seq"));

        SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(value.getClass());

        SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, codec);

        try {
            writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
