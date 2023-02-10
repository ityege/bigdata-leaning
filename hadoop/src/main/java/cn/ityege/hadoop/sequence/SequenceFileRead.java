package cn.ityege.hadoop.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileRead {
    public static void main(String[] args) throws Exception {
        //设置客户端运行身份 以root去操作访问HDFS
        System.setProperty("HADOOP_USER_NAME", "root");
        //Configuration 用于指定相关参数属性
        Configuration conf = new Configuration();
        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(new Path("hdfs://bigdata1:8020/test/java/seq.out"));
        SequenceFile.Reader.Option option2 = SequenceFile.Reader.length(1024);//指定要读取的字节数

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(conf, option1, option2);
            //通过反射创建key value对象
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();//返回输入文件中的当前字节位置
            while (reader.next(key,value)){
                String syncSeen =  reader.syncSeen()?"*":"";//是否返回了Sync Mark同步标记
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}
