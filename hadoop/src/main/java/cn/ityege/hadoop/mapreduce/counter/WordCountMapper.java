package cn.ityege.hadoop.mapreduce.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类似spark和flink中的累加器
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从程序上下文对象获取一个全局计数器：用于统计apple出现的个数
        //需要指定计数器组 和计数器的名字
        Counter counter = context.getCounter("counters", "hello Counter");

        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            //判断读取内容是否为apple  如果是 计数器加1
            if("hello".equals(word)){
                counter.increment(1);
            }
            context.write(new Text(word),new LongWritable(1));
        }
    }
}