package cn.ityege.hadoop.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidPartitionMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        //以州作为key 参与分区 通过自定义分区 同一个州的数据到同一个分区同一个reducetask处理
        String state = strings[2];
        outKey.set(state);
        context.write(outKey, value);
    }
}
