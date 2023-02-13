package cn.ityege.hadoop.mapreduce.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadDBMapper extends Mapper<LongWritable, StudentBean,LongWritable, Text> {
    LongWritable outputKey = new LongWritable();
    Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, StudentBean value, Mapper<LongWritable, StudentBean, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        outputKey.set(key.get());
        outputValue.set(value.toString());

        context.write(outputKey,outputValue);
    }
}
