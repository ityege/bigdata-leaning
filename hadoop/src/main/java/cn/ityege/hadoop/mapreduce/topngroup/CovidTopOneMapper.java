package cn.ityege.hadoop.mapreduce.topngroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidTopOneMapper extends Mapper<LongWritable, Text, CovidBean, NullWritable> {
    CovidBean outKey = new CovidBean();
    NullWritable outValue = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        outKey.set(fields[2],fields[1],Long.parseLong(fields[fields.length-2]));
        context.write(outKey,outValue);
    }
}
