package cn.ityege.hadoop.mapreduce.topngroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: 为了验证验证结果方便，可以在输出的时候以cases作为value，实际上为空即可，value并不实际意义。
 * @author: Itcast
 */
public class CovidTopNMapper extends Mapper<LongWritable, Text, CovidBean, LongWritable> {

    CovidBean outKey = new CovidBean();
    LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        outKey.set(fields[2], fields[1], Long.parseLong(fields[fields.length - 2]));
        outValue.set(Long.parseLong(fields[fields.length - 2]));

        context.write(outKey, outValue);
    }
}
