package cn.ityege.hadoop.mapreduce.db;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 */
public class WriteDBMapper extends Mapper<LongWritable, Text, NullWritable, StudentBean> {

    NullWritable outputKey = NullWritable.get();
    StudentBean outputValue = new StudentBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //插入数据库成功的计数器
        final Counter sc = context.getCounter("mr_sql_counters", "SUCCESS");
        //插入数据库失败的计数器
        final Counter fc = context.getCounter("mr_sql_counters", "FAILED");

        //解析输入数据
        String[] fields = value.toString().split("\\s+");
        //判断输入的数据字段是否有缺少 如果不满足需求 则为非法数据
        if (fields.length > 6) {
            outputValue.set(
                    Integer.valueOf(fields[1]),
                    fields[2],
                    Integer.valueOf(fields[3]),
                    Integer.valueOf(fields[4]),
                    Integer.valueOf(fields[5]),
                    Integer.valueOf(fields[6]),
                    Double.valueOf(fields[7])
            );
            context.write(outputKey, outputValue);
            //合法数据 计数器+1
            sc.increment(1);
        } else {
            //非法数据 计数器+1
            fc.increment(1);
        }
    }
}