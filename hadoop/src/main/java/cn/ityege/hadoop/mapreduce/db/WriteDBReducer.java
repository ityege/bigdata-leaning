package cn.ityege.hadoop.mapreduce.db;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: 在使用DBOutputFormat时，要求程序最终输出的key必须是继承自DBWritable的类型 value则没有具体要求
 */
public class WriteDBReducer extends Reducer<NullWritable, StudentBean,StudentBean,NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<StudentBean> values, Context context) throws IOException, InterruptedException {
        for (StudentBean value : values) {
            context.write(value,key);
        }
    }
}