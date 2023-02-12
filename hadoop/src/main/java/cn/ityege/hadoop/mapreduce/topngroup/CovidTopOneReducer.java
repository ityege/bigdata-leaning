package cn.ityege.hadoop.mapreduce.topngroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description:
 * @author: Itcast
 */
public class CovidTopOneReducer extends Reducer<CovidBean, NullWritable,CovidBean,NullWritable> {
    @Override
    protected void reduce(CovidBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //todo 不遍历迭代器 此时的key就是分组中第一个kv键值对的key. 也就是该州确诊病例数最多的top1.
        context.write(key,NullWritable.get());
    }
}
