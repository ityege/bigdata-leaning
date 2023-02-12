package cn.ityege.hadoop.mapreduce.topngroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
*这个key是变化的,这是为什么
 */
public class CovidTopNReducer extends Reducer<CovidBean, LongWritable, CovidBean, LongWritable> {
    @Override
    protected void reduce(CovidBean key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for (LongWritable value : values) {
            if (num < 3) {
                context.write(key, value);
                num++;
            } else {
                return;
            }
        }

        //todo  探究reduce方法 输入的key到底是哪个key
        //todo  1、不迭代values 直接输出kv  此时key是分组中第一个kv对所对应的key
        //        context.write(key,new LongWritable(111L));

        //todo  2、一边迭代values 一边输出kv 此时key会随着value的变化而变化 与之对应
//        for (LongWritable value : values) {
//            context.write(key,value);
//        }

        //todo  3、迭代完values 最终输出一次kv  此时的key是分组中最后一个key
//        for (LongWritable value : values) {
//        }
//
//        context.write(key,new LongWritable(11111L));
    }
}
