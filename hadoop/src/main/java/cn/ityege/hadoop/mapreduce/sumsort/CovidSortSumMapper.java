package cn.ityege.hadoop.mapreduce.sumsort;

import cn.ityege.hadoop.mapreduce.beans.CovidCountBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidSortSumMapper extends Mapper<LongWritable, Text, CovidCountBean, Text> {
    CovidCountBean outKey = new CovidCountBean();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidCountBean, Text>.Context context) throws IOException, InterruptedException {
        //读取汇总结果的一行数据 根据制表符进行切割
        String[] strings = value.toString().split(",");
        //取值赋值
        outKey.set(Long.parseLong(strings[strings.length-2]),Long.parseLong(strings[strings.length-1]));// 确诊数 死亡数
        outValue.set(strings[2]);//州
        //输出结果
        context.write(outKey, outValue);

    }
}
