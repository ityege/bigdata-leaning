package cn.ityege.hadoop.mapreduce.sumserialize;

import cn.ityege.hadoop.mapreduce.beans.CovidCountBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidSumMapper extends Mapper<LongWritable, Text,Text, CovidCountBean> {
    Text outKey=new Text();
    CovidCountBean outValue=new CovidCountBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        //读取一行数据 进行切割
        String[] fields = value.toString().split(",");
        //提取数据  州  确诊数  死亡数
        outKey.set(fields[2]);
        //因为疫情数据中 美国某些县没有编码 导致数据缺失一个字段 正着数就会角标越界  可以采用倒着数
        outValue.set(Long.parseLong(fields[fields.length-2]),Long.parseLong(fields[fields.length-1]));
        //输出结果
        context.write(outKey,outValue);//  <州，CovidCountBean>
    }
}
