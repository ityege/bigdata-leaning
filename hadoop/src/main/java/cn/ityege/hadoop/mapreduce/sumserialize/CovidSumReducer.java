package cn.ityege.hadoop.mapreduce.sumserialize;

import cn.ityege.hadoop.mapreduce.beans.CovidCountBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidSumReducer extends Reducer<Text, CovidCountBean, Text, CovidCountBean> {
    CovidCountBean outValue = new CovidCountBean();

    @Override
    protected void reduce(Text key, Iterable<CovidCountBean> values, Reducer<Text, CovidCountBean, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {

        //统计变量
        Long totalCases = 0L;
        Long totalDeaths = 0L;
        //遍历该州的各个县的数据
        for (CovidCountBean value : values) {
            totalCases += value.getCases();
            totalDeaths += value.getDeaths();
        }

        //赋值
        outValue.set(totalCases, totalDeaths);

        //输出结果
        context.write(key, outValue);


    }
}
