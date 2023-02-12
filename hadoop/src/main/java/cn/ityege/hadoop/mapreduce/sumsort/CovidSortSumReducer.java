package cn.ityege.hadoop.mapreduce.sumsort;

import cn.ityege.hadoop.mapreduce.beans.CovidCountBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 没有实现分组,谁有根据内存地址,每一个对象都不一样
 * 按照key排序,按照确诊数进行排序
 */
public class CovidSortSumReducer extends Reducer<CovidCountBean,Text, Text,CovidCountBean> {
    @Override
    protected void reduce(CovidCountBean key, Iterable<Text> values, Reducer<CovidCountBean, Text, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        //排序好之后 reduce会进行分组操作  分组规则是判断key是否相等
        //本业务中 使用自定义对象作为key 并且没有重写分组规则 默认就会比较对象的地址  导致每个kv对自己就是一组
        //  <CovidCountBean,加州> ---><CovidCountBean,Iterable[加州]>
        //  <CovidCountBean,德州> ---><CovidCountBean,Iterable[德州]>

        Text outKey = values.iterator().next();//州
        context.write(outKey,key);
    }
}
