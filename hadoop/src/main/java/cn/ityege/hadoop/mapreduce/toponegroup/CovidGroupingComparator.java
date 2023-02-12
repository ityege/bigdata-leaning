package cn.ityege.hadoop.mapreduce.toponegroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CovidGroupingComparator extends WritableComparator {

    protected CovidGroupingComparator(){
        super(CovidBean.class,true);//允许创建对象实例
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //类型转换
        CovidBean aBean = (CovidBean) a;
        CovidBean bBean = (CovidBean) b;

        //本需求中 分组规则是，只要前后两个数据的state一样 就应该分到同一组。
        //只要compare 返回0  mapreduce框架就认为两个一样  返回不为0 就认为不一样
        return aBean.getState().compareTo(bBean.getState());
    }
}
