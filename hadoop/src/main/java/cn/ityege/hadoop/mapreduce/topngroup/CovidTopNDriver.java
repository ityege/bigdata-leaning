package cn.ityege.hadoop.mapreduce.topngroup;

import cn.ityege.hadoop.mapreduce.beans.CovidBean;
import cn.ityege.hadoop.mapreduce.toponegroup.CovidGroupingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CovidTopNDriver {
    public static void main(String[] args) throws Exception{

        //创建配置对象
        Configuration conf = new Configuration();

        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, CovidTopNDriver.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(CovidTopNDriver.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(CovidTopNMapper.class);
        job.setReducerClass(CovidTopNReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(CovidBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(CovidBean.class);
        job.setOutputValueClass(LongWritable.class);

        //todo 如果重写了分组规则 好需要在job中进行设置 才能生效
        job.setGroupingComparatorClass(CovidGroupingComparator.class);

        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path("data\\input\\covid19\\us-covid19-counties.dat");
        Path output = new Path("data\\output\\group");
        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //todo 判断输出路径是否已经存在 如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);//rm -rf
        }

        //最终提交本次job作业
//        job.submit();
        //采用waitForCompletion提交job 参数表示是否开启实时监视追踪作业的执行情况
        boolean resultflag = job.waitForCompletion(true);
        //退出程序 和job结果进行绑定
        System.exit(resultflag ? 0: 1);
    }
}
