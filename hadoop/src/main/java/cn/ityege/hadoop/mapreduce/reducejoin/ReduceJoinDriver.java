package cn.ityege.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* K相同输出到,到reduce端key相同进行join
 */
public class ReduceJoinDriver {
    public static void main(String[] args) throws Exception{

        //创建配置对象
        Configuration conf = new Configuration();

        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, ReduceJoinDriver.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(ReduceJoinDriver.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path("data\\input\\join");
        Path output = new Path("data\\output\\join");
        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //todo 判断输出路径是否已经存在 如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);
        }

        //采用waitForCompletion提交job 参数表示是否开启实时监视追踪作业的执行情况
        boolean resultflag = job.waitForCompletion(true);
        //退出程序 和job结果进行绑定
        System.exit(resultflag ? 0: 1);
    }
}
