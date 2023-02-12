package cn.ityege.hadoop.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CovidPartitionDriver {
    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration conf = new Configuration();
        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, CovidPartitionDriver.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(CovidPartitionDriver.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(CovidPartitionMapper.class);
        job.setReducerClass(CovidPartitionReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //探究：reducetask个数和分区个数之间的关系
        //正常情况下：  reducetask个数 == 分区个数
        //           reducetask个数 > 分区个数    程序可以运行 只不过有空文件产生  浪费性能
        //           reducetask个数 < 分区个数    程序直接报错 错误信息：Illegal partition非法分区
        job.setNumReduceTasks(10);
        job.setPartitionerClass(StatePartitioner.class);
        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path("data\\input\\covid19\\us-covid19-counties.dat");
        Path output = new Path("data\\output\\partition");
        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        //todo 判断输出路径是否已经存在 如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        //最终提交本次job作业
//        job.submit();
        //采用waitForCompletion提交job 参数表示是否开启实时监视追踪作业的执行情况
        boolean resultFlag = job.waitForCompletion(true);
        //退出程序 和job结果进行绑定
        System.exit(resultFlag ? 0 : 1);

    }
}
