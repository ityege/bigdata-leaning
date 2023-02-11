package cn.ityege.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration conf = new Configuration();
        //todo 使用工具类ToolRunner提交程序
        int status = ToolRunner.run(conf, new WordCountDriver2(), args);
        //退出客户端
        System.exit(status);
    }
    @Override
    public int run(String[] args) throws Exception {
        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(getConf(), WordCountDriver2.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(WordCountDriver2.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //配置本次作业的输入数据路径 和输出数据路径
        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job,new Path("data\\input\\wordcount.txt"));
        FileOutputFormat.setOutputPath(job,new Path("data\\output\\wordcountoutput"));


        return job.waitForCompletion(true)? 0:1;
    }
}
