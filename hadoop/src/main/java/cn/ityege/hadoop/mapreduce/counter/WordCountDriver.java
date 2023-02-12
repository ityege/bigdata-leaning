package cn.ityege.hadoop.mapreduce.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // 创建作业实例
        Job job = Job.getInstance(getConf(), WordCountDriver.class.getSimpleName());
        // 设置作业驱动类
        job.setJarByClass(this.getClass());

        // 设置作业mapper reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置作业mapper阶段输出key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置作业reducer阶段输出key value数据类型 也就是程序最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 配置作业的输入数据路径
        FileInputFormat.addInputPath(job, new Path("data\\input\\wordcount.txt"));
        // 配置作业的输出数据路径
        FileOutputFormat.setOutputPath(job, new Path("data/output/counter"));

        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path("data/output/counter"))) {
            fs.delete(new Path("data/output/counter"), true);
        }
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        //配置文件对象
        Configuration conf = new Configuration();
        //使用工具类ToolRunner提交程序
        int status = ToolRunner.run(conf, new WordCountDriver(), args);
        //退出客户端程序 客户端退出状态码和MapReduce程序执行结果绑定
        System.exit(status);
    }
}