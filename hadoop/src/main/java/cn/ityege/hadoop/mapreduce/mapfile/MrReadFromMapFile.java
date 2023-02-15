package cn.ityege.hadoop.mapreduce.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName MrReadFromMapFile
 * @Description TODO 读取MapFile文件，转换为普通文本文件
 */
public class MrReadFromMapFile extends Configured implements Tool {

    //构建、配置、提交一个 MapReduce的Job
    public int run(String[] args) throws Exception {
        // 实例化作业
        Job job = Job.getInstance(this.getConf(), "MrReadFromMapFile");
        // 设置作业的主程序
        job.setJarByClass(this.getClass());
        // 设置作业的输入为SequenceFileInputFormat（Hadoop没有直接提供MapFileInput）
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 设置作业的输入路径
        SequenceFileInputFormat.addInputPath(job, new Path("data/output/mr/map1"));
        // 设置Map端的实现类
        job.setMapperClass(ReadMapFileAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置作业的输出为SequenceFileOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置Reduce端的实现类
        job.setReducerClass(ReadMapFileAppReducer.class);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(NullWritable.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(Text.class);
        // 从参数中获取输出路径
        Path outputDir = new Path("data/output/mr/map2");
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(this.getConf()).delete(outputDir, true);
        // 设置作业的输出路径
        TextOutputFormat.setOutputPath(job, outputDir);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //程序入口，调用run
    public static void main(String[] args) throws Exception {
        //用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new MrReadFromMapFile(), args);
        System.exit(status);
    }


    /**
     * 定义Mapper类
     */
    public static class ReadMapFileAppMapper extends Mapper<IntWritable, Text, NullWritable, Text> {


        private NullWritable outputKey = NullWritable.get();

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(outputKey, value);
        }

    }

    /**
     * 定义Reduce类
     */
    public static class ReadMapFileAppReducer extends Reducer<NullWritable,Text,NullWritable,Text>{

        @Override
        protected void reduce(NullWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = value.iterator();
            while (iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }

}
