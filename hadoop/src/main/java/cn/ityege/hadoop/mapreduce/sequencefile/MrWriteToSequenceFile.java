package cn.ityege.hadoop.mapreduce.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName MrWriteToSequenceFile
 * @Description TODO 读取文本文件，转换为SequenceFile文件
 */
public class MrWriteToSequenceFile extends Configured implements Tool {

    //构建、配置、提交一个 MapReduce的Job
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 实例化作业
        Job job = Job.getInstance(conf, "MrWriteToSequenceFile");
        // 设置作业的主程序
        job.setJarByClass(this.getClass());
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path("data\\input\\SequenceFileMapFile\\secondhouse.csv"));
        // 设置Map端的实现类
        job.setMapperClass(WriteSeqFileAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置作业的输出为SequenceFileOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 使用SequenceFile的块级别压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        // 设置Reduce端的实现类
        job.setReducerClass(WriteSeqFileAppReducer.class);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(NullWritable.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(Text.class);
        // 从参数中获取输出路径
        Path outputDir = new Path("data/output/mr/seq1");
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //程序入口，调用run
    public static void main(String[] args) throws Exception {
        //用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new MrWriteToSequenceFile(), args);
        System.exit(status);
    }


    /**
     * 定义Mapper类
     */
    public static class WriteSeqFileAppMapper extends Mapper<LongWritable, Text,NullWritable, Text>{


        private NullWritable outputKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(outputKey, value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }

    }

    /**
     * 定义Reduce类
     */
    public static class WriteSeqFileAppReducer extends Reducer<NullWritable,Text,NullWritable,Text>{

        private NullWritable outputKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = value.iterator();
            while (iterator.hasNext()) {
                context.write(outputKey, iterator.next());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }

    }

}
