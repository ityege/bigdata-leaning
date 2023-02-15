package cn.ityege.hadoop.mapreduce.compress.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName MRMapOutputSnappy
 * @Description TODO 读取普通文件数据，对Map输出的数据以Snappy格式进行压缩
 */
public class MRMapOutputSnappy extends Configured implements Tool {

    //构建、配置、提交一个 MapReduce的Job
    public int run(String[] args) throws Exception {

        //构建Job
        Job job = Job.getInstance(this.getConf(),this.getClass().getSimpleName());
        job.setJarByClass(MRMapOutputSnappy.class);

        //input：配置输入
        Path inputPath = new Path("data\\input\\Compress\\SogouQ.reduced");
        TextInputFormat.setInputPaths(job,inputPath);

        //map：配置Map
        job.setMapperClass(MrMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //reduce：配置Reduce
        job.setReducerClass(MrReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        //output：配置输出
        Path outputPath = new Path("data\\output\\mr\\Compress\\snappy3");
        TextOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    //程序入口，调用run
    public static void main(String[] args) throws Exception {
        //用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        //配置Map输出结果压缩为Snappy格式
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        //配置Reduce输出结果压缩为Snappy格式
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        //调用run方法，提交运行Job
        int status = ToolRunner.run(conf, new MRMapOutputSnappy(), args);
        System.exit(status);
    }


    /**
     * 定义Mapper类
     */
    public static class MrMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

        private NullWritable outputKey = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //直接输出每条数据
            context.write(this.outputKey,value);
        }
    }

    /**
     * 定义Reduce类
     */
    public static class MrReduce extends Reducer<NullWritable, Text,NullWritable, Text> {

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //直接输出每条数据
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

}
