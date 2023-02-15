package cn.ityege.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @description:  调整一行数据中字段顺序  把属于同一笔订单的不同商品聚集在一起  以订单编号作为key
 */
public class ReduceJoinSort {

    public static class ReduceJoinSortMapper extends Mapper<LongWritable, Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            outKey.set(fields[1]);//订单编号作为key
            //输出的结果：订单编号 商品ID 商品编码 商品名称 实际成交金额
            outValue.set(fields[1]+"\t"+fields[0]+"\t"+fields[3]+"\t"+fields[4]+"\t"+fields[2]);

            context.write(outKey,outValue);
        }
    }

    public static class ReduceJoinSortReducer extends Reducer<Text,Text,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,NullWritable.get());//订单编号 商品ID 商品编码 商品名称 实际成交金额
            }
        }
    }


    public static void main(String[] args) throws Exception{
        //创建配置对象
        Configuration conf = new Configuration();

        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, ReduceJoinSort.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(ReduceJoinSort.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(ReduceJoinSortMapper.class);
        job.setReducerClass(ReduceJoinSortReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path("data\\output\\join");
        Path output = new Path("data\\output\\joinsort");
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
