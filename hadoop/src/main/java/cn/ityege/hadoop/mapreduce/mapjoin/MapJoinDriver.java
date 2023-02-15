package cn.ityege.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
*map端join,不需要reduce了,把其中一个小表进行分布式缓存
 */
public class MapJoinDriver {
    public static void main(String[] args) throws Exception{

        //创建配置对象
        Configuration conf = new Configuration();

        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, MapJoinDriver.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(MapJoinDriver.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(MapJoinMapper.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //todo 注意在map端join的时候 已经不需要reducetask了
        job.setNumReduceTasks(0);

        //todo 添加分布式缓存文件
        job.addCacheFile(new URI("/input/goods/itheima_goods.txt"));

        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path("/input/goods");
        Path output = new Path("/output/goods");
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
