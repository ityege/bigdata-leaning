package cn.ityege.hadoop.mapreduce.jobflow;


import cn.ityege.hadoop.mapreduce.reducejoin.ReduceJoinDriver;
import cn.ityege.hadoop.mapreduce.reducejoin.ReduceJoinMapper;
import cn.ityege.hadoop.mapreduce.reducejoin.ReduceJoinReducer;
import cn.ityege.hadoop.mapreduce.reducejoin.ReduceJoinSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * mapreduce 工作流
 */
public class MRJobFlow {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        //第一个作业的job
        Job job1 = Job.getInstance(conf, ReduceJoinDriver.class.getSimpleName());
        //设置mr程序运行的主类
        job1.setJarByClass(ReduceJoinDriver.class);
        //设置本次mr程序的mapper类型  reducer类
        job1.setMapperClass(ReduceJoinMapper.class);
        job1.setReducerClass(ReduceJoinReducer.class);
        //指定mapper阶段输出的key value数据类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //配置本次作业的输入数据路径 和输出数据路径
        FileInputFormat.setInputPaths(job1,new Path("data\\input\\join"));
        FileOutputFormat.setOutputPath(job1,new Path("data\\output\\join"));
        //todo 将普通的作用包装成受控作业
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);

        //第二个作业的job
        Job job2 = Job.getInstance(conf, ReduceJoinSort.class.getSimpleName());
        //设置mr程序运行的主类
        job2.setJarByClass(ReduceJoinSort.class);
        //设置本次mr程序的mapper类型  reducer类
        job2.setMapperClass(ReduceJoinSort.ReduceJoinSortMapper.class);
        job2.setReducerClass(ReduceJoinSort.ReduceJoinSortReducer.class);
        //指定mapper阶段输出的key value数据类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job2,new Path("data\\output\\join"));
        FileOutputFormat.setOutputPath(job2,new Path("data\\output\\joinsort"));

        //todo 将普通的作用包装成受控作业
        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);

        //todo 设置作业之间的依赖关系
        cj2.addDependingJob(cj1);

        //todo 创建主控制器 控制上面两个作业 一起提交
        JobControl jc = new JobControl("myctrl");
        jc.addJob(cj1);
        jc.addJob(cj2);

        //使用线程启动JobControl
        Thread t = new Thread(jc);
        t.start();

        while (true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                break;
            }
        }
    }
}
