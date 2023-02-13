package cn.ityege.hadoop.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 这个任务本地可以运行,但是打成jar包,提交到yarn上面一直报mysql的驱动类找不到
 */
public class WriteDBApp extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        //配置当前作业需要使用的JDBC信息
        DBConfiguration.configureDB(
                conf,
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://192.168.184.128:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT&allowPublicKeyRetrieval=true",
                "root",
                "123456"
        );

        // 创建作业实例
        Job job = Job.getInstance(conf, WriteDBApp.class.getSimpleName());
        // 设置作业驱动类
        job.setJarByClass(WriteDBApp.class);

        //设置mapper相关信息
        job.setMapperClass(WriteDBMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(StudentBean.class);

        //设置reducer相关信息
        job.setReducerClass(WriteDBReducer.class);
        job.setOutputKeyClass(StudentBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入的文件的路径
        FileInputFormat.setInputPaths(job,new Path("data/output/db"));

        //设置输出的format类型
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setNumReduceTasks(10);
        //配置当前作业输出到数据库的表、字段信息
        DBOutputFormat.setOutput(
               job,
                "studentout",
                "sid", "sname", "ssex", "chinese", "math","english","score");

        return  job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WriteDBApp(), args);
        System.exit(status);
    }
}