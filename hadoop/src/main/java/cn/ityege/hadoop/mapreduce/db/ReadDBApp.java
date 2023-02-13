package cn.ityege.hadoop.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 这个任务本地可以运行,但是打成jar包,提交到yarn上面一直报mysql的驱动类找不到
 */
public class ReadDBApp extends Configured implements Tool {
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
        Job job = Job.getInstance(conf, ReadDBApp.class.getSimpleName());
        // 设置作业驱动类
        job.setJarByClass(ReadDBApp.class);

        //设置inputformat类
        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path("data/output/db"));

        job.setMapperClass(ReadDBMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        //配置当前作业要查询的SQL语句和接收查询结果的JavaBean

        DBInputFormat.setInput(
                job,
                StudentBean.class,
                "SELECT sid,sname,ssex,chinese,math,english,score FROM test.student",
                "SELECT count(*) from test.student"
        );

        return  job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new ReadDBApp(), args);
        System.exit(status);
    }
}