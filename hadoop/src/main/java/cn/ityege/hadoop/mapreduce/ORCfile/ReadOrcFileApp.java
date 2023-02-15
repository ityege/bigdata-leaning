package cn.ityege.hadoop.mapreduce.ORCfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

/**
 * @ClassName ReadOrcFileApp
 * @Description TODO 读取ORC文件进行解析还原成普通文本文件
 */
public class ReadOrcFileApp extends Configured implements Tool {
    // 作业名称
    private static final String JOB_NAME = WriteOrcFileApp.class.getSimpleName();

    /**
     * 重写Tool接口的run方法，用于提交作业
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        // 实例化作业
        Job job = Job.getInstance(this.getConf(), JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(ReadOrcFileApp.class);
        // 设置作业的输入为OrcInputFormat
        job.setInputFormatClass(OrcInputFormat.class);
        // 设置作业的输入路径
        OrcInputFormat.addInputPath(job, new Path("data/output/mr/orc1"));
        // 设置作业的Mapper类
        job.setMapperClass(ReadOrcFileAppMapper.class);
        // 设置作业使用0个Reduce（直接从map端输出）
        job.setNumReduceTasks(0);
        // 设置作业的输入为TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        // 从参数中获取输出路径
        Path outputDir = new Path("data/output/mr/orc2");
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(this.getConf()).delete(outputDir, true);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }


    //程序入口，调用run
    public static void main(String[] args) throws Exception {
        //用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new ReadOrcFileApp(), args);
        System.exit(status);
    }

    /**
     * 实现Mapper类
     */
    public static class ReadOrcFileAppMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
        private NullWritable outputKey;
        private Text outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputKey = NullWritable.get();
            outputValue = new Text();
        }
        public void map(NullWritable key, OrcStruct value, Context output) throws IOException, InterruptedException {
            //将ORC中的每条数据转换为Text对象
            this.outputValue.set(
                    value.getFieldValue(0).toString()+","+
                            value.getFieldValue(1).toString()+","+
                            value.getFieldValue(2).toString()+","+
                            value.getFieldValue(3).toString()+","+
                            value.getFieldValue(4).toString()+","+
                            value.getFieldValue(5).toString()+","+
                            value.getFieldValue(6).toString()+","+
                            value.getFieldValue(7).toString()
            );
            //输出结果
            output.write(outputKey, outputValue);
        }
    }

}
