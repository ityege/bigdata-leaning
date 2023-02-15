package cn.ityege.hadoop.mapreduce.ORCfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @ClassName WriteOrcFileApp
 * @Description TODO 用于读取普通文本文件转换为ORC文件
 */
public class WriteOrcFileApp extends Configured implements Tool {
    // 作业名称
    private static final String JOB_NAME = WriteOrcFileApp.class.getSimpleName();
    //构建日志监听
    private static final Logger LOG = LoggerFactory.getLogger(WriteOrcFileApp.class);
    //定义数据的字段信息
    private static final String SCHEMA = "struct<id:string,type:string,orderID:string,bankCard:string,cardType:string,ctime:string,utime:string,remark:string>";


    /**
     * 重写Tool接口的run方法，用于提交作业
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // 设置Schema
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(this.getConf(), SCHEMA);
        // 实例化作业
        Job job = Job.getInstance(this.getConf(), JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(WriteOrcFileApp.class);
        // 设置作业的Mapper类
        job.setMapperClass(WriteOrcFileAppMapper.class);
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输出为OrcOutputFormat
        job.setOutputFormatClass(OrcOutputFormat.class);
        // 设置作业使用0个Reduce（直接从map端输出）
        job.setNumReduceTasks(0);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path("data\\input\\ORCFile\\pay.csv"));
        // 从参数中获取输出路径
        Path outputDir = new Path("data/output/mr/orc1");
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(this.getConf()).delete(outputDir, true);
        // 设置作业的输出路径
        OrcOutputFormat.setOutputPath(job, outputDir);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //程序入口，调用run
    public static void main(String[] args) throws Exception {
        //用于管理当前程序的所有配置
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WriteOrcFileApp(), args);
        System.exit(status);
    }

    /**
     * 实现Mapper类
     */
    public static class WriteOrcFileAppMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {
        //获取字段描述信息
        private TypeDescription schema = TypeDescription.fromString(SCHEMA);
        //构建输出的Key
        private final NullWritable outputKey = NullWritable.get();
        //构建输出的Value为ORCStruct类型
        private final OrcStruct outputValue = (OrcStruct) OrcStruct.createValue(schema);
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            //将读取到的每一行数据进行分割，得到所有字段
            String[] fields = value.toString().split(",",8);
            //将所有字段赋值给Value中的列
            outputValue.setFieldValue(0, new Text(fields[0]));
            outputValue.setFieldValue(1, new Text(fields[1]));
            outputValue.setFieldValue(2, new Text(fields[2]));
            outputValue.setFieldValue(3, new Text(fields[3]));
            outputValue.setFieldValue(4, new Text(fields[4]));
            outputValue.setFieldValue(5, new Text(fields[5]));
            outputValue.setFieldValue(6, new Text(fields[6]));
            outputValue.setFieldValue(7, new Text(fields[7]));
            //输出KeyValue
            output.write(outputKey, outputValue);
        }
    }



}
