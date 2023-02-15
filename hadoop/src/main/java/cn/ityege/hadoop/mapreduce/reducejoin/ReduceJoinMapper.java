package cn.ityege.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text outKey = new Text();
    Text outValue = new Text();
    StringBuilder sb = new StringBuilder();
    String fileName =null;

    //maptask的初始化方法 获取当前处理的切片所属的文件名称
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取当前处理的切片
        FileSplit split = (FileSplit) context.getInputSplit();
        //切片所属的文件名称 也就是当前Task处理的数据是属于哪一个文件的
        fileName = split.getPath().getName();
        System.out.println("当前处理的文件是："+fileName);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //清空sb
        sb.setLength(0);

        //切割处理读取的一行数据
        String[] fields = value.toString().split("\\|");

        //判断当前处理的文件是哪个
        if (fileName.contains("itheima_goods.txt")){//商品数据
            //  商品数据格式： 100111|155084782094837|黄冠梨4个单果约250g-300g新鲜水果
            outKey.set(fields[0]);

            sb.append(fields[1]).append("\t").append(fields[2]);
            outValue.set(sb.insert(0,"goods#").toString());
            context.write(outKey,outValue);

        }else{//处理的就是订单数据
        //订单数据格式：  1|107860|7191 (订单编号 商品ID 实际支付价格)
            outKey.set(fields[1]);
            sb.append(fields[0]).append("\t").append(fields[2]);
            outValue.set(sb.insert(0,"orders#").toString());
            context.write(outKey,outValue);
        }
    }
}
