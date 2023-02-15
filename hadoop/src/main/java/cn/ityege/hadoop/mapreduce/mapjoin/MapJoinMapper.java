package cn.ityege.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    //创建集合 用于缓存商品数据
    Map<String,String> goodsMap = new HashMap<String, String>();

    Text outKey = new Text();

    /**
     * todo 在程序初始化的方法中 从分布式缓存文件中加载内容：itheima_goods.txt 到本程序自己创建的集合中 便于map方法中join操作
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取缓存的文件，并把文件内容封装到集合 pd.txt
        URI[]  cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        System.err.println("分布式缓存的文件:"+ Arrays.toString(cacheFiles));
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line = null;
        while ((line =br.readLine()) != null){
            //100111|155084782094837|黄冠梨4个单果约250g-300g新鲜水果
            //商品ID 商品编号  商品名称
            String[] fields = line.split("\\|");
            //把读取的分布式缓存内容添加到集合中
            goodsMap.put(fields[0],fields[1]+"\t"+fields[2]);  //k:商品ID v:商品编码 商品名称
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //526|111032|1899  [订单编号，商品ID,实际成交价格]
        String[] fields = value.toString().split("\\|");
        //todo 根据订单数据中的商品ID 在缓存集合中找出来对应的商品的名称及相关信息 完成拼接 也就是所谓的join关联
        String goodsInfo = goodsMap.get(fields[1]);
        outKey.set(value.toString()+"\t"+goodsInfo);
        context.write(outKey,NullWritable.get());
    }
}
