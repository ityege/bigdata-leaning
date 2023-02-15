package cn.ityege.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ReduceJoinReducer extends Reducer<Text,Text,Text,Text> {

    //创建集合 用于保存订单数据和商品数据 便于后续join关联拼接数据

    //用来保存 商品编号、商品名称
    List<String> goodsList = new ArrayList<String>();
    //用来保存 订单编号、实际支付价格
    List<String> ordersList = new ArrayList<String>();

    Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历values
        for (Text value : values) {
            //判断数据是订单数据 还是商品数据
            if(value.toString().startsWith("goods#")){
                String s = value.toString().split("#")[1];
                //添加到商品集合中
                goodsList.add(s);
            }
            if(value.toString().startsWith("orders#")){
                String s = value.toString().split("#")[1];
                //添加到订单集合中
                ordersList.add(s);
            }
        }

        //获取两个集合的长度
        int goodsSize = goodsList.size();
        int ordersSize = ordersList.size();

        for (int i = 0; i < ordersSize; i++) {
            for (int j = 0; j < goodsSize; j++) {
                outValue.set(ordersList.get(i)+"\t"+goodsList.get(j));
                //最终输出：商品ID,订单编号，实际支付价格，商品编号，商品名称
                context.write(key,outValue);
            }
        }

        //清空集合
        ordersList.clear();
        goodsList.clear();
    }
}
