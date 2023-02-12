package cn.ityege.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class StatePartitioner extends Partitioner<Text, Text> {

    //模拟美国各州的数据字典，实际中可以从redis进行读取加载 如果数据量不大 也可以创建数据集合保存
    public static HashMap<String, Integer> stateMap = new HashMap<String, Integer>();

    static {
        stateMap.put("Alabama", 0);
        stateMap.put("Alaska", 1);
        stateMap.put("Arkansas", 2);
        stateMap.put("California", 3);
        stateMap.put("Colorado", 4);
    }

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        Integer code = stateMap.get(key.toString());
        if (code != null) {
            return code;
        }
        return 5;
    }
}
