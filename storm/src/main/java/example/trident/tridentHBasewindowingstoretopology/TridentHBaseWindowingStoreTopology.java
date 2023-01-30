package example.trident.tridentHBasewindowingstoretopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.trident.windowing.HBaseWindowsStoreFactory;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;

/**
 * 将storm的window状态
 * 需要在hbase里面创建列簇
 * // window-state table should already be created with cf:tuples column
 * create 'window-state','cf'
 */
public class TridentHBaseWindowingStoreTopology {
    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);
        HashMap<String, Object> hbaseConfig = new HashMap<>();
        hbaseConfig.put("hbase.zookeeper.quorum", "bigdata1:2181,bigdata2:2181,bigdata3:2181");
        HBaseWindowsStoreFactory windowStoreFactory =
                new HBaseWindowsStoreFactory(hbaseConfig, "window-state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
                        new Split(), new Fields("word"))
                .window(TumblingCountWindow.of(1000), windowStoreFactory, new Fields("word"), new CountAsAggregator(),
                        new Fields("count"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.err.println("Received tuple: " + input);
                    }
                });
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.put(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT, 100);

        String topoName = "wordCounterWithWindowing";

        conf.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, conf, topology.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }

}
