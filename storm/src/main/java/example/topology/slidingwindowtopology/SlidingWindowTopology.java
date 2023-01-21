package example.topology.slidingwindowtopology;

import example.topology.bolt.SlidingWindowSumBolt;

import example.topology.bolt.PrinterBolt;
import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

public class SlidingWindowTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RandomIntegerSpout(), 1);
//        滑动窗口
        builder.setBolt("slidingsum", new SlidingWindowSumBolt()
                        .withWindow(BaseWindowedBolt.Count.of(30), BaseWindowedBolt.Count.of(10))
                , 1).shuffleGrouping("integer");
//        滚动窗口
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withTumblingWindow(BaseWindowedBolt.Count.of(3)), 1)
                .shuffleGrouping("slidingsum");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
