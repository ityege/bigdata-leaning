package example.topology.statefulwindowingtopology;

import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

public class StatefulWindowingTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout());
        builder.setBolt("sumbolt", new WindowSumBolt()
                .withWindow(new BaseWindowedBolt.Count(5), new BaseWindowedBolt.Count(3))
                .withMessageIdField("msgid"), 1
        ).shuffleGrouping("spout");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("sumbolt");
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
