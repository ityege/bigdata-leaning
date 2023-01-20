package example.topology.statefultopology;

import example.topology.bolt.StatefulSumBolt;
import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class StatefulTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout(), 1);
        builder.setBolt("partialsum", new StatefulSumBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("partialsum");
        builder.setBolt("total", new StatefulSumBolt(), 1).shuffleGrouping("printer");
        builder.setBolt("printtotal", new PrinterBolt(), 1).shuffleGrouping("total");
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
