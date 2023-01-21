package example.topology.slidingtupletstopology;

import example.topology.bolt.PrinterBolt;
import example.topology.bolt.SlidingWindowSumBolt;
import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

public class SlidingTupleTsTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        BaseWindowedBolt bolt=new SlidingWindowSumBolt()
                .withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS),new BaseWindowedBolt.Duration(3,TimeUnit.SECONDS))
                .withTimestampField("ts")
                .withLag(new BaseWindowedBolt.Duration(5,TimeUnit.SECONDS));
        builder.setSpout("integer",new RandomIntegerSpout(),1);
        builder.setBolt("slidingsum",bolt,1).shuffleGrouping("integer");
        builder.setBolt("printer",new PrinterBolt(),1).shuffleGrouping("slidingsum");
        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
