package example.topology.lambdatopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.UUID;

/**
 * 通过lambda表达式进行函数式编程
 */
public class LambdaTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String prefix = "Hello lambda:";
        String suffix = ":so cool!";
        builder.setSpout("spout1", () -> UUID.randomUUID().toString());
        builder.setBolt("bolt1", (tuple, collector) -> {
//            Halting process: Async loop died!
                    System.out.println("+++++++++++++++++++++");
                    String[] parts = tuple.getString(0).split("\\-");
                    collector.emit(new Values(prefix + parts[0] + suffix, 999));
                }
                , "strValue", "intValue"
        ).shuffleGrouping("spout1");
        builder.setBolt("bolt2", tuple -> System.err.println(tuple.getValues())).shuffleGrouping("bolt1");
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(2000000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
