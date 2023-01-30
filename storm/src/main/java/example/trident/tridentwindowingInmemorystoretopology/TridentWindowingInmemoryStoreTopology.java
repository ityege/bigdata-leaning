package example.trident.tridentwindowingInmemorystoretopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.SlidingCountWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TridentWindowingInmemoryStoreTopology {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(3);
        WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();
        String topoName = "wordCounter";
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout1", spout).parallelismHint(1).each(
                        new Fields("sentence"), new Split(), new Fields("word")
                ).window(SlidingCountWindow.of(1000, 100), mapState, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.err.println(input.getValues());
                    }
                });

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, topology.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }
}
