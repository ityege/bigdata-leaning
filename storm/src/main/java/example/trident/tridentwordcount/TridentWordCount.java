package example.trident.tridentwordcount;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentWordCount {
    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(1).each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);

        topology.newDRPCStream("words").each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("word"), new FilterNull())
                .project(new Fields("word", "count"));

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        String topoName = "wordCounter";
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topology.build());

    }

    private static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
}
