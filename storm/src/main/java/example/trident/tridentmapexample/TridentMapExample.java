package example.trident.tridentmapexample;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class TridentMapExample {
    public static void main(String[] args) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("word"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16)
                .flatMap(split)
                .map(toUpper, new Fields("uppercased"))
                .filter(theFilter)
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.out.println(input.getString(0));
                    }
                })
                .groupBy(new Fields("uppercased"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream("words")
                .flatMap(split, new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .filter(new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        String topoName = "wordCounter";
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topology.build());
    }

    private static MapFunction toUpper = new MapFunction() {
        @Override
        public Values execute(TridentTuple input) {
            return new Values(input.getStringByField("word").toUpperCase());
        }
    };

    private static FlatMapFunction split = new FlatMapFunction() {
        @Override
        public Iterable<Values> execute(TridentTuple input) {
            List<Values> valuesList = new ArrayList<>();
            for (String word : input.getString(0).split(" ")) {
                valuesList.add(new Values(word));
            }
            return valuesList;
        }
    };

    private static Filter theFilter = new BaseFilter() {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getString(0).equals("THE");
        }
    };
}
