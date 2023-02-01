package example.streams.typedtupleexample;

import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.TupleValueMappers;
import org.apache.storm.streams.tuple.Tuple3;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

public class TypedTupleExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        Stream<Tuple3<Integer, Long, Long>> stream = builder.newStream(new RandomIntegerSpout(), TupleValueMappers.of(0, 1, 2));
        PairStream<Long, Integer> pairs = stream.mapToPair(t -> Pair.of(t.value2 / 10000, t.value1));
        pairs.window(TumblingWindows.of(BaseWindowedBolt.Count.of(10))).groupByKey().print();
        String topoName = "test";
        Config config = new Config();
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }
}
