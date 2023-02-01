package example.streams.aggregateexample;

import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

public class AggregateExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();

        builder.newStream(new RandomIntegerSpout(), new ValueMapper<Integer>(0), 2)
                .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(5)))
                .filter(x -> x > 0 && x < 500)
                .aggregate(new Avg())
                .print();

        Config config = new Config();
        String topoName = "AGG_EXAMPLE";
        config.setNumWorkers(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }
    private static class Avg implements CombinerAggregator<Integer, Pair<Integer, Integer>, Double> {
        @Override
        public Pair<Integer, Integer> init() {
            return Pair.of(0, 0);
        }

        @Override
        public Pair<Integer, Integer> apply(Pair<Integer, Integer> sumAndCount, Integer value) {
            return Pair.of(sumAndCount.value1 + value, sumAndCount.value2 + 1);
        }

        @Override
        public Pair<Integer, Integer> merge(Pair<Integer, Integer> sumAndCount1, Pair<Integer, Integer> sumAndCount2) {
            System.out.println("Merge " + sumAndCount1 + " and " + sumAndCount2);
            return Pair.of(
                    sumAndCount1.value1 + sumAndCount2.value1,
                    sumAndCount1.value2 + sumAndCount2.value2
            );
        }

        @Override
        public Double result(Pair<Integer, Integer> sumAndCount) {
            return (double) sumAndCount.value1 / sumAndCount.value2;
        }
    }
}
