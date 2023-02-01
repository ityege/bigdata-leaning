package example.streams.joinexample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class JoinExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        // a stream of (number, square) pairs
        PairStream<Integer, Integer> squares = builder
                .newStream(new NumberSpout(x -> x * x),
                        new PairValueMapper<>(0, 1));
        // a stream of (number, cube) pairs
        PairStream<Integer, Integer> cubes = builder
                .newStream(new NumberSpout(x -> x * x * x),
                        new PairValueMapper<>(0, 1));
        // create a windowed stream of five seconds duration
        squares.window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(5)))
                /*
                 * Join the squares and the cubes stream within the window.
                 * The values in the squares stream having the same key as that
                 * of the cubes stream within the window will be joined together.
                 */
                .join(cubes)
                /**
                 * The results should be of the form (number, (square, cube))
                 */
                .print();
        Config config = new Config();
        String topoName = "JoinExample";

        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }

    private static class NumberSpout extends BaseRichSpout {
        private final Function<Integer, Integer> function;
        private SpoutOutputCollector collector;
        private int count = 1;

        NumberSpout(Function<Integer, Integer> function) {
            this.function = function;
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(990);
            collector.emit(new Values(count, function.apply(count)));
            count++;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "val"));
        }
    }
}
