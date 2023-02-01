package example.streams.groupbykeyandwindowexample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupByKeyAndWindowExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();

        // a stream of stock quotes
        builder.newStream(new StockQuotes(), new PairValueMapper<String, Double>(0, 1))
                /*
                 * The elements having the same key within the window will be grouped
                 * together and the corresponding values will be merged.
                 *
                 * The result is a PairStream<String, Iterable<Double>> with
                 * 'stock symbol' as the key and 'stock prices' for that symbol within the window as the value.
                 */
                .groupByKeyAndWindow(SlidingWindows.of(BaseWindowedBolt.Count.of(6), BaseWindowedBolt.Count.of(3)))
                .print();
        // a stream of stock quotes
        builder.newStream(new StockQuotes(), new PairValueMapper<String, Double>(0, 1))
                /*
                 * The elements having the same key within the window will be grouped
                 * together and their values will be reduced using the given reduce function.
                 *
                 * Here the result is a PairStream<String, Double> with
                 * 'stock symbol' as the key and the maximum price for that symbol within the window as the value.
                 */
                .reduceByKeyAndWindow((x, y) -> x > y ? x : y, SlidingWindows.of(BaseWindowedBolt.Count.of(6), BaseWindowedBolt.Count.of(3)))
                .print();
        Config config = new Config();
        String topoName = "GroupByKeyAndWindowExample";

        config.setNumWorkers(1);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }
    private static class StockQuotes extends BaseRichSpout {
        private final List<List<Values>> values = Arrays.asList(
                Arrays.asList(new Values("AAPL", 100.0), new Values("GOOG", 780.0), new Values("FB", 125.0)),
                Arrays.asList(new Values("AAPL", 105.0), new Values("GOOG", 790.0), new Values("FB", 130.0)),
                Arrays.asList(new Values("AAPL", 102.0), new Values("GOOG", 788.0), new Values("FB", 128.0))
        );
        private SpoutOutputCollector collector;
        private int index = 0;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(5000);
            for (Values v : values.get(index)) {
                collector.emit(v);
            }
            index = (index + 1) % values.size();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("symbol", "price"));
        }
    }
}
