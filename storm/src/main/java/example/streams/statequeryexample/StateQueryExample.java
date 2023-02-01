package example.streams.statequeryexample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.StreamState;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * statequery并且将状态保存到redis中
 * 终于找到在哪里配置redis了
 */
public class StateQueryExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        StreamState<String, Long> ss = builder.newStream(new TestWordSpout(), new ValueMapper<String>(0), 2)
                /*
                 * Transform the stream of words to a stream of (word, 1) pairs
                 */
                .mapToPair(w -> Pair.of(w, 1))
                /*
                 *  Update the count in the state. Here the first argument 0L is the initial value for the
                 *  count and
                 *  the second argument is a function that increments the count for each value received.
                 */
                .updateStateByKey(0L, (count, val) -> count + 1);
        /*
         * A stream of words emitted by the QuerySpout is used as
         * the keys to query the state.
         */
        builder.newStream(new QuerySpout(), new ValueMapper<String>(0))
                /*
                 * Queries the state and emits the
                 * matching (key, value) as results. The stream state returned
                 * by the updateStateByKey is passed as the argument to stateQuery.
                 */
                .stateQuery(ss).print();

        Config config = new Config();
        // use redis based state store for persistence
        config.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        config.put("topology.state.provider.config","{\n" +
                "\t\"jedisPoolConfig\": {\n" +
                "\t\t\"host\": \"docker\",\n" +
                "\t\t\"port\": 6379,\n" +
                "\t\t\"timeout\": 2000,\n" +
                "\t\t\"database\": 0\n" +
                "\t}\n" +
                "}");
        String topoName = "test";
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();


    }

    private static class QuerySpout extends BaseRichSpout {
        private final String[] words = { "nathan", "mike" };
        private SpoutOutputCollector collector;

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(2000);
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
}
