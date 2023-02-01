package example.streams.statefulwordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

public class StatefulWordCount {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        // a stream of words
        builder.newStream(new TestWordSpout(), new ValueMapper<String>(0), 1)
                .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(2)))
                /*
                 * create a stream of (word, 1) pairs
                 */
                .mapToPair(w -> Pair.of(w, 1))
                /*
                 * compute the word counts in the last two second window
                 */
                .countByKey()
                /*
                 * update the word counts in the state.
                 * Here the first argument 0L is the initial value for the state
                 * and the second argument is a function that adds the count to the current value in the state.
                 */
                .updateStateByKey(0L, (state, count) -> state + count)
                /*
                 * convert the state back to a stream and print the results
                 */
                .toPairStream()
                .print();
        Config config = new Config();
        // use redis based state store for persistence
        config.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");
        String topoName = "test";
        config.setNumWorkers(1);
        config.put("topology.state.provider.config","{\n" +
                "\t\"jedisPoolConfig\": {\n" +
                "\t\t\"host\": \"docker\",\n" +
                "\t\t\"port\": 6379,\n" +
                "\t\t\"timeout\": 2000,\n" +
                "\t\t\"database\": 0\n" +
                "\t}\n" +
                "}");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }
}
