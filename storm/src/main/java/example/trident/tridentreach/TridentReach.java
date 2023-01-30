package example.trident.tridentreach;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.ReadOnlyState;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.ReadOnlyMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

public class TridentReach {
    private static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
        {
            put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
            put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
            put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
        }
    };

    private static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
        {
            put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
            put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
            put("tim", Arrays.asList("alex"));
            put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
            put("adam", Arrays.asList("david", "carissa"));
            put("mike", Arrays.asList("john", "bob"));
            put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
        }
    };

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        TridentTopology topology = new TridentTopology();
        TridentState urlToTweeters = topology.newStaticState(new Factory(TWEETERS_DB));
        TridentState tweetersToFollowers = topology.newStaticState(new Factory(FOLLOWERS_DB));
        topology
                .newDRPCStream("reach")
                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
                .each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter")).shuffle()
                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
                .each(new Fields("followers"), new ExpandList(), new Fields("follower"))
                .groupBy(new Fields("follower")).aggregate(new One(), new Fields("one"))
                .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
        StormSubmitter.submitTopology("reach", config, topology.build());
    }

    private static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {

        private Map map;

        public StaticSingleKeyMapState(Map map) {
            this.map = map;
        }

        @Override
        public List<Object> multiGet(List<List<Object>> keys) {
            List<Object> ret = new ArrayList<>();
            for (List<Object> key : keys) {
                Object singleKey = key.get(0);
                ret.add(map.get(singleKey));
            }
            return ret;
        }

    }

    private static class Factory implements StateFactory {
        private Map map;

        public Factory(Map map) {
            this.map = map;
        }

        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new StaticSingleKeyMapState(map);
        }
    }

    private static class One implements CombinerAggregator<Integer> {

        @Override
        public Integer init(TridentTuple tuple) {
            return 1;
        }

        @Override
        public Integer combine(Integer val1, Integer val2) {
            return 1;
        }

        @Override
        public Integer zero() {
            return 1;
        }
    }

    private static class ExpandList extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            List l = (List) tuple.getValue(0);
            if (l != null) {
                for (Object o : l) {
                    collector.emit(new Values(o));
                }
            }
        }
    }
}
