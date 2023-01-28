package example.topology.reachtopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

/**
 * 统计每一个网址关注的人员数量
 */
public class ReachTopology {
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
        {
            put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
            put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
            put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
        }
    };

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
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
//        这个drpc构造器不知道为什么不需要指定前面的bolt
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
        Config config = new Config();
        config.setNumWorkers(6);
        String topoName = "reach-drpc";
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology(topoName, config, builder.createLocalTopology(drpc));

        String[] urlsToTry = new String[]{"foo.com/blog/1"
//                , "engineering.twitter.com/blog/5", "notaurl.com"
        };
        for (String url : urlsToTry) {
            System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
        }

        Utils.sleep(2000000000);
        drpc.shutdown();
        cluster.killTopology("test");
        cluster.shutdown();
    }

    public static class GetTweeters extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            Object id = input.getValue(0);
            String url = input.getString(1);
            System.err.println("GetTweeters->id: " + id);
            System.err.println("GetTweeters->url: " + url);
            List<String> tweeters = TWEETERS_DB.get(url);
            if (tweeters != null) {
                for (String tweeter : tweeters) {
                    System.err.println("GetTweeters->emit id: " + id + " tweeter: " + tweeter);
                    collector.emit(new Values(id, tweeter));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "tweeter"));
        }
    }

    public static class GetFollowers extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            Object id = input.getValue(0);
            String tweeter = input.getString(1);
            System.err.println("GetFollowers->id: " + id);
            System.err.println("GetFollowers->tweeter: " + tweeter);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if (followers != null) {
                for (String follower : followers) {
                    System.err.println("GetTweeters->emit id: " + id + " follower: " + follower);
                    collector.emit(new Values(id, follower));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "follower"));
        }
    }

    public static class PartialUniquer extends BaseBatchBolt<Object> {
        private BatchOutputCollector collector;
        private Object id;
        private Set<String> followers = new HashSet<>();

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            this.collector = collector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            System.err.println("PartialUniquer->tuple.getString(1): " + tuple.getString(1));
            followers.add(tuple.getString(1));
        }

        @Override
        public void finishBatch() {
            System.err.println("PartialUniquer->id: " + id + " followers.size(): " + followers.size());
            collector.emit(new Values(id, followers.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }

    public static class CountAggregator extends BaseBatchBolt<Object> {
        private BatchOutputCollector collector;
        private Object id;
        private int count = 0;

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            this.collector = collector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            System.err.println("CountAggregator->tuple.getInteger(1): " + tuple.getInteger(1));
            count += tuple.getInteger(1);
            System.err.println("CountAggregator->count: " + count);
        }

        @Override
        public void finishBatch() {
            collector.emit(new Values(id, count));
            System.err.println("CountAggregator->id: " + id + " count: " + count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
    }
}
