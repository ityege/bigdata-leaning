package example.topology.persistentwindowingtopology;

import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 这个是将storm的状态管理持久化到redis中,但是redis是本地的并且端口号也是默认的,在哪里配置redis没有找到.
 */
public class PersistentWindowingTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomIntegerSpout());
        builder.setBolt("avgbolt", new AvgBolt()
                .withWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(2, TimeUnit.SECONDS))
                .withPersistence()
                .withMaxEventsInMemory(2000), 1
        ).shuffleGrouping("spout");
        builder.setBolt("printer", (x, y) -> System.out.println(x.getValue(0)), 1).shuffleGrouping("avgbolt");
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 5000);
        config.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.redis.state.RedisKeyValueStateProvider");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();

    }

    private static class AvgBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Pair<Long, Long>>> {
        private static final String STATE_KEY = "avg";
        private OutputCollector collector;
        private KeyValueState<String, Pair<Long, Long>> state;
        private Pair<Long, Long> globalAvg;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }


        @Override
        public void initState(KeyValueState<String, Pair<Long, Long>> state) {
            this.state = state;
            globalAvg = state.get(STATE_KEY, Pair.of(0L, 0L));
            System.err.println("initState with global avg [" + (double) globalAvg.getFirst() / globalAvg.getSecond() + "]");
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            int count = 0;
            Iterator<Tuple> iter = inputWindow.getIter();
            while (iter.hasNext()) {

                Tuple tuple = iter.next();
                sum += tuple.getInteger(0);
                ++count;

            }
            System.err.println("Count : " + count);
            globalAvg = Pair.of(globalAvg.getFirst() + sum, globalAvg.getSecond() + count);
            state.put(STATE_KEY, globalAvg);
            collector.emit(new Values(new Averages((double) globalAvg.getFirst() / globalAvg.getSecond(), (double) sum / count)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }

    /**
     * 这个对象需要在网络上面传输,但是为什么没有实现序列化接口.不了解storm怎么网络传输数据的.
     */
    private static class Averages {
        private final double global;
        private final double window;

        public Averages(double global, double window) {
            this.global = global;
            this.window = window;
        }

        @Override
        public String toString() {
            return "Averages{" + "global=" + String.format("%.2f", global) + ", window=" + String.format("%.2f", window) + '}';
        }
    }


}
