package example.topology.multithreadwordcounttopology;

import example.topology.bolt.PrinterBolt;
import example.topology.bolt.WordCountBolt;
import example.topology.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/***
 * 这个多线程的wordcount感觉没什么.
 */
public class MultiThreadWordCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new MultiThreadedSplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("printer",new PrinterBolt(),1).shuffleGrouping("count");
        Config config = new Config();
        config.put(Config.TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER, true);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);
        config.setTopologyWorkerMaxHeapSize(128);

        String topologyName = "multithreaded-word-count";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

    private static class MultiThreadedSplitSentence implements IRichBolt {

        private OutputCollector collector;
        private ExecutorService executor;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            executor = Executors.newFixedThreadPool(6);
            context.registerMetric("dummy-counter", () -> 0, 1);
        }

        @Override
        public void execute(Tuple input) {
            String str = input.getString(0);
            String[] splits = str.split("\\s+");
            for (String s : splits) {
                Runnable runnableTask = () -> {
                    collector.emit(new Values(s));
                };
                executor.submit(runnableTask);
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void cleanup() {

        }
    }
}
