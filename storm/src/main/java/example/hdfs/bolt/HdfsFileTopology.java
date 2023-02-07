package example.hdfs.bolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class HdfsFileTopology {
    static final String SENTENCE_SPOUT_ID = "sentence-spout";
    static final String BOLT_ID = "my-bolt";
    static final String TOPOLOGY_NAME = "test-topology";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Config config = new Config();
        config.setNumWorkers(1);

        SentenceSpout spout = new SentenceSpout();

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/foo/")
                .withExtension(".txt");

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://bigdata1:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(new MoveFileAction().toDestination("/tmp/dest2/"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, bolt, 4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        String topoName = TOPOLOGY_NAME;
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.createTopology());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }


    public static class SentenceSpout extends BaseRichSpout {
        private ConcurrentHashMap<UUID, Values> pending;
        private SpoutOutputCollector collector;
        private String[] sentences = {
                "my dog has fleas",
                "i like cold beverages",
                "the dog ate my homework",
                "don't have a cow man",
                "i don't think i like fleas"
        };
        private int index = 0;
        private int count = 0;
        private long total = 0L;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence", "timestamp"));
        }

        @Override
        public void open(Map<String, Object> config, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
            this.pending = new ConcurrentHashMap<UUID, Values>();
        }

        @Override
        public void nextTuple() {
            Values values = new Values(sentences[index], System.currentTimeMillis());
            UUID msgId = UUID.randomUUID();
            this.pending.put(msgId, values);
            this.collector.emit(values, msgId);
            index++;
            if (index >= sentences.length) {
                index = 0;
            }
            count++;
            total++;
            if (count > 20000) {
                count = 0;
                System.out.println("Pending count: " + this.pending.size() + ", total: " + this.total);
            }
            Thread.yield();
        }

        @Override
        public void ack(Object msgId) {
            this.pending.remove(msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println("**** RESENDING FAILED TUPLE");
            this.collector.emit(this.pending.get(msgId), msgId);
        }
    }
}
