
package example.hive.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.trident.HiveStateFactory;
import org.apache.storm.hive.trident.HiveUpdater;
import org.apache.storm.hooks.SubmitterHookException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TridentHiveTopology {


    public static StormTopology buildTopology(String metaStoreUri, String dbName, String tblName, Object keytab, Object principal) {
        int batchSize = 100;
        FixedBatchSpout spout = new FixedBatchSpout(batchSize);
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("hiveTridentspout1", spout);
        String[] partNames = {"city", "state"};
        String[] colNames = {"id", "name", "phone", "street"};
        Fields hiveFields = new Fields("id", "name", "phone", "street", "city", "state");
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
            .withColumnFields(new Fields(colNames))
            .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions;
        if (keytab != null && principal != null) {
            hiveOptions = new HiveOptions(metaStoreUri, dbName, tblName, mapper)
                .withTxnsPerBatch(10)
                .withBatchSize(batchSize)
                .withIdleTimeout(10)
                .withCallTimeout(30000)
                .withKerberosKeytab((String) keytab)
                .withKerberosPrincipal((String) principal);
        } else  {
            hiveOptions = new HiveOptions(metaStoreUri, dbName, tblName, mapper)
                .withTxnsPerBatch(10)
                .withBatchSize(batchSize)
                .withCallTimeout(30000)
                .withIdleTimeout(10);
        }
        StateFactory factory = new HiveStateFactory().withOptions(hiveOptions);
        TridentState state = stream.partitionPersist(factory, hiveFields, new HiveUpdater(), new Fields());
        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        String metaStoreUri = "thrift://bigdata2:9083";;
        String dbName = "test";
        String tblName = "tridenthivetopology";
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        String topoName = "tridentHiveTopology";
        String keytab = null;
        String principal = null;



        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, conf, buildTopology(metaStoreUri, dbName, tblName, keytab, principal));
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }

    public static class FixedBatchSpout implements IBatchSpout {
        int maxBatchSize;
        HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
        private Values[] outputs = {
            new Values("1", "user1", "123456", "street1", "sunnyvale", "ca"),
            new Values("2", "user2", "123456", "street2", "sunnyvale", "ca"),
            new Values("3", "user3", "123456", "street3", "san jose", "ca"),
            new Values("4", "user4", "123456", "street4", "san jose", "ca"),
        };
        private int index = 0;
        boolean cycle = false;

        public FixedBatchSpout(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public void setCycle(boolean cycle) {
            this.cycle = cycle;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("id", "name", "phone", "street", "city", "state");
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context) {
            index = 0;
        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if (batch == null) {
                batch = new ArrayList<List<Object>>();
                if (index >= outputs.length && cycle) {
                    index = 0;
                }
                for (int i = 0; i < maxBatchSize; index++, i++) {
                    if (index == outputs.length) {
                        index = 0;
                    }
                    batch.add(outputs[index]);
                }
                this.batches.put(batchId, batch);
            }
            for (List<Object> list : batch) {
                collector.emit(list);
            }
        }

        @Override
        public void ack(long batchId) {
            this.batches.remove(batchId);
        }

        @Override
        public void close() {
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }

    }

}
