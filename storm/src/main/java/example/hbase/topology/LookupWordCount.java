package example.hbase.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseLookupBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class LookupWordCount {

    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String LOOKUP_BOLT = "LOOKUP_BOLT";
    private static final String TOTAL_COUNT_BOLT = "TOTAL_COUNT_BOLT";

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        WordSpout spout = new WordSpout();
        SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField("word");
        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "count"));
        WordCountValueMapper rowToTupleMapper = new WordCountValueMapper();
        HBaseLookupBolt lookupBolt = new HBaseLookupBolt("WordCount", mapper, rowToTupleMapper)
//                建hbase-site.xml文件复制到resource目录下面
//                .withConfigKey("hbase.conf")
                .withProjectionCriteria(projectionCriteria);
        //wordspout -> lookupbolt -> totalCountBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(LOOKUP_BOLT, lookupBolt, 1).shuffleGrouping(WORD_SPOUT);
        TotalWordCounter totalBolt = new TotalWordCounter();
        builder.setBolt(TOTAL_COUNT_BOLT, totalBolt, 1).fieldsGrouping(LOOKUP_BOLT, new Fields("columnName"));
        String topoName = "test";

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.createTopology());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }
}
