package example.hdfs.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DefaultSequenceFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TridentSequenceTopology {

    public static StormTopology buildTopology(String hdfsUrl) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence", "key"), 1000, new Values("the cow jumped over the moon", 1L),
                                                    new Values("the man went to the store and bought some candy", 2L),
                                                    new Values("four score and seven years ago", 3L),
                                                    new Values("how many apples can you eat", 4L),
                                                    new Values("to be or not to be the person", 5L));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        Fields hdfsFields = new Fields("sentence", "key");

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
            .withPath("/tmp/trident/sequence")
            .withPrefix("trident")
            .withExtension(".seq");

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions()
            .withFileNameFormat(fileNameFormat)
            .withSequenceFormat(new DefaultSequenceFormat("key", "sentence"))
            .withRotationPolicy(rotationPolicy)
            .withFsUrl(hdfsUrl)
            .addRotationAction(new MoveFileAction().toDestination("/tmp/dest2/"));
        StateFactory factory = new HdfsStateFactory().withOptions(seqOpts);

        TridentState state = stream
            .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        String topoName = "wordCounter";

        conf.setNumWorkers(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, conf, buildTopology("hdfs://bigdata1:8020"));
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }
}
