package example.topology.resourceawareexampletopology;

import example.topology.bolt.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.SharedOffHeapWithinNode;
import org.apache.storm.topology.SharedOnHeap;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * 设置topology的资源
 */
public class ResourceAwareExampleTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        SpoutDeclarer spout = builder.setSpout("word", new TestWordSpout(), 10).setCPULoad(20);
        spout.setMemoryLoad(64, 16);
        SharedOnHeap exclaimCache = new SharedOnHeap(100, "exclaim-cache");
        SharedOffHeapWithinNode notImplementedButJustAnExample = new SharedOffHeapWithinNode(500, "not-implemented-node-level-cache");
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("word")
                .addSharedMemory(exclaimCache);
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1")
                .setMemoryLoad(100)
                .addSharedMemory(exclaimCache)
                .addSharedMemory(notImplementedButJustAnExample);
        builder.setBolt("printer",new PrinterBolt(),1).shuffleGrouping("exclaim2");
        Config config = new Config();
        config.setDebug(true);
        config.setTopologyWorkerMaxHeapSize(1024.0);
        config.setTopologyPriority(29);
        config.setTopologyStrategy("org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
