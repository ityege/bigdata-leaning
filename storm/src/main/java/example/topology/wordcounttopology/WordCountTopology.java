package example.topology.wordcounttopology;

import example.topology.bolt.SplitSentence;
import example.topology.bolt.WordCountBolt;
import example.topology.spout.RandomSentenceSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("print", new PrintBolt(), 1).shuffleGrouping("count");
        conf.setDebug(true);
        String topologyName = "word-count";
        conf.setNumWorkers(3);
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }


}
