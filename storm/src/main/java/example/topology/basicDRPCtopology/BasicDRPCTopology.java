package example.topology.basicDRPCtopology;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BasicDRPCTopology {
    public static void main(String[] args) throws Exception {
        Config config = new Config();

        String topoName = "DRPCExample";
        String function = "exclamation";
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        builder.addBolt(new ExclaimBolt(), 3);
        config.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, config, builder.createRemoteTopology());

    }

    public static class ExclaimBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String input = tuple.getString(1);
            collector.emit(new Values(tuple.getValue(0), input + "!!!!!!!!@@@@@@@$$$$$$$$$"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }
}
