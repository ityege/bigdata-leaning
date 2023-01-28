package example.topology.manualDRPC;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class ManualDRPC {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        DRPCSpout spout = new DRPCSpout("exclamation");
        builder.setSpout("drpc", spout);
        builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");
        StormSubmitter.submitTopology("exclaim", new Config(), builder.createTopology());
    }

    public static class ExclamationBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String arg = input.getString(0);
            Object retInfo = input.getValue(1);
            System.err.println("arg: " + arg + " retInfo: " + retInfo);
            collector.emit(new Values(arg + "!!!", retInfo));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result", "return-info"));
        }
    }
}
