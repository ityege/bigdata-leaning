package example.topology.statefultopology;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrinterBolt extends BaseBasicBolt {
    private TopologyContext context;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        this.context = context;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.err.println(context.getThisComponentId()+"-->"+input.getValues());
        collector.emit(input.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));
    }
}
