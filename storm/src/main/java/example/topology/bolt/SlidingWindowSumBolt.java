package example.topology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class SlidingWindowSumBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private int sum = 0;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expireTuples = inputWindow.getExpired();
//        System.err.println("Events in current window: " + tuplesInWindow.size());
//        System.err.println("tuplesInWindow : " + tuplesInWindow);
//        System.err.println("newTuples : " + newTuples);
//        System.err.println("expireTuples : " + expireTuples);
        for (Tuple tuple : newTuples) {
            sum += (Integer) tuple.getValue(0);
        }
        for (Tuple tuple : expireTuples) {
            sum -= (int) tuple.getValue(0);
        }

        collector.emit(new Values(sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("sum"));
    }
}
