package example.topology.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/***
 * 这个状态管理我没看出来怎么状态管理的.
 */
public class StatefulSumBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {
    private KeyValueState<String, Long> kvState;
    private OutputCollector collector;
    private Long sum;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Long> state) {
        this.kvState = state;
        sum = kvState.get("sum", 0L);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));
    }

    @Override
    public void execute(Tuple input) {
        sum += ((Number) input.getValueByField("value")).longValue();
        kvState.put("sum", sum);
        collector.emit(input, new Values(sum));
        collector.ack(input);
    }


}
