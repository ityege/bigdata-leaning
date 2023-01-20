package example.topology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


import java.util.Map;
import java.util.Random;

public class RandomIntegerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private long msgId = 0;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        msgId++;
//        当前时间的前一天
        collector.emit(new Values(random.nextInt(10), System.currentTimeMillis() - (24 * 60 * 60 * 1000), msgId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value", "ts","msgid"));
    }

    @Override
    public void ack(Object msgId) {
        System.err.println("ack: " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("fail: " + msgId);
    }
}
