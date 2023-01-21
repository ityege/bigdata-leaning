package example.topology.resourceawareexampletopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExclamationBolt extends BaseRichBolt {
    private static final ConcurrentHashMap<String, String> myCrummyCache = new ConcurrentHashMap<>();
    private static final int CACHE_SIZE = 100_000;
    private OutputCollector collector;

    protected static String getFromCache(String key) {
        return myCrummyCache.get(key);
    }

    protected static void addToCache(String key, String value) {
        myCrummyCache.putIfAbsent(key, value);
        int numToRemove = myCrummyCache.size() - CACHE_SIZE;
        if (numToRemove > 0) {
            Iterator<Map.Entry<String, String>> iterator = myCrummyCache.entrySet().iterator();
            for (; numToRemove > 0 && iterator.hasNext(); numToRemove--) {
                iterator.next();
                iterator.remove();
            }
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String orig = input.getString(0);
        String ret = getFromCache(orig);
        if (ret == null) {
            ret = orig + "!!!";
            addToCache(orig, ret);
        }
        collector.emit(input, new Values(ret));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
