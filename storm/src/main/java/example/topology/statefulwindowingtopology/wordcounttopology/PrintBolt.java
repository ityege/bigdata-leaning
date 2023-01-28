package example.topology.statefulwindowingtopology.wordcounttopology;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class PrintBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static KafkaProducer<String, String> producer;
    private static Properties prop = new Properties();

    static {
        prop.put("bootstrap.servers", "bigdata1:9092,bigdata2:9092,bigdata3:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("acks", "all");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("linger.ms", 1);
        prop.put("buffer.memory", 33554432);

        producer = new KafkaProducer<>(prop);

    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = input.getInteger(1);
        System.err.println("word= "+word+","+"count= "+count);
        producer.send(new ProducerRecord<String, String>("test","word= "+word+","+"count= "+count +" --> "+ new Date().getTime()));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
