package example.stormsql;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class Source {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();

        prop.put("bootstrap.servers", "bigdata1:9092,bigdata2:9092,bigdata3:9092");
        prop.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        prop.put("acks", "all");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("linger.ms", 1);
        prop.put("buffer.memory", 33554432);
        String topic = "test";
        KafkaProducer<String, Object> producer = new KafkaProducer<>(prop);
        Map<String, Object> map = new HashMap<>();
        String[] names = new String[]{"张三", "李四", "王五", "赵六"};
        Random random = new Random();
        Gson gson = new Gson();
        while (true) {
            Thread.sleep(1000);
            map.clear();
            map.put("STU_ID", UUID.randomUUID().toString());
            map.put("STU_NAME", names[random.nextInt(4)]);
            map.put("STU_TIME", new Date().getTime());

            producer.send(new ProducerRecord<String, Object>(topic, gson.toJson(map)));
        }
    }
}
