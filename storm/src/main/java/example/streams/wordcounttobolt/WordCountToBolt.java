package example.streams.wordcounttobolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.Utils;

/**
 * stream API wordcount 并且将结果写入redis中,storm连接redis目前只支持写入,只支持从socket和kafka中读取数据
 */
public class WordCountToBolt {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        // The redis bolt (sink)
        IRichBolt redisStoreBolt = new RedisStoreBolt(poolConfig, storeMapper);
        builder.newStream(new TestWordSpout(), new ValueMapper<String>(0))
                /*
                 * create a stream of (word, 1) pairs
                 */
                .mapToPair(w -> Pair.of(w, 1))
                /*
                 * aggregate the count
                 */
                .countByKey()
                /*
                 * The result of aggregation is forwarded to
                 * the RedisStoreBolt. The forwarded tuple is a
                 * key-value pair of (word, count) with ("key", "value")
                 * being the field names.
                 */
                .to(redisStoreBolt);

        Config config = new Config();
        String topoName = "test";
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }

    private static class WordCountStoreMapper implements RedisStoreMapper {
        private final RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        WordCountStoreMapper() {
            description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("key");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return String.valueOf(tuple.getLongByField("value"));
        }
    }
}
