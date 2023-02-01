package example.streams.windowedwordcount;

import example.topology.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

public class WindowedWordCount {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(new RandomSentenceSpout(), new ValueMapper<String>(0), 2)
                /*
                 * a two seconds tumbling window
                 */
                .window(TumblingWindows.of(BaseWindowedBolt.Duration.seconds(2)))
                /*
                 * split the sentences to words
                 */
                .flatMap(s -> Arrays.asList(s.split(" ")))
                /*
                 * create a stream of (word, 1) pairs
                 */
                .mapToPair(w -> Pair.of(w, 1))
                /*
                 * compute the word counts in the last two second window
                 */
                .countByKey()
                /*
                 * emit the count for the words that occurred
                 * at-least five times in the last two seconds
                 */
                .filter(x -> x.getSecond() >= 5)
                /*
                 * print the results to stdout
                 */
                .print();
        Config config = new Config();
        String topoName = "test";
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();

    }
}
