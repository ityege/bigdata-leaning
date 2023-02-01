package example.streams.branchexample;

import example.topology.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.utils.Utils;

public class BranchExample {
    public static void main(String[] args) throws Exception {
        StreamBuilder builder = new StreamBuilder();
        Stream<Integer>[] evenAndOdd = builder
                /*
                 * Create a stream of random numbers from a spout that
                 * emits random integers by extracting the tuple value at index 0.
                 */
                .newStream(new RandomIntegerSpout(), new ValueMapper<Integer>(0))
                /*
                 * Split the stream of numbers into streams of
                 * even and odd numbers. The first stream contains even
                 * and the second contains odd numbers.
                 */
                .branch(x -> (x % 2) == 0,
                        x -> (x % 2) == 1);
        evenAndOdd[0].forEach(x -> System.out.println("EVEN> " + x));
        evenAndOdd[1].forEach(x -> System.err.println("ODD > " + x));

        Config config = new Config();
        String topoName = "branchExample";

        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topoName, config, builder.build());
        Utils.sleep(2000000);
        cluster.killTopology(topoName);
        cluster.shutdown();
    }
}
