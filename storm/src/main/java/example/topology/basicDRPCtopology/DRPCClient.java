package example.topology.basicDRPCtopology;

import org.apache.storm.utils.Utils;

import java.util.Map;

public class DRPCClient {
    public static void main(String[] args) throws Exception {
        Map config = Utils.readDefaultConfig();
        org.apache.storm.utils.DRPCClient client = new org.apache.storm.utils.DRPCClient(config, "bigdata3", 3772);
        System.out.println(client.execute("exclamation", "aaa"));
        System.out.println(client.execute("exclamation", "bbb"));
    }
}
