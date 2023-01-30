package example.trident.tridentreach;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class Client {
    public static void main(String[] args) throws Exception {
        Map config = Utils.readDefaultConfig();
        DRPCClient client = new DRPCClient(config, "bigdata3", 3772);
        System.out.println("REACH: " + client.execute("reach", "aaa"));
        System.out.println("REACH: " + client.execute("reach", "foo.com/blog/1"));
        System.out.println("REACH: " + client.execute("reach", "engineering.twitter.com/blog/5"));
    }
}
