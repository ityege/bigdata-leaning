package example.trident;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class Client {
    public static void main(String[] args) throws Exception {
        Map config = Utils.readDefaultConfig();
        DRPCClient client = new DRPCClient(config, "bigdata3", 3772);
        System.out.println("DRPC RESULT: " + client.execute("words", "cat the dog jumped"));
    }
}
