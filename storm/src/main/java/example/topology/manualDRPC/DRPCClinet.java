package example.topology.manualDRPC;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * drpc客户端
 */
public class DRPCClinet {
    public static void main(String[] args) throws Exception {
        Map config = Utils.readDefaultConfig();
        DRPCClient client = new DRPCClient(config, "bigdata3", 3772);
        System.out.println(client.execute("exclamation", "aaa"));
        System.out.println(client.execute("exclamation", "bbb"));

    }
}
