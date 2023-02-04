/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.elasticsearch.bolt;

import example.elasticsearch.common.EsConstants;
import example.elasticsearch.common.EsTestUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * storm的官方案例自己创建一个本地的elasticsearch,并不是连接远程的,最操蛋的是版本还是2.4.4的,
 * 现在最新的elasticsearch版本是8.6.1的,这个例子用的是上古版本.
 * 代码里面还有一堆bug,连elasticsearch的http连接端口号都没配置对,apache那帮人写完代码有没有测试一下.
 * 这个根本用不了.
 */

public final class EsIndexTopology {

    private static final String TOPOLOGY_NAME = "elasticsearch-test-topology1";


    public static void main(final String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(1);
        TopologyBuilder builder = new TopologyBuilder();
        UserDataSpout spout = new UserDataSpout();
        builder.setSpout("spout", spout, 1);
        EsTupleMapper tupleMapper = EsTestUtil.generateDefaultTupleMapper();
        EsConfig esConfig = new EsConfig("http://localhost:9200");
        builder.setBolt("bolt", new EsIndexBolt(esConfig, tupleMapper), 1)
                .shuffleGrouping("spout");

        EsTestUtil.startEsNode();
        EsTestUtil.waitForSeconds(EsConstants.WAIT_DEFAULT_SECS);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.sleep(2000000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    public static class UserDataSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        private String indexName = "index1";
        private String typeName = "type1";
        private String[] sources = new String[]{
                "{\"name\": \"张三\",\"age\": 18}",
                "{\"name\": \"李四\",\"age\": 19}"
        };
        private Random random=new Random();


        @Override
        public void declareOutputFields(final OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("source", "index", "type", "id"));
        }

        @Override
        public void open(final Map<String, Object> config,
                         final TopologyContext context,
                         final SpoutOutputCollector collectorArg) {
            this.collector = collectorArg;
        }

        /**
         * Makes the spout emit the next tuple, if any.
         */
        @Override
        public void nextTuple() {
            String source = sources[random.nextInt(2)];
            String msgId = UUID.randomUUID().toString();
            Values values = new Values(source, indexName, typeName, msgId);
            this.collector.emit(values, msgId);

        }

        @Override
        public void ack(final Object msgId) {
            System.err.println("ack: " + msgId);
        }


        @Override
        public void fail(final Object msgId) {
            System.err.println("fail: " + msgId);
        }
    }

}
