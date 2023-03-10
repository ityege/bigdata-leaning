/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.elasticsearch.common;

import org.apache.storm.Config;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * ElasticSearch example utilities.
 */
public final class EsTestUtil {



    /**
     * Generates a new tuple mapper.
     * @return the generated mapper
     */
    public static EsTupleMapper generateDefaultTupleMapper() {
        return new DefaultEsTupleMapper();
    }

    /**
     * Starts an ElasticSearch node.
     * @return the started node.
     */
    public static Node startEsNode() {
        Node node = NodeBuilder.nodeBuilder().data(true).settings(
                Settings.settingsBuilder()
                        .put(ClusterName.SETTING, EsConstants.CLUSTER_NAME)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", true)
                        .put("index.percolator.map_unmapped_fields_as_string",
                                true)
                        .put("index.store.type", "mmapfs")
                        .put("path.home", "./data")
        ).build();
        node.start();
        return node;
    }

    /**
     * Waits for specified seconds and ignores {@link InterruptedException}.
     * @param seconds the seconds to wait
     */
    public static void waitForSeconds(final int seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException ex) {
            //expected
        }
    }
}
