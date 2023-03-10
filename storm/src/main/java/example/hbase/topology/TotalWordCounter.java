/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package example.hbase.topology;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import static org.apache.storm.utils.Utils.tuple;

public class TotalWordCounter implements IBasicBolt {

    private static final Random RANDOM = new Random();
    private BigInteger total = BigInteger.ZERO;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
    }

    /*
     * Just output the word value with a count of 1.
     * The HBaseBolt will handle incrementing the counter.
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        total = total.add(new BigInteger(input.getValues().get(1).toString()));
        collector.emit(tuple(total.toString()));
        //prints the total with low probability.
        if (RANDOM.nextInt(1000) > 995) {
            System.err.println("Running total = " + total);
        }
    }

    @Override
    public void cleanup() {
        System.err.println("Final total = " + total);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("total"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
