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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.SharedOffHeapWithinNode;
import backtype.storm.topology.SharedOnHeap;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class
        ResourceAwareExampleTopology {
    public static class ExclamationBolt extends BaseRichBolt {
        //Have a crummy cache to show off shared memory accounting
        private static final ConcurrentHashMap<String, String> myCrummyCache = new ConcurrentHashMap<>();
        private static final int CACHE_SIZE = 100_000;
        OutputCollector _collector;

        protected static String getFromCache(String key) {
            return myCrummyCache.get(key);
        }
        
        protected static void addToCache(String key, String value) {
            myCrummyCache.putIfAbsent(key, value);
            int numToRemove = myCrummyCache.size() - CACHE_SIZE;
            if (numToRemove > 0) {
                //Remove something randomly...
                Iterator<Entry<String, String>> it = myCrummyCache.entrySet().iterator();
                for (; numToRemove > 0 && it.hasNext(); numToRemove--) {
                    it.next();
                    it.remove();
                }
            }
        }
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String orig = tuple.getString(0);
            String ret = getFromCache(orig);
            if (ret == null) {
                ret = orig + "!!!";
                addToCache(orig, ret);
            }
            _collector.emit(tuple, new Values(ret));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //A topology can set resources in terms of CPU and Memory for each component
        // These can be chained (like with setting the CPU requirement)
        SpoutDeclarer spout =  builder.setSpout("word", new TestWordSpout(), 10).setCPULoad(20);
        // Or done separately like with setting the 
        // onheap and offheap memory requirement
        spout.setMemoryLoad(64, 16);
        //On heap memory is used to help calculate the heap of the java process for the worker
        // off heap memory is for things like JNI memory allocated off heap, or when using the
        // ShellBolt or ShellSpout.  In this case the 16 MB of off heap is just as an example
        // as we are not using it.

        // Some times a Bolt or Spout will have some memory that is shared between the instances
        // These are typically caches, but could be anything like a static database that is memory
        // mapped into the processes. These can be declared separately and added to the bolts and
        // spouts that use them.  Or if only one uses it they can be created inline with the add
        SharedOnHeap exclaimCache = new SharedOnHeap(100, "exclaim-cache");
        SharedOffHeapWithinNode notImplementedButJustAnExample = new SharedOffHeapWithinNode(500, "not-implemented-node-level-cache");
        
        //If CPU or memory is not set the values stored in topology.component.resources.onheap.memory.mb,
        // topology.component.resources.offheap.memory.mb and topology.component.cpu.pcore.percent
        // will be used instead
        builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word")
          .addSharedMemory(exclaimCache);

        builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1")
          .setMemoryLoad(100)
          .addSharedMemory(exclaimCache)
          .addSharedMemory(notImplementedButJustAnExample);

        Config conf = new Config();
        conf.setDebug(true);

        //in RAS the number of workers will be computed for you so you don't need to set
        //conf.setNumWorkers(3);
        
        // The size of a worker is limited by the amount of heap assigned to it and can be overridden by
        conf.setTopologyWorkerMaxHeapSize(1024.0);
        // This is to try and balance the time needed to devote to GC against not needing to 
        // serialize/deserialize tuples

        //The priority of a topology describes the importance of the topology in decreasing importance
        // starting from 0 (i.e. 0 is the highest priority and the priority importance decreases as the priority number increases).
        //Recommended range of 0-29 but no hard limit set.
        // If there are not enough resources in a cluster the priority in combination with how far over a guarantees
        // a user is will decide which topologies are run and which ones are not.
        conf.setTopologyPriority(29);

         //set to use the default resource aware strategy
        conf.setTopologyStrategy(backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
