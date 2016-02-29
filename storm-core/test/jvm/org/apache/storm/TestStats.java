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

package org.apache.storm;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.CommonAggregateStats;
import org.apache.storm.generated.SpoutAggregateStats;
import org.apache.storm.generated.BoltAggregateStats;
import org.apache.storm.generated.ComponentAggregateStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.TopologyStats;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a BAD TEST DO NOT MERGE!!!!!
 */
public class TestStats {
  private static final Logger LOG = LoggerFactory.getLogger(TestStats.class);

  private static Object lock = new Object();
  private static int numToEmit = 0;
  private static CountDownLatch allDone = null;

  private static class CountDownSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      synchronized (lock) {
        if (numToEmit > 0) {
            _collector.emit(new Values(numToEmit), "ANCHOR");
            numToEmit--;
        }
      }
    }

    @Override
    public void ack(Object id) {
      allDone.countDown();
    }

    @Override
    public void fail(Object id) {
      allDone.countDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("number"));
    }
  }

  public static class PassThroughBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getValue(0)));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("again"));
    }
  }

  @Test
  public void testBasic() throws Exception {
    final int emitCount = 10000;
    numToEmit = emitCount;
    allDone = new CountDownLatch(emitCount);

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new CountDownSpout(), 5);
    builder.setBolt("bolt", new PassThroughBolt(), 5).shuffleGrouping("spout");

    Config conf = new Config();
    conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);
    conf.setNumEventLoggers(0);
    conf.setDebug(true);

    LOG.info("STARTING CLUSTER...");
    LocalCluster cluster = new LocalCluster();
    try {
      cluster.submitTopology("test", conf, builder.createTopology());
      try {
        assertTrue(allDone.await(30, TimeUnit.SECONDS));
        synchronized(lock) {
          assertEquals(0, numToEmit);
        }
        ClusterSummary cs = cluster.getClusterInfo();
        String id = null;
        for (TopologySummary ts: cs.get_topologies()) {
          if ("test".equals(ts.get_name())) {
            id = ts.get_id();
          }
        }
        assertTrue(id != null);
        TopologyPageInfo info = null;
        long start = System.currentTimeMillis();
        while (true) {
            info = cluster.getTopologyPageInfo(id, ":all-time", false);        
            Long emitted = info.get_topology_stats().get_window_to_emitted().get(":all-time");
            if (emitted != null && emitted == (emitCount * 2)) { //1 for spout 1 for the bolt
                //looks like we got everything
                break;
            }
            Thread.sleep(100);
            assertTrue(System.currentTimeMillis() - start < 30000);
        }
        TopologyStats topoStats = info.get_topology_stats();
        LOG.info("TOPO STATS: {}", topoStats);
        assertEquals(new Long(emitCount * 2), topoStats.get_window_to_emitted().get(":all-time"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_transferred().get(":all-time"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_acked().get(":all-time"));
        assertEquals((Long) null, topoStats.get_window_to_failed().get(":all-time"));
        assertEquals(500.0, (double)topoStats.get_window_to_complete_latencies_ms().get(":all-time"), 500.0);

        assertEquals(new Long(emitCount * 2), topoStats.get_window_to_emitted().get("10800"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_transferred().get("10800"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_acked().get("10800"));
        assertEquals((Long) null, topoStats.get_window_to_failed().get("10800"));
        assertEquals(500.0, (double)topoStats.get_window_to_complete_latencies_ms().get("10800"), 500.0);

        assertEquals(new Long(emitCount * 2), topoStats.get_window_to_emitted().get("86400"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_transferred().get("86400"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_acked().get("86400"));
        assertEquals((Long) null, topoStats.get_window_to_failed().get("86400"));
        assertEquals(500.0, (double)topoStats.get_window_to_complete_latencies_ms().get("86400"), 500.0);

        assertEquals(new Long(emitCount * 2), topoStats.get_window_to_emitted().get("600"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_transferred().get("600"));
        assertEquals(new Long(emitCount), topoStats.get_window_to_acked().get("600"));
        assertEquals((Long) null, topoStats.get_window_to_failed().get("600"));
        assertEquals(500.0, (double)topoStats.get_window_to_complete_latencies_ms().get("600"), 500.0);

        Map<String, ComponentAggregateStats> spoutStats = info.get_id_to_spout_agg_stats();
        LOG.info("SPOUT-STATS: {}", spoutStats);
        ComponentAggregateStats spout = spoutStats.get("spout");
        assertTrue(spout != null); 
        CommonAggregateStats sCommon = spout.get_common_stats();
        assertTrue(sCommon != null); 

        assertEquals(emitCount, sCommon.get_emitted());
        assertEquals(emitCount, sCommon.get_transferred());
        assertEquals(emitCount, sCommon.get_acked());
        assertEquals(0, sCommon.get_failed());
        assertEquals(500.0, (double)spout.get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        ComponentPageInfo sInfo = cluster.getComponentPageInfo(id, "spout", ":all-time", false);
        LOG.info("S-INFO: {}", sInfo);
        assertEquals(emitCount, sInfo.get_window_to_stats().get(":all-time").get_common_stats().get_emitted());
        assertEquals(emitCount, sInfo.get_window_to_stats().get(":all-time").get_common_stats().get_transferred());
        assertEquals(emitCount, sInfo.get_window_to_stats().get(":all-time").get_common_stats().get_acked());
        assertEquals(0, sInfo.get_window_to_stats().get(":all-time").get_common_stats().get_failed());
        assertEquals(500.0, (double)sInfo.get_window_to_stats().get(":all-time").get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        assertEquals(emitCount, sInfo.get_window_to_stats().get("10800").get_common_stats().get_emitted());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("10800").get_common_stats().get_transferred());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("10800").get_common_stats().get_acked());
        assertEquals(0, sInfo.get_window_to_stats().get("10800").get_common_stats().get_failed());
        assertEquals(500.0, (double)sInfo.get_window_to_stats().get("10800").get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        assertEquals(emitCount, sInfo.get_window_to_stats().get("86400").get_common_stats().get_emitted());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("86400").get_common_stats().get_transferred());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("86400").get_common_stats().get_acked());
        assertEquals(0, sInfo.get_window_to_stats().get("86400").get_common_stats().get_failed());
        assertEquals(500.0, (double)sInfo.get_window_to_stats().get("86400").get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        assertEquals(emitCount, sInfo.get_window_to_stats().get("600").get_common_stats().get_emitted());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("600").get_common_stats().get_transferred());
        assertEquals(emitCount, sInfo.get_window_to_stats().get("600").get_common_stats().get_acked());
        assertEquals(0, sInfo.get_window_to_stats().get("600").get_common_stats().get_failed());
        assertEquals(500.0, (double)sInfo.get_window_to_stats().get("600").get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        assertEquals(emitCount, sInfo.get_sid_to_output_stats().get("default").get_common_stats().get_emitted());
        assertEquals(emitCount, sInfo.get_sid_to_output_stats().get("default").get_common_stats().get_transferred());
        assertEquals(emitCount, sInfo.get_sid_to_output_stats().get("default").get_common_stats().get_acked());
        assertEquals(0, sInfo.get_sid_to_output_stats().get("default").get_common_stats().get_failed());
        assertEquals(500.0, (double)sInfo.get_sid_to_output_stats().get("default").get_specific_stats().get_spout().get_complete_latency_ms(), 500.0);

        Map<String, ComponentAggregateStats> boltStats = info.get_id_to_bolt_agg_stats();
        LOG.info("BOLT-STATS: {}", boltStats);

        ComponentAggregateStats bolt = boltStats.get("bolt");
        assertTrue(bolt != null); 
        CommonAggregateStats bCommon = bolt.get_common_stats();
        assertTrue(bCommon != null); 

        assertEquals(emitCount, bCommon.get_emitted());
        assertEquals(0, bCommon.get_transferred());
        assertEquals(emitCount, bCommon.get_acked());
        assertEquals(0, bCommon.get_failed());
        assertEquals(10.0, (double)bolt.get_specific_stats().get_bolt().get_execute_latency_ms(), 10.0);
        assertEquals(10.0, (double)bolt.get_specific_stats().get_bolt().get_process_latency_ms(), 10.0);
        assertEquals(1.0, (double)bolt.get_specific_stats().get_bolt().get_capacity(), 1.0);
        assertEquals(emitCount, bolt.get_specific_stats().get_bolt().get_executed());

        ComponentPageInfo bInfo = cluster.getComponentPageInfo(id, "bolt", ":all-time", false);
        LOG.info("B-INFO: {}", bInfo);

      } finally {
        cluster.killTopology("test");
      }
    } finally {
      cluster.shutdown();
    }
  }
}
