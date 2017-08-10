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

package org.apache.storm.starter.loadgen;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.misc.metric.HttpForwardingMetricsServer;
import org.apache.storm.starter.ThroughputVsLatency;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;

/**
 * Generate a simulated load
 */
public class GenLoad {
    //TODO lets parse a file and do this for real....

    private static class MemMeasure {
        private long _mem = 0;
        private long _time = 0;

        public synchronized void update(long mem) {
            _mem = mem;
            _time = System.currentTimeMillis();
        }

        public synchronized long get() {
            return isExpired() ? 0l : _mem;
        }

        public synchronized boolean isExpired() {
            return (System.currentTimeMillis() - _time) >= 20000;
        }
    }

    private static final Histogram _histo = new Histogram(3600000000000L, 3);
    private static final AtomicLong _systemCPU = new AtomicLong(0);
    private static final AtomicLong _userCPU = new AtomicLong(0);
    private static final AtomicLong _gcCount = new AtomicLong(0);
    private static final AtomicLong _gcMs = new AtomicLong(0);
    private static final ConcurrentHashMap<String, MemMeasure> _memoryBytes = new ConcurrentHashMap<>();

    private static long readMemory() {
        long total = 0;
        for (MemMeasure mem: _memoryBytes.values()) {
            total += mem.get();
        }
        return total;
    }

    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;

    public static void printMetrics(Nimbus.Iface client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        for (ExecutorSummary exec: info.get_executors()) {
            if ("spout".equals(exec.get_component_id()) && exec.get_stats() != null && exec.get_stats().get_specific() != null) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                if (ackedMap != null) {
                    for (String key: ackedMap.keySet()) {
                        if (failedMap != null) {
                            Long tmp = failedMap.get(key);
                            if (tmp != null) {
                                failed += tmp;
                            }
                        }
                        long ackVal = ackedMap.get(key);
                        acked += ackVal;
                    }
                }
            }
        }
        long ackedThisTime = acked - _prev_acked;
        long thisTime = uptime - _prev_uptime;
        long nnpct, nnnpct, min, max;
        double mean, stddev;
        synchronized(_histo) {
            nnpct = _histo.getValueAtPercentile(99.0);
            nnnpct = _histo.getValueAtPercentile(99.9);
            min = _histo.getMinValue();
            max = _histo.getMaxValue();
            mean = _histo.getMean();
            stddev = _histo.getStdDeviation();
            _histo.reset();
        }
        long user = _userCPU.getAndSet(0);
        long sys = _systemCPU.getAndSet(0);
        long gc = _gcMs.getAndSet(0);
        double memMB = readMemory() / (1024.0 * 1024.0);
        System.out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d " +
                "99%%: %,15d 99.9%%: %,15d min: %,15d max: %,15d mean: %,15.2f " +
                "stddev: %,15.2f user: %,10d sys: %,10d gc: %,10d mem: %,10.2f\n",
            uptime, ackedThisTime, (((double)ackedThisTime)/thisTime), failed, nnpct, nnnpct,
            min, max, mean, stddev, user, sys, gc, memMB);
        _prev_uptime = uptime;
        _prev_acked = acked;
    }

    public static void kill(Nimbus.Iface client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        //TODO need to do this better
        HttpForwardingMetricsServer metricServer = new HttpForwardingMetricsServer(conf) {
            @Override
            public void handle(IMetricsConsumer.TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
                //crud no simple way to tie this to a given topology :(
                String worker = taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort;
                for (DataPoint dp: dataPoints) {
                    if (dp.name.startsWith("comp-lat-histo") && dp.value instanceof Histogram) {
                        synchronized(_histo) {
                            _histo.add((Histogram)dp.value);
                        }
                    } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object sys = m.get("sys-ms");
                        if (sys instanceof Number) {
                            _systemCPU.getAndAdd(((Number)sys).longValue());
                        }
                        Object user = m.get("user-ms");
                        if (user instanceof Number) {
                            _userCPU.getAndAdd(((Number)user).longValue());
                        }
                    } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object count = m.get("count");
                        if (count instanceof Number) {
                            _gcCount.getAndAdd(((Number)count).longValue());
                        }
                        Object time = m.get("timeMs");
                        if (time instanceof Number) {
                            _gcMs.getAndAdd(((Number)time).longValue());
                        }
                    } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object val = m.get("usedBytes");
                        if (val instanceof Number) {
                            MemMeasure mm = _memoryBytes.get(worker);
                            if (mm == null) {
                                mm = new MemMeasure();
                                MemMeasure tmp = _memoryBytes.putIfAbsent(worker, mm);
                                mm = tmp == null ? mm : tmp;
                            }
                            mm.update(((Number)val).longValue());
                        }
                    }
                }
            }
        };

        metricServer.serve();
        String url = metricServer.getUrl();

        NimbusClient client = NimbusClient.getConfiguredClient(conf);
        //TODO need to generate topologies from a file not hard coded...
        conf.setNumWorkers(1);
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(org.apache.storm.misc.metric.HttpForwardingMetricsConsumer.class, url, 1);
        Map<String, String> workerMetrics = new HashMap<>();
        if (!NimbusClient.isLocalOverride()) {
            //sigar uses JNI and does not work in local mode
            workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
        }
        conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);
        conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
            "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

        TopologyBuilder builder = new TopologyBuilder();

        LoadSpout spout = new LoadSpout(new StreamStats("default", new NormalDistStats(1000, 200, 500, 2000), false));
        builder.setSpout("spout", spout, 1);
        LoadBolt level1 = new LoadBolt(new StreamStats("default", new NormalDistStats(10000, 2000, 5000, 20000), true));
        builder.setBolt("level1", level1, 1).shuffleGrouping("spout", "default");
        LoadBolt level2 = new LoadBolt();
        builder.setBolt("level2", level2, 10).fieldsGrouping("level1", "default", new Fields("key"));

        try {
            StormSubmitter.submitTopology("TEST", conf, builder.createTopology());

            for (int i = 0; i < 3 * 2; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client.getClient(), "TEST");
            }
        } catch (Exception e) {
            System.err.println("Coud not submit or trace TEST");
            e.printStackTrace();
        } finally {
            try {
                kill(client.getClient(), "TEST");
            } catch (Exception e) {
                System.err.println("Coud not kill TEST");
                e.printStackTrace();
            }
            System.exit(0);
        }
    }
}
