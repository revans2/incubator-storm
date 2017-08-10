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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.misc.metric.HttpForwardingMetricsServer;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ObjectReader;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Generate a simulated load
 */
public class GenLoad {
    //TODO we should make this configurable some how...
    static final int NUM_MINS = 3;
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

    private static final Histogram histo = new Histogram(3600000000000L, 3);
    private static final AtomicLong systemCPU = new AtomicLong(0);
    private static final AtomicLong userCPU = new AtomicLong(0);
    private static final AtomicLong gcCount = new AtomicLong(0);
    private static final AtomicLong gcMs = new AtomicLong(0);
    private static final ConcurrentHashMap<String, MemMeasure> memoryBytes = new ConcurrentHashMap<>();

    private static long readMemory() {
        long total = 0;
        for (MemMeasure mem: memoryBytes.values()) {
            total += mem.get();
        }
        return total;
    }

    private static long prevAcked = 0;
    private static long prevUptime = 0;

    public static void printMetrics(Nimbus.Iface client, List<String> names) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        Set<String> ids = new HashSet<>();
        for (TopologySummary ts: summary.get_topologies()) {
            if (names.contains(ts.get_name())) {
                ids.add(ts.get_id());
            }
        }
        if (ids.size() != names.size()) {
            throw new Exception("Could not find all topologies: "+names);
        }
        int uptime = 0;
        long acked = 0;
        long failed = 0;
        for (String id: ids) {
            TopologyInfo info = client.getTopologyInfo(id);
            uptime = Math.max(uptime, info.get_uptime_secs());
            for (ExecutorSummary exec : info.get_executors()) {
                if ("spout".equals(exec.get_component_id()) && exec.get_stats() != null && exec.get_stats().get_specific() != null) {
                    SpoutStats stats = exec.get_stats().get_specific().get_spout();
                    Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                    Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                    if (ackedMap != null) {
                        for (String key : ackedMap.keySet()) {
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
        }
        long ackedThisTime = acked - prevAcked;
        long thisTime = uptime - prevUptime;
        prevUptime = uptime;
        prevAcked = acked;

        long nnpct, nnnpct, min, max;
        double mean, stddev;
        synchronized (histo) {
            nnpct = histo.getValueAtPercentile(99.0);
            nnnpct = histo.getValueAtPercentile(99.9);
            min = histo.getMinValue();
            max = histo.getMaxValue();
            mean = histo.getMean();
            stddev = histo.getStdDeviation();
            histo.reset();
        }
        long user = userCPU.getAndSet(0);
        long sys = systemCPU.getAndSet(0);
        long gc = gcMs.getAndSet(0);
        double memMB = readMemory() / (1024.0 * 1024.0);

        System.out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d " +
                "99%%: %,15d 99.9%%: %,15d min: %,15d max: %,15d mean: %,15.2f " +
                "stddev: %,15.2f user: %,10d sys: %,10d gc: %,10d mem: %,10.2f\n",
            uptime, ackedThisTime, (((double) ackedThisTime) / thisTime), failed, nnpct, nnnpct,
            min, max, mean, stddev, user, sys, gc, memMB);
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
                        synchronized(histo) {
                            histo.add((Histogram)dp.value);
                        }
                    } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object sys = m.get("sys-ms");
                        if (sys instanceof Number) {
                            systemCPU.getAndAdd(((Number)sys).longValue());
                        }
                        Object user = m.get("user-ms");
                        if (user instanceof Number) {
                            userCPU.getAndAdd(((Number)user).longValue());
                        }
                    } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object count = m.get("count");
                        if (count instanceof Number) {
                            gcCount.getAndAdd(((Number)count).longValue());
                        }
                        Object time = m.get("timeMs");
                        if (time instanceof Number) {
                            gcMs.getAndAdd(((Number)time).longValue());
                        }
                    } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object val = m.get("usedBytes");
                        if (val instanceof Number) {
                            MemMeasure mm = memoryBytes.get(worker);
                            if (mm == null) {
                                mm = new MemMeasure();
                                MemMeasure tmp = memoryBytes.putIfAbsent(worker, mm);
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
        int exitStatus = -1;
        try (NimbusClient client = NimbusClient.getConfiguredClient(conf)) {
            List<String> topoNames = new ArrayList<>();
            try {
                for (String topoFile : args) {
                    try {
                        topoNames.add(parseAndSubmit(topoFile, url));
                    } catch (Exception e) {
                        System.err.println("Could Not Submit Topology From " + topoFile);
                        e.printStackTrace(System.err);
                    }
                }

                for (int i = 0; i < NUM_MINS * 2; i++) {
                    Thread.sleep(30 * 1000);
                    printMetrics(client.getClient(), topoNames);
                }
            } finally {
                for (String topoName : topoNames) {
                    try {
                        kill(client.getClient(), topoName);
                    } catch (Exception e) {
                        System.err.println("Could not kill " + topoName);
                        e.printStackTrace();
                    }
                }
            }
            exitStatus = 0;
        } finally {
            System.exit(exitStatus);
        }
    }

    static int uniquifier = 0;

    private static String parseAndSubmit(String topoFile, String url) throws IOException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        File f = new File(topoFile);
        String fileName = f.getName();
        int dot = fileName.lastIndexOf('.');
        final String topoName = fileName.substring(0, dot) + "-" + uniquifier++;

        //This is a format that is an extension/subset of flux
        // spouts:
        //   - id: "spout"
        //     parallelism: 1
        //     streams:
        //       - rate:
        //           mean: 1000
        //           stddev: 200
        //           min: 500
        //           max: 2000
        //         areKeysSkewed: false
        //
        // # bolt definitions
        // bolts:
        //   - id: "level1"
        //     parallelism: 1
        //     streams:
        //       - rate:
        //           mean: 10000
        //           stddev: 2000
        //           min: 5000
        //           max: 12000
        //        areKeysSkewed: true
        //   - id: "level2"
        //     parallelism: 1
        //
        // #stream definitions
        // streams:
        //   - name: "spout --> level1"
        //     from: "spout"
        //     to: "level1"
        //     grouping:
        //       type: SHUFFLE
        //   - name: "level1 --> level2"
        //     from: "level1"
        //     to: "level2"
        //     grouping:
        //       type: FIELDS

        Yaml yaml = new Yaml(new SafeConstructor());
        @SuppressWarnings("unchecked")
        Map<String, Object> yamlConf = (Map<String, Object>)yaml.load(new FileReader(f));

        //First we need some configs
        Config conf = new Config();
        if (yamlConf.containsKey("config")) {
            conf.putAll((Map<String, Object>)yamlConf.get("config"));
        }
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(org.apache.storm.misc.metric.HttpForwardingMetricsConsumer.class, url, 1);
        Map<String, String> workerMetrics = new HashMap<>();
        if (!NimbusClient.isLocalOverride()) {
            //sigar uses JNI and does not work in local mode
            workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
        }
        conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);

        TopologyBuilder builder = new TopologyBuilder();
        //Lets build a topology.
        List<Map<String, Object>> spoutInfos = (List<Map<String, Object>>) yamlConf.get("spouts");
        for (Map<String, Object> spoutInfo: spoutInfos) {
            String id = (String) spoutInfo.get("id");
            if (id == null) {
                throw new IllegalArgumentException("A spout ID cannot be null");
            }
            //TODO we need a way to scale these up and/or down...
            int parallelism = ObjectReader.getInt(spoutInfo.get("parallelism"), 1);
            List<StreamStats> streams = new ArrayList<>();
            List<Map<String, Object>> streamData = (List<Map<String, Object>>) spoutInfo.get("streams");
            if (streamData != null) {
                for (Map<String, Object> streamInfo: streamData) {
                    String streamId = (String)streamInfo.getOrDefault("streamId", "default");
                    Map<String, Object> rate = (Map<String, Object>) streamInfo.get("rate");
                    double mean = ObjectReader.getDouble(rate.get("mean"));
                    double stddev = ObjectReader.getDouble(rate.get("stddev"), mean/4);
                    double min = ObjectReader.getDouble(rate.get("min"), 0.0);
                    double max = ObjectReader.getDouble(rate.get("max"), Double.MAX_VALUE);
                    NormalDistStats dist = new NormalDistStats(mean, stddev, min, max);
                    boolean areKeysSkewed = ObjectReader.getBoolean(streamInfo.get("areKeysSkewed"), false);
                    streams.add(new StreamStats(streamId, dist, areKeysSkewed));
                }
            }

            System.out.println("ADDING SPOUT " + id);
            builder.setSpout(id, new LoadSpout(streams), parallelism);
        }

        Map<String, BoltDeclarer> boltDeclarers = new HashMap<>();
        List<Map<String, Object>> boltInfos = (List<Map<String, Object>>) yamlConf.get("bolts");
        if (boltInfos != null) {
            for (Map<String, Object> boltInfo : boltInfos) {
                String id = (String) boltInfo.get("id");
                if (id == null) {
                    throw new IllegalArgumentException("A bolt ID cannot be null");
                }
                //TODO we need a way to scale these up and/or down...
                int parallelism = ObjectReader.getInt(boltInfo.get("parallelism"), 1);
                List<StreamStats> streams = new ArrayList<>();
                List<Map<String, Object>> streamData = (List<Map<String, Object>>) boltInfo.get("streams");
                if (streamData != null) {
                    //TODO need to make this common instead of copy/paste
                    for (Map<String, Object> streamInfo : streamData) {
                        String streamId = (String) streamInfo.getOrDefault("streamId", "default");
                        Map<String, Object> rate = (Map<String, Object>) streamInfo.get("rate");
                        double mean = ObjectReader.getDouble(rate.get("mean"));
                        double stddev = ObjectReader.getDouble(rate.get("stddev"), mean / 4);
                        double min = ObjectReader.getDouble(rate.get("min"), 0.0);
                        double max = ObjectReader.getDouble(rate.get("max"), Double.MAX_VALUE);
                        NormalDistStats dist = new NormalDistStats(mean, stddev, min, max);
                        boolean areKeysSkewed = ObjectReader.getBoolean(streamInfo.get("areKeysSkewed"), false);
                        streams.add(new StreamStats(streamId, dist, areKeysSkewed));
                    }
                }

                System.out.println("ADDING BOLT " + id);
                boltDeclarers.put(id, builder.setBolt(id, new LoadBolt(streams), parallelism));
            }
        }

        List<Map<String, Object>> streamInfos = (List<Map<String, Object>>) yamlConf.get("streams");
        if (streamInfos != null) {
            for (Map<String, Object> streamInfo: streamInfos) {
                String from = (String) streamInfo.get("from");
                if (from == null) {
                    throw new IllegalArgumentException("from cannot be null");
                }
                String to = (String) streamInfo.get("to");
                if (to == null) {
                    throw new IllegalArgumentException("to cannot be null");
                }
                BoltDeclarer declarer = boltDeclarers.get(to);
                if (declarer == null) {
                    throw new IllegalArgumentException("to bolt " + to + " does not exist");
                }
                Map<String, Object> grouping = (Map<String, Object>) streamInfo.get("grouping");
                String streamId = "default";
                String type = "SHUFFLE";
                if (grouping != null) {
                    type = (String) grouping.getOrDefault("type", "SHUFFLE");
                    streamId = (String) grouping.getOrDefault("streamId", "default");
                }
                type = type.toUpperCase(Locale.ENGLISH);
                switch (type) {
                    case "SHUFFLE":
                        declarer.shuffleGrouping(from, streamId);
                        break;
                    case "FIELDS":
                        declarer.fieldsGrouping(from, streamId, new Fields("key"));
                        break;
                    case "ALL":
                        declarer.allGrouping(from, streamId);
                        break;
                    case "GLOBAL":
                        declarer.globalGrouping(from, streamId);
                        break;
                    case "LOCAL_OR_SHUFFLE":
                        declarer.localOrShuffleGrouping(from, streamId);
                        break;
                    case "NONE":
                        declarer.noneGrouping(from, streamId);
                        break;
                    case "PARTIAL_KEY":
                        declarer.partialKeyGrouping(from, streamId, new Fields("key"));
                        break;
                    default:
                        throw new IllegalArgumentException(type + " is not a supported grouping...");
                }
            }
        }
        //TODO is this a YAML???  What are we actually going to do with this???
        StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
        return topoName;
    }
}