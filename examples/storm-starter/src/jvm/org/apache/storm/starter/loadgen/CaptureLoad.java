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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerSummary;
import org.apache.storm.utils.NimbusClient;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Capture running topologies for load gen later on.
 */
public class CaptureLoad {
    private final static Logger LOG = LoggerFactory.getLogger(CaptureLoad.class);

    private static final Set<String> IMPORTANT_CONF_KEYS = Collections.unmodifiableSet(new HashSet(Arrays.asList(
        Config.TOPOLOGY_WORKERS,
        Config.TOPOLOGY_ACKER_EXECUTORS,
        Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
        Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
        Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
        Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING,
        Config.TOPOLOGY_DEBUG,
        Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
        Config.TOPOLOGY_FLUSH_TUPLE_FREQ_MILLIS,
        Config.TOPOLOGY_ISOLATED_MACHINES,
        Config.TOPOLOGY_MAX_SPOUT_PENDING,
        Config.TOPOLOGY_MAX_TASK_PARALLELISM,
        Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        Config.TOPOLOGY_PRIORITY,
        Config.TOPOLOGY_PRODUCER_BATCH_SIZE,
        Config.TOPOLOGY_SCHEDULER_STRATEGY,
        Config.TOPOLOGY_SHELLBOLT_MAX_PENDING,
        Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS,
        Config.TOPOLOGY_SPOUT_WAIT_STRATEGY,
        Config.TOPOLOGY_WORKER_CHILDOPTS,
        Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
        Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE
    )));

    private static List<Double> extractBoltValues(List<ExecutorSummary> summaries,
                                                  GlobalStreamId id,
                                                  Function<BoltStats, Map<String, Map<GlobalStreamId, Double>>> func) {

        List<Double> ret = new ArrayList<>();
        for (ExecutorSummary summ: summaries) {
            if (summ.is_set_stats()) {
                Map<String, Map<GlobalStreamId, Double>> data = func.apply(summ.get_stats().get_specific().get_bolt());
                if (data != null) {
                    List<Double> subvalues = data.values().stream()
                        .map((subMap) -> subMap.get(id))
                        .filter((value) -> value != null)
                        .mapToDouble((value) -> value.doubleValue())
                        .boxed().collect(Collectors.toList());
                    ret.addAll(subvalues);
                }
            }
        }
        return ret;
    }

    private static void captureTopology(Nimbus.Iface client, TopologySummary topologySummary, File baseOut) throws Exception {
        String topologyName = topologySummary.get_name();
        LOG.info("Capturing {}...", topologyName);
        String topologyId = topologySummary.get_id();
        TopologyInfo info = client.getTopologyInfo(topologyId);
        TopologyPageInfo tpinfo = client.getTopologyPageInfo(topologyId, ":all-time", false);
        StormTopology topo = client.getUserTopology(topologyId);
        Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(client.getTopologyConf(topologyId));
        Map<String, Object> savedTopoConf = new HashMap<>();
        for (String key: IMPORTANT_CONF_KEYS) {
            Object o = topoConf.get(key);
            if (o != null) {
                savedTopoConf.put(key, o);
                LOG.info("with config {}: {}", key, o);
            }
        }

        Map<String, LoadCompConf.Builder> boltBuilders = new HashMap<>();
        Map<String, LoadCompConf.Builder> spoutBuilders = new HashMap<>();
        List<InputStream.Builder> inputStreams = new ArrayList<>();
        Map<GlobalStreamId, OutputStream.Builder> outStreams = new HashMap<>();

        //Bolts
        if (topo.get_bolts() != null){
            for (Map.Entry<String, Bolt> boltSpec : topo.get_bolts().entrySet()) {
                String boltComp = boltSpec.getKey();
                LOG.info("Found bolt {}...", boltComp);
                Bolt bolt = boltSpec.getValue();
                ComponentCommon common = bolt.get_common();
                Map<GlobalStreamId, Grouping> inputs = common.get_inputs();
                if (inputs != null) {
                    for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
                        GlobalStreamId id = input.getKey();
                        LOG.info("with input {}...", id);
                        Grouping grouping = input.getValue();
                        InputStream.Builder builder = new InputStream.Builder()
                            .withId(id.get_streamId())
                            .withFromComponent(id.get_componentId())
                            .withToComponent(boltComp)
                            .withGroupingType(grouping);
                        inputStreams.add(builder);
                    }
                }
                Map<String, StreamInfo> outputs = common.get_streams();
                if (outputs != null) {
                    for (String name : outputs.keySet()) {
                        GlobalStreamId id = new GlobalStreamId(boltComp, name);
                        LOG.info("and output {}...", id);
                        OutputStream.Builder builder = new OutputStream.Builder()
                            .withId(name);
                        outStreams.put(id, builder);
                    }
                }
                LoadCompConf.Builder builder = new LoadCompConf.Builder()
                    .withParallelism(common.get_parallelism_hint())
                    .withId(boltComp);
                boltBuilders.put(boltComp, builder);
            }
        }

        //Spouts
        if (topo.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spoutSpec : topo.get_spouts().entrySet()) {
                String spoutComp = spoutSpec.getKey();
                LOG.info("Found Spout {}...", spoutComp);
                SpoutSpec spout = spoutSpec.getValue();
                ComponentCommon common = spout.get_common();

                Map<String, StreamInfo> outputs = common.get_streams();
                if (outputs != null) {
                    for (String name : outputs.keySet()) {
                        GlobalStreamId id = new GlobalStreamId(spoutComp, name);
                        LOG.info("with output {}...", id);
                        OutputStream.Builder builder = new OutputStream.Builder()
                            .withId(name);
                        outStreams.put(id, builder);
                    }
                }
                LoadCompConf.Builder builder = new LoadCompConf.Builder()
                    .withParallelism(common.get_parallelism_hint())
                    .withId(spoutComp);
                spoutBuilders.put(spoutComp, builder);
            }
        }

        //Stats...
        Map<String, List<ExecutorSummary>> byComponent = new HashMap<>();
        for (ExecutorSummary executor: info.get_executors()) {
            String component = executor.get_component_id();
            List<ExecutorSummary> list = byComponent.get(component);
            if (list == null) {
                list = new ArrayList<>();
                byComponent.put(component, list);
            }
            list.add(executor);
        }

        List<InputStream> streams = new ArrayList<>(inputStreams.size());
        //Compute the stats for the different input streams
        for (InputStream.Builder builder : inputStreams) {
            GlobalStreamId streamId = new GlobalStreamId(builder.getFromComponent(), builder.getId());
            List<ExecutorSummary> summaries = byComponent.get(builder.getToComponent());
            //Execute and process latency...
            builder.withProcessTime(new NormalDistStats(
                extractBoltValues(summaries, streamId, BoltStats::get_process_ms_avg)));
            builder.withExecTime(new NormalDistStats(
                extractBoltValues(summaries, streamId, BoltStats::get_execute_ms_avg)));
            //InputStream is done
            streams.add(builder.build());
        }

        //There is a bug in some versions that returns 0 for the uptime.
        // To work around it we should get it an alternative (working) way.
        Map<String, Integer> workerToUptime = new HashMap<>();
        for (WorkerSummary ws : tpinfo.get_workers()) {
            workerToUptime.put(ws.get_supervisor_id() + ":" + ws.get_port(), ws.get_uptime_secs());
        }
        LOG.debug("WORKER TO UPTIME {}", workerToUptime);

        for (Map.Entry<GlobalStreamId, OutputStream.Builder> entry : outStreams.entrySet()) {
            OutputStream.Builder builder = entry.getValue();
            GlobalStreamId id = entry.getKey();
            List<Double> emittedRate = new ArrayList<>();
            List<ExecutorSummary> summaries = byComponent.get(id.get_componentId());
            for (ExecutorSummary summary: summaries) {
                if (summary.is_set_stats()) {
                    int uptime = summary.get_uptime_secs();
                    LOG.debug("UPTIME {}", uptime);
                    if (uptime <= 0) {
                        //Likely it is because of a bug, so try to get it another way
                        String key = summary.get_host() + ":" + summary.get_port();
                        uptime = workerToUptime.getOrDefault(key, 1);
                        LOG.debug("Getting uptime for worker {}, {}", key, uptime);
                    }
                    for (Map.Entry<String, Map<String, Long>> statEntry : summary.get_stats().get_emitted().entrySet()) {
                        String timeWindow = statEntry.getKey();
                        long timeSecs = uptime;
                        try {
                            timeSecs = Long.valueOf(timeWindow);
                        } catch (NumberFormatException e) {
                            //Ignored...
                        }
                        timeSecs = Math.min(timeSecs, uptime);
                        Long count = statEntry.getValue().get(id.get_streamId());
                        if (count != null) {
                            LOG.debug("{} emitted {} for {} secs or {} tuples/sec", id, count, timeSecs, count.doubleValue()/timeSecs);
                            emittedRate.add(count.doubleValue()/timeSecs);
                        }
                    }
                }

            }
            builder.withRate(new NormalDistStats(emittedRate));
            //TODO to know if the output keys are skewed we have to guess by looking
            // at the down stream executed stats, but for now we are going to ignore it

            //The OutputStream is done
            LoadCompConf.Builder comp = boltBuilders.get(id.get_componentId());
            if (comp == null) {
                comp = spoutBuilders.get(id.get_componentId());
            }
            comp.withStream(builder.build());
        }

        List<LoadCompConf> spouts = spoutBuilders.values().stream()
            .map((b) -> b.build())
            .collect(Collectors.toList());

        List<LoadCompConf> bolts = boltBuilders.values().stream()
            .map((b) -> b.build())
            .collect(Collectors.toList());

        TopologyLoadConf finalConf = new TopologyLoadConf(topologyName, savedTopoConf, spouts, bolts, streams);
        finalConf.writeTo(new File(baseOut, topologyName + ".yaml"));
    }

    private static void captureTopologies(Nimbus.Iface client, List<String> topologyNames, File baseOut) throws Exception {
        ClusterSummary clusterSummary = client.getClusterInfo();
        for (TopologySummary topologySummary: clusterSummary.get_topologies()) {
            if (topologyNames.isEmpty() || topologyNames.contains(topologySummary.get_name())) {
                captureTopology(client, topologySummary, baseOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        int exitStatus = -1;
        String outputDir = "./loadgen/";
        if (args.length > 0) {
            outputDir = args[0];
        }
        File baseOut = new File(outputDir);
        LOG.info("Will save captured topologies to {}", baseOut);
        baseOut.mkdirs();

        try (NimbusClient client = NimbusClient.getConfiguredClient(conf)) {
            List<String> topologyNames = new ArrayList<>();
            if (args.length > 1) {
                for (int i = 1; i < args.length; i++) {
                    topologyNames.add(args[i]);
                }
            }
            captureTopologies(client.getClient(), topologyNames, baseOut);

            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error trying to capture topologies...", e);
        } finally {
            System.exit(exitStatus);
        }
    }
}