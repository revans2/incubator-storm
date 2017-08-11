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
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.json.simple.JSONValue;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Capture running topologies for load gen later on.
 */
public class CaptureLoad {

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

    private static void captureTopology(Nimbus.Iface client, TopologySummary topologySummary, File baseOut) throws Exception {
        String topologyName = topologySummary.get_name();
        String topologyId = topologySummary.get_id();
        TopologyInfo info = client.getTopologyInfo(topologyId);
        StormTopology topo = client.getUserTopology(topologyId);

        Map<String, Object> yamlConf = new HashMap<>();

        yamlConf.put("name", topologyName);

        Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(client.getTopologyConf(topologyId));
        Map<String, Object> savedTopoConf = new HashMap<>();
        for (String key: IMPORTANT_CONF_KEYS) {
            Object o = topoConf.get(key);
            if (o != null) {
                savedTopoConf.put(key, o);
            }
        }
        //We want a Spout with global output streams, and stats for each output stream
        //We want a Bolt with global input streams/groupings/exec/process latency stats
        //     and global output streams, and stats for each output stream
        //We want to compute some stats about spouts and bolts, so we want to get every spout and every bolt

        //First we want the graph of spouts connected to bolts with streams and groupings
        for (Map.Entry<String, SpoutSpec> spoutSpec : topo.get_spouts().entrySet()) {
            //From the spout I can get all of the inputs/grouping (should be empty here)
        }

        yamlConf.put("conf", savedTopoConf);
        for (ExecutorSummary exec : info.get_executors()) {
            //exec.
        }

        //spouts


        //bolts
        //streams

        Yaml yaml = new Yaml(new SafeConstructor());
        try (FileWriter writer = new FileWriter(new File(baseOut,  topologyName + ".yaml"))) {
            yaml.dump(yamlConf, writer);
        }
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
        if (args.length > 1) {
            outputDir = args[0];
        }
        File baseOut = new File(outputDir);
        baseOut.mkdirs();

        try (NimbusClient client = NimbusClient.getConfiguredClient(conf)) {
            List<String> topologyNames = new ArrayList<>();
            if (args.length > 2) {
                for (int i = 1; i < args.length; i++) {
                    topologyNames.add(args[i]);
                }
            }
            captureTopologies(client.getClient(), topologyNames, baseOut);

            exitStatus = 0;
        } finally {
            System.exit(exitStatus);
        }
    }
}