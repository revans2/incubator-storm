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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

    private static TopologyLoadConf captureTopology(Nimbus.Iface client, TopologySummary topologySummary) throws Exception {
        String topologyName = topologySummary.get_name();
        LOG.info("Capturing {}...", topologyName);
        String topologyId = topologySummary.get_id();
        TopologyInfo info = client.getTopologyInfo(topologyId);
        TopologyPageInfo tpinfo = client.getTopologyPageInfo(topologyId, ":all-time", false);
        StormTopology topo = client.getUserTopology(topologyId);
        Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(client.getTopologyConf(topologyId));
        Map<String, Object> savedTopoConf = new HashMap<>();
        for (String key: TopologyLoadConf.IMPORTANT_CONF_KEYS) {
            Object o = topoConf.get(key);
            if (o != null) {
                savedTopoConf.put(key, o);
                LOG.info("with config {}: {}", key, o);
            }
        }
        //Lets use the number of actually scheduled workers as a way to bridge RAS and non-RAS
        int numWorkers = tpinfo.get_num_workers();
        if (savedTopoConf.containsKey(Config.TOPOLOGY_WORKERS)) {
            numWorkers = Math.max(numWorkers, ((Number)savedTopoConf.get(Config.TOPOLOGY_WORKERS)).intValue());
        }
        savedTopoConf.put(Config.TOPOLOGY_WORKERS, numWorkers);

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

        return new TopologyLoadConf(topologyName, savedTopoConf, spouts, bolts, streams);
    }

    public static final String DEFAULT_OUT_DIR = "./loadgen/";
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(Option.builder("a")
            .longOpt("anonymize")
            .desc("Strip out any possibly identifiable information")
            .build());
        options.addOption(Option.builder("o")
            .longOpt("output-dir")
            .argName("<file>")
            .desc("Where to write (defaults to " + DEFAULT_OUT_DIR + ")")
            .build());
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Print a help message")
            .build());
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        ParseException pe = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            pe = e;
        }
        if (pe != null || cmd.hasOption('h')) {
            if (pe != null) {
                System.err.println("ERROR " + pe.getMessage());
            }
            new HelpFormatter().printHelp("CaptureLoad [options] [topologyName]*", options);
            return;
        }

        Config conf = new Config();
        int exitStatus = -1;
        String outputDir = DEFAULT_OUT_DIR;
        if (cmd.hasOption('o')) {
            outputDir = cmd.getOptionValue('o');
        }
        File baseOut = new File(outputDir);
        LOG.info("Will save captured topologies to {}", baseOut);
        baseOut.mkdirs();

        try (NimbusClient nc = NimbusClient.getConfiguredClient(conf)) {
            Nimbus.Iface client = nc.getClient();
            List<String> topologyNames = cmd.getArgList();

            ClusterSummary clusterSummary = client.getClusterInfo();
            for (TopologySummary topologySummary: clusterSummary.get_topologies()) {
                if (topologyNames.isEmpty() || topologyNames.contains(topologySummary.get_name())) {
                    TopologyLoadConf capturedConf = captureTopology(client, topologySummary);
                    if (cmd.hasOption('a')) {
                        capturedConf = capturedConf.anonymize();
                    }
                    capturedConf.writeTo(new File(baseOut, capturedConf.name + ".yaml"));
                }
            }

            exitStatus = 0;
        } catch (Exception e) {
            LOG.error("Error trying to capture topologies...", e);
        } finally {
            System.exit(exitStatus);
        }
    }
}