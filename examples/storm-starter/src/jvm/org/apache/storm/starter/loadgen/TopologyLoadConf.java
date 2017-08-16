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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Configuration for a simulated topology.
 */
public class TopologyLoadConf {
    private final static Logger LOG = LoggerFactory.getLogger(TopologyLoadConf.class);
    static final Set<String> IMPORTANT_CONF_KEYS = Collections.unmodifiableSet(new HashSet(Arrays.asList(
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

    //TODO we need to save the owner
    public final String name;
    public final Map<String, Object> topoConf;
    public final List<LoadCompConf> spouts;
    public final List<LoadCompConf> bolts;
    public final List<InputStream> streams;

    public static TopologyLoadConf fromConf(File file) throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor());
        Map<String, Object> yamlConf = (Map<String, Object>)yaml.load(new FileReader(file));
        return TopologyLoadConf.fromConf(yamlConf);
    }

    public static TopologyLoadConf fromConf(Map<String, Object> conf) {
        String name = (String) conf.get("name");

        Map<String, Object> topoConf = null;
        if (conf.containsKey("config")) {
            topoConf = new HashMap<>((Map<String, Object>)conf.get("config"));
        }

        List<LoadCompConf> spouts = new ArrayList<>();
        for (Map<String, Object> spoutInfo: (List<Map<String, Object>>) conf.get("spouts")) {
            spouts.add(LoadCompConf.fromConf(spoutInfo));
        }

        List<LoadCompConf> bolts = new ArrayList<>();
        List<Map<String, Object>> boltInfos = (List<Map<String, Object>>) conf.get("bolts");
        if (boltInfos != null) {
            for (Map<String, Object> boltInfo : boltInfos) {
                bolts.add(LoadCompConf.fromConf(boltInfo));
            }
        }

        List<InputStream> streams = new ArrayList<>();
        List<Map<String, Object>> streamInfos = (List<Map<String, Object>>) conf.get("streams");
        if (streamInfos != null) {
            for (Map<String, Object> streamInfo: streamInfos) {
                streams.add(InputStream.fromConf(streamInfo));
            }
        }

        return new TopologyLoadConf(name, topoConf, spouts, bolts, streams);
    }

    public void writeTo(File file) throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor());
        try (FileWriter writer = new FileWriter(file)) {
            yaml.dump(toConf(), writer);
        }
    }

    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        if (name != null) {
            ret.put("name", name);
        }
        if (topoConf != null) {
            ret.put("config", topoConf);
        }
        if (spouts != null && !spouts.isEmpty()) {
            ret.put("spouts", spouts.stream().map(LoadCompConf::toConf)
                .collect(Collectors.toList()));
        }

        if (bolts != null && !bolts.isEmpty()) {
            ret.put("bolts", bolts.stream().map(LoadCompConf::toConf)
                .collect(Collectors.toList()));
        }

        if (streams != null && !streams.isEmpty()) {
            ret.put("streams", streams.stream().map(InputStream::toConf)
                .collect(Collectors.toList()));
        }
        return ret;
    }

    public TopologyLoadConf(String name, Map<String, Object> topoConf,
                            List<LoadCompConf> spouts, List<LoadCompConf> bolts, List<InputStream> streams) {
        this.name = name;
        this.topoConf = topoConf;
        this.spouts = spouts;
        this.bolts = bolts;
        this.streams = streams;
    }

    private static AtomicInteger topoUniquifier = new AtomicInteger(0);
    private static String getUniqueTopoName() {
        return "topology_" + asCharString(topoUniquifier.getAndIncrement());
    }

    private AtomicInteger boltUniquifier = new AtomicInteger(0);
    private String getUniqueBoltName() {
        return "bolt_" + asCharString(boltUniquifier.getAndIncrement());
    }

    private AtomicInteger spoutUniquifier = new AtomicInteger(0);
    private String getUniqueSpoutName() {
        return "spout_" + asCharString(spoutUniquifier.getAndIncrement());
    }

    private AtomicInteger streamUniquifier = new AtomicInteger(0);
    private String getUniqueStreamName() {
        return "stream_" + asCharString(spoutUniquifier.getAndIncrement());
    }

    private static String asCharString(int value) {
        int div = value/26;
        int remainder = value % 26;
        String ret = "";
        if (div > 0) {
            ret = asCharString(div);
        }
        ret += (char)((int)'a' + remainder);
        return ret;
    }

    /**
     * Create a new version of this topology with identifiable information removed.
     * @return the anonymized version of the TopologyLoadConf.
     */
    public TopologyLoadConf anonymize() {
        String newName = getUniqueTopoName();
        //topoConf this is not going to be simple (have to parse/strip command line args)
        //spouts, bolts, and streams all have names that reference each other, so we need to
        //collect them all and then anonymize the names and then have each replace them all.
        Map<String, Object> remappedConf = anonymizeTopoConf(topoConf);

        Map<String, String> remappedComponents = new HashMap<>();
        Map<GlobalStreamId, GlobalStreamId> remappedStreams = new HashMap<>();
        for (LoadCompConf comp: bolts) {
            String newId = getUniqueBoltName();
            remappedComponents.put(comp.id, newId);
            if (comp.streams != null) {
                for (OutputStream out : comp.streams) {
                    GlobalStreamId orig = new GlobalStreamId(comp.id, out.id);
                    GlobalStreamId remapped = new GlobalStreamId(newId, getUniqueStreamName());
                    remappedStreams.put(orig, remapped);
                }
            }
        }

        for (LoadCompConf comp: spouts) {
            remappedComponents.put(comp.id, getUniqueSpoutName());
            String newId = getUniqueSpoutName();
            remappedComponents.put(comp.id, newId);
            if (comp.streams != null) {
                for (OutputStream out : comp.streams) {
                    GlobalStreamId orig = new GlobalStreamId(comp.id, out.id);
                    GlobalStreamId remapped = new GlobalStreamId(newId, getUniqueStreamName());
                    remappedStreams.put(orig, remapped);
                }
            }
        }

        for (InputStream in : streams) {
            if (!remappedComponents.containsKey(in.toComponent)) {
                remappedComponents.put(in.toComponent, getUniqueSpoutName());
            }
            GlobalStreamId orig = in.gsid();
            if (!remappedStreams.containsKey(orig)) {
                //Even if the topology is not valid we still need to remap it all
                String remappedComp = remappedComponents.computeIfAbsent(in.fromComponent, (key) -> {
                    LOG.warn("stream's {} from is not defined {}", in.id, in.fromComponent);
                    return getUniqueBoltName();
                });
                remappedStreams.put(orig, new GlobalStreamId(remappedComp, getUniqueStreamName()));
            }
        }

        //Now we need to map them all back again
        List<LoadCompConf> remappedSpouts = spouts.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        List<LoadCompConf> remappedBolts = bolts.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        List<InputStream> remappedInputStreams = streams.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        return new TopologyLoadConf(newName, remappedConf, remappedSpouts, remappedBolts, remappedInputStreams);
    }

    private static Map<String,Object> anonymizeTopoConf(Map<String, Object> topoConf) {
        //Only keep important conf keys
        Map<String, Object> ret = new HashMap<>();
        for (Map.Entry<String, Object> entry: topoConf.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (IMPORTANT_CONF_KEYS.contains(key)) {
                if (Config.TOPOLOGY_WORKER_CHILDOPTS.equals(key) ||
                    Config.TOPOLOGY_WORKER_GC_CHILDOPTS.equals(key)) {
                    value = cleanupChildOpts(value);
                }
                ret.put(key, value);
            }
        }
        return ret;
    }

    private static Object cleanupChildOpts(Object value) {
        if (value instanceof String) {
            String sv = (String) value;
            StringBuffer ret = new StringBuffer();
            for (String part: sv.split("\\s+")) {
                if (part.startsWith("-X")) {
                    ret.append(part).append(" ");
                }
            }
            return ret.toString();
        } else {
            List<String> ret = new ArrayList<>();
            for (String subValue: (Collection<String>)value) {
                ret.add((String)cleanupChildOpts(subValue));
            }
            return ret.stream().filter((item) -> item != null && !item.isEmpty()).collect(Collectors.toList());
        }
    }
}
