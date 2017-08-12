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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Configuration for a simulated topology.
 */
public class TopologyLoadConf {
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
}
