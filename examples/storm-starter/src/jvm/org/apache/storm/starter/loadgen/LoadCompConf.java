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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.utils.ObjectReader;

/**
 * Configuration for a simulated spout.
 */
public class LoadCompConf {
    public final String id;
    public final int parallelism;
    public final List<OutputStream> streams;
    public final CompStats stats;

    public static LoadCompConf fromConf(Map<String, Object> conf) {
        String id = (String) conf.get("id");
        int parallelism = ObjectReader.getInt(conf.get("parallelism"), 1);
        List<OutputStream> streams = new ArrayList<>();
        List<Map<String, Object>> streamData = (List<Map<String, Object>>) conf.get("streams");
        if (streamData != null) {
            for (Map<String, Object> streamInfo: streamData) {
                streams.add(OutputStream.fromConf(streamInfo));
            }
        }

        return new LoadCompConf(id, parallelism, streams, CompStats.fromConf(conf));
    }

    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("id", id);
        ret.put("parallelism", parallelism);

        if (streams != null) {
            List<Map<String, Object>> streamData = new ArrayList<>();
            for (OutputStream out : streams) {
                streamData.add(out.toConf());
            }
            ret.put("streams", streamData);
        }
        if (stats != null) {
            stats.addToConf(ret);
        }
        return ret;
    }

    public LoadCompConf remap(Map<String, String> remappedComponents, Map<GlobalStreamId, GlobalStreamId> remappedStreams) {
        String remappedId = remappedComponents.get(id);
        List<OutputStream> remappedOutStreams = streams.stream()
            .map((orig) -> orig.remap(id, remappedStreams))
            .collect(Collectors.toList());

        return new LoadCompConf(remappedId, parallelism, remappedOutStreams, stats);
    }

    public static class Builder {
        private String id;
        private int parallelism = 1;
        private List<OutputStream> streams;
        private CompStats stats;

        public String getId() {
            return id;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public int getParallelism() {
            return parallelism;
        }

        public Builder withParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public List<OutputStream> getStreams() {
            return streams;
        }

        public Builder withStream(OutputStream stream) {
            if (streams == null) {
                streams = new ArrayList<>();
            }
            streams.add(stream);
            return this;
        }

        public Builder withStreams(List<OutputStream> streams) {
            this.streams = streams;
            return this;
        }

        public CompStats getStats() {
            return stats;
        }

        public Builder withStats(CompStats stats) {
            this.stats = stats;
            return this;
        }

        public LoadCompConf build() {
            return new LoadCompConf(id, parallelism, streams, stats);
        }
    }

    public LoadCompConf(String id, int parallelism, List<OutputStream> streams, CompStats stats) {
        this.id = id;
        if (id == null) {
            throw new IllegalArgumentException("A spout ID cannot be null");
        }
        this.parallelism = parallelism;
        this.streams = streams;
        this.stats = stats;
    }
}
