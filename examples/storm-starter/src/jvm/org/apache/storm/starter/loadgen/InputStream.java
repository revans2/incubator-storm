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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A set of measurements about a stream so we can statistically reproduce it.
 */
public class InputStream implements Serializable {
    public final String fromComponent;
    public final String toComponent;
    public final String id;
    public final NormalDistStats execTime;
    public final NormalDistStats processTime;
    public final GroupingType groupingType;

    /**
     * Create an output stream from a config.
     * @param conf the config to read from.
     * @return the read OutputStream.
     */
    public static InputStream fromConf(Map<String, Object> conf) {
        String component = (String) conf.get("from");
        String toComp = (String) conf.get("to");
        NormalDistStats execTime = NormalDistStats.fromConf((Map<String, Object>) conf.get("execTime"));
        NormalDistStats processTime = NormalDistStats.fromConf((Map<String, Object>) conf.get("processTime"));
        Map<String, Object> grouping = (Map<String, Object>) conf.get("grouping");
        GroupingType groupingType = GroupingType.fromConf((String) grouping.get("type"));
        String streamId = (String) grouping.getOrDefault("streamId", "default");
        return new InputStream(component, toComp, streamId, execTime, processTime, groupingType);
    }

    /**
     * Convert this to a conf.
     * @return the conf.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("from", fromComponent);
        ret.put("to", toComponent);
        ret.put("execTime", execTime.toConf());
        ret.put("processTime", processTime.toConf());

        Map<String, Object> grouping = new HashMap<>();
        grouping.put("streamId", id);
        grouping.put("type", groupingType.toConf());
        ret.put("grouping", grouping);

        return ret;
    }

    /**
     * Create a new input stream to a bolt.
     * @param fromComponent the source component of the stream.
     * @param id the id of the stream
     * @param execTime exec time stats
     * @param processTime process time stats
     */
    public InputStream(String fromComponent, String toComponent, String id, NormalDistStats execTime,
                       NormalDistStats processTime, GroupingType groupingType) {
        this.fromComponent = fromComponent;
        this.toComponent = toComponent;
        if (fromComponent == null) {
            throw new IllegalArgumentException("from cannot be null");
        }
        if (toComponent == null) {
            throw new IllegalArgumentException("to cannot be null");
        }
        this.id = id;
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        this.execTime = execTime;
        this.processTime = processTime;
        this.groupingType = groupingType;
    }
}
