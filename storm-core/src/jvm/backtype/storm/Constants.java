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
package backtype.storm;

import backtype.storm.coordination.CoordinatedBolt;
import clojure.lang.RT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Constants {
    public static final String COORDINATED_STREAM_ID = CoordinatedBolt.class.getName() + "/coord-stream"; 

    public static final long SYSTEM_TASK_ID = -1;
    public static final Object SYSTEM_EXECUTOR_ID = RT.readString("[-1 -1]");
    public static final String SYSTEM_COMPONENT_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
    public static final String METRICS_COMPONENT_ID_PREFIX = "__metrics";
    public static final String METRICS_STREAM_ID = "__metrics";
    public static final String METRICS_TICK_STREAM_ID = "__metrics_tick";
    public static final String CREDENTIALS_CHANGED_STREAM_ID = "__credentials";

    public static final String ACKER_COMPONENT_ID = "__acker";
    public static final String ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
    public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";
    public static final String ACKER_RESET_TIMEOUT_STREAM_ID = "__ack_reset_timeout";

    public static final String COMMON_CPU_RESOURCE_NAME = "cpu.pcore.percent";
    public static final String COMMON_ONHEAP_MEMORY_RESOURCE_NAME = "onheap.memory.mb";
    public static final String COMMON_OFFHEAP_MEMORY_RESOURCE_NAME = "offheap.memory.mb";
    public static final String COMMON_TOTAL_MEMORY_RESOURCE_NAME = "memory.mb";

    public static final Map<String, String> resourceNameMapping;

    static {
        Map<String, String> tmp = new HashMap<>();
        tmp.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_CPU_CAPACITY, COMMON_CPU_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, COMMON_ONHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, COMMON_OFFHEAP_MEMORY_RESOURCE_NAME);
        tmp.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, COMMON_TOTAL_MEMORY_RESOURCE_NAME);
        resourceNameMapping = Collections.unmodifiableMap(tmp);
    }

}
    
