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
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.scheduler.resource.RAS_TYPES;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseConfigurationDeclarer<T extends ComponentConfigurationDeclarer> implements ComponentConfigurationDeclarer<T> {
    @Override
    public T addConfiguration(String config, Object value) {
        Map configMap = new HashMap();
        configMap.put(config, value);
        return addConfigurations(configMap);
    }

    @Override
    public T setDebug(boolean debug) {
        return addConfiguration(Config.TOPOLOGY_DEBUG, debug);
    }

    @Override
    public T setMaxTaskParallelism(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_MAX_TASK_PARALLELISM, val);
    }

    @Override
    public T setMaxSpoutPending(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_MAX_SPOUT_PENDING, val);
    }
    
    @Override
    public T setNumTasks(Number val) {
        if(val!=null) val = val.intValue();
        return addConfiguration(Config.TOPOLOGY_TASKS, val);
    }

    @Override
    public T setMemoryLoad(Double onHeap) {
        return setMemoryLoad(onHeap, RAS_TYPES.DEFAULT_ONHEAP_MEMORY_REQUIREMENT);
    } 

    @Override
    public T setMemoryLoad(Double onHeap, Double offHeap) {
        if (onHeap != null) {
            onHeap = onHeap.doubleValue();
        }
        if (offHeap!=null) {
            offHeap = offHeap.doubleValue();
        }
        Map <String, Number> memoryMap = new HashMap<String, Number>();
        memoryMap.put(RAS_TYPES.TYPE_MEMORY_ONHEAP, onHeap);
        memoryMap.put(RAS_TYPES.TYPE_MEMORY_OFFHEAP, offHeap);
        return addConfiguration(Config.TOPOLOGY_RESOURCES_MEMORY_MB, memoryMap);
    }

    @Override
    public T setCPULoad(Double amount) {
        return addConfiguration(Config.TOPOLOGY_RESOURCES_CPU, amount);
    } 
}
