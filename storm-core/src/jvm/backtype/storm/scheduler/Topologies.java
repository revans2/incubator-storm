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
package backtype.storm.scheduler;

import backtype.storm.scheduler.resource.RAS_Component;
import backtype.storm.scheduler.resource.RAS_TYPES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Topologies {
    Map<String, TopologyDetails> topologies;
    Map<String, String> nameToId;
    Map<String, Map<String, RAS_Component>> _allRAS_Components;

    private static final Logger LOG = LoggerFactory.getLogger(Topologies.class);


    public Topologies(Map<String, TopologyDetails> _topologies) {
        if (_topologies == null) _topologies = new HashMap<String, TopologyDetails>();
        this.topologies = new HashMap<String, TopologyDetails>(_topologies.size());
        this.topologies.putAll(_topologies);
        this.nameToId = new HashMap<String, String>(_topologies.size());

        for (String topologyId : _topologies.keySet()) {
            TopologyDetails topology = _topologies.get(topologyId);
            this.nameToId.put(topology.getName(), topologyId);
        }
    }

    public TopologyDetails getById(String topologyId) {
        return this.topologies.get(topologyId);
    }

    public TopologyDetails getByName(String topologyName) {
        String topologyId = this.nameToId.get(topologyName);

        if (topologyId == null) {
            return null;
        } else {
            return this.getById(topologyId);
        }
    }

    public Collection<TopologyDetails> getTopologies() {
        return this.topologies.values();
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     * @param cluster the cluster object that maintains a copy of the scheduling state
     */
    public void checkAndAddDefaultRes(Cluster cluster) {
        for (TopologyDetails td : topologies.values()) {
            for (ExecutorDetails exec : cluster.getUnassignedExecutors(td)) {
                if (!td.hasExecInTopo(exec)) {
                    if (td.getExecutorToComponent().get(exec)
                            .compareTo("__acker") == 0) {
                        LOG.info(
                                "Scheduling __acker {} with memory requirement as {} - {} and {} - {} and CPU requirement as {}-{}",
                                exec,
                                RAS_TYPES.TYPE_MEMORY_ONHEAP,
                                RAS_TYPES.DEFAULT_ONHEAP_MEMORY_REQUIREMENT,
                                RAS_TYPES.TYPE_MEMORY_OFFHEAP,
                                RAS_TYPES.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT,
                                RAS_TYPES.TYPE_CPU_TOTAL,
                                RAS_TYPES.DEFAULT_CPU_REQUIREMENT);
                    } else {
                        LOG.info(
                                "Executor: {} of Component: {} of topology: {} does not have a set resource requirement!",
                                exec, td.getExecutorToComponent().get(exec), td.getId());
                    }
                    td.addDefaultResforExec(exec);
                } else {
                    LOG.info(
                            "Executor: {} of Component: {} of topology: {} already has a set memory resource requirement!",
                            exec, td.getExecutorToComponent().get(exec), td.getId());
                }
            }
        }
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public Map<String, Map<String, RAS_Component>> getAllRAS_Components() {
        if (_allRAS_Components == null) {
            _allRAS_Components = new HashMap<>();
            for (Map.Entry<String, TopologyDetails> entry : this.topologies.entrySet()) {
                _allRAS_Components.put(entry.getKey(), entry.getValue().getRAS_Components());
            }
        }
        return _allRAS_Components;
    }
}
