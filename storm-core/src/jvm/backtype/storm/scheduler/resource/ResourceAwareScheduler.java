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

package backtype.storm.scheduler.resource;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.Strategies.ResourceAwareStrategy;

public class ResourceAwareScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(ResourceAwareScheduler.class);
    @SuppressWarnings("rawtypes")
    private Map _conf;

    @Override
    public void prepare(Map conf) {
        _conf = conf;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("\n\n\nRerunning ResourceAwareScheduler...");

        topologies.checkAndAddDefaultRes(cluster);

        for (TopologyDetails td : topologies.getTopologies()) {
            String topId = td.getId();
            Map<Node, Collection<ExecutorDetails>> taskToNodesMap;
            if (cluster.needsScheduling(td) && cluster.getUnassignedExecutors(td).size() > 0) {
                LOG.info("/********Scheduling topology {} ************/", topId);
                int totalTasks = td.getExecutors().size();
                int executorsNotRunning = cluster.getUnassignedExecutors(td).size();
                LOG.info(
                        "Total number of executors: {} " +
                                "Total number of Unassigned Executors: {}",
                        totalTasks, executorsNotRunning);
                LOG.info("executors that need scheduling: {}",
                        cluster.getUnassignedExecutors(td));

                ResourceAwareStrategy rs = new ResourceAwareStrategy(td, cluster, topologies);
                taskToNodesMap = rs.schedule(topologies, td, cluster.getUnassignedExecutors(td));
                LOG.debug("taskToNodesMap: {}", taskToNodesMap);

                if (taskToNodesMap != null) {
                    try {
                        for (Map.Entry<Node, Collection<ExecutorDetails>> entry : taskToNodesMap.entrySet()) {
                            entry.getKey().assign(td.getId(), entry.getValue(), cluster);
                            LOG.info("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To Node: "
                                            + entry.getKey().getId() + ", Slots left: "
                                            + entry.getKey().totalSlotsFree(), td.getId(),
                                    entry.getValue());
                        }
                        LOG.info("Topology: {} assigned to {} nodes", td.getId(), taskToNodesMap.keySet().size());
                        cluster.setStatus(td.getId(), td.getId() + " Fully Scheduled");
                    } catch (IllegalStateException ex) {
                        LOG.error(ex.toString());
                        LOG.error("Unsuccessfull in scheduling topology {}", td.getId());
                        cluster.setStatus(td.getId(), "Unsuccessfull in scheduling topology" + td.getId());
                    }
                } else {
                    LOG.error("Unsuccessfull in scheduling topology {}", td.getId());
                    cluster.setStatus(td.getId(), "Unsuccessfull in scheduling topology" + td.getId());
                }
            } else {
                cluster.setStatus(td.getId(), td.getId() + " Fully Scheduled");
            }
        }
    }

    private Map<String, Double> getUserConf() {
        Map<String, Double> ret = new HashMap<String, Double>();
        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                (Double)_conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        ret.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                (Double)_conf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        return ret;
    }

    @Override
    public Map<String, Object> config() {
        return (Map) getUserConf();
    }
}
