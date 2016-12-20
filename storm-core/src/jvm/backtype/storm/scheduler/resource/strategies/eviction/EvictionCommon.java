/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler.resource.strategies.eviction;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class EvictionCommon {
    private static final Logger LOG = LoggerFactory
            .getLogger(EvictionCommon.class);

    /**
     * Evicts a topology
     * @param topologyEvict the topology to evict
     * @param cluster the cluster on which to operate
     * @param userMap a map of user names to User instances
     * @param nodes nodes on which slots may be freed.
     * @return true if eviction was successful, false otherwise.
     */
    public static boolean evictTopology(TopologyDetails topologyEvict, Cluster cluster, Map<String, User> userMap, RAS_Nodes nodes) {
        Collection<WorkerSlot> workersToEvict = cluster.getUsedSlotsByTopologyId(topologyEvict.getId());
        User submitter = userMap.get(topologyEvict.getTopologySubmitter());

        LOG.info("Evicting Topology {} with workers: {} from user {}", topologyEvict.getName(), workersToEvict, topologyEvict.getTopologySubmitter());
        if (submitter.moveTopoFromRunningToPending(topologyEvict, cluster)) {
            nodes.freeSlots(workersToEvict);
            return true;
        }
        return false;
    }
}
