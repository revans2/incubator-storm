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
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class DefaultEvictionStrategy implements IEvictionStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultEvictionStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;
    private RAS_Nodes nodes;

    @Override
    public void prepare(SchedulingState schedulingState) {
        this.cluster = schedulingState.cluster;
        this.userMap = schedulingState.userMap;
        this.nodes = schedulingState.nodes;
    }

    @Override
    public boolean makeSpaceForTopo(TopologyDetails td) {
        LOG.debug("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
        User submitter = this.userMap.get(td.getTopologySubmitter());
        if (submitter.getCPUResourceGuaranteed() == null || submitter.getMemoryResourceGuaranteed() == null
                || submitter.getCPUResourceGuaranteed() == 0.0 || submitter.getMemoryResourceGuaranteed() == 0.0) {
            return false;
        }

        double cpuNeeded = td.getTotalRequestedCpu() / submitter.getCPUResourceGuaranteed();
        double memoryNeeded = (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap()) / submitter.getMemoryResourceGuaranteed();

        User evictUser = this.findUserWithHighestAverageResourceUtilAboveGuarantee();
        //check if user has enough resource under his or her resource guarantee to schedule topology
        if ((1.0 - submitter.getCPUResourcePoolUtilization()) >= cpuNeeded && (1.0 - submitter.getMemoryResourcePoolUtilization()) >= memoryNeeded) {
            if (evictUser != null) {
                TopologyDetails topologyEvict = evictUser.getRunningTopologyWithLowestPriority();
                LOG.debug("Running Topology {} from user {} is still within user's resource guarantee thus, POTENTIALLY evicting Topology {} from user {} since:" +
                        "\n(1.0 - submitter.getCPUResourcePoolUtilization()) = {} >= cpuNeeded = {}" +
                        "\nand" +
                        "\n(1.0 - submitter.getMemoryResourcePoolUtilization()) = {} >= memoryNeeded = {}"
                        , td, submitter, topologyEvict, evictUser, (1.0 - submitter.getCPUResourcePoolUtilization())
                        , cpuNeeded, (1.0 - submitter.getMemoryResourcePoolUtilization()), memoryNeeded);
                EvictionCommon.evictTopology(topologyEvict, this.cluster, this.userMap, this.nodes);
                return true;
            }
        } else {
            if (evictUser != null) {
                if ((evictUser.getResourcePoolAverageUtilization() - 1.0) > (((cpuNeeded + memoryNeeded) / 2) + (submitter.getResourcePoolAverageUtilization() - 1.0))) {
                    TopologyDetails topologyEvict = evictUser.getRunningTopologyWithLowestPriority();
                    LOG.debug("POTENTIALLY Evicting Topology {} from user {} since:" +
                            "\n((evictUser.getResourcePoolAverageUtilization() - 1.0) = {}" +
                            "\n(cpuNeeded + memoryNeeded) / 2) = {} and (submitter.getResourcePoolAverageUtilization() - 1.0)) = {} Thus," +
                            "\n(evictUser.getResourcePoolAverageUtilization() - 1.0) = {} > (((cpuNeeded + memoryNeeded) / 2) + (submitter.getResourcePoolAverageUtilization() - 1.0)) = {}"
                            , topologyEvict, evictUser, (evictUser.getResourcePoolAverageUtilization() - 1.0), ((cpuNeeded + memoryNeeded) / 2)
                            , (submitter.getResourcePoolAverageUtilization() - 1.0), (evictUser.getResourcePoolAverageUtilization() - 1.0)
                            , (((cpuNeeded + memoryNeeded) / 2) + (submitter.getResourcePoolAverageUtilization() - 1.0)));
                    EvictionCommon.evictTopology(topologyEvict, this.cluster, this.userMap, this.nodes);
                    return true;
                }
            }
        }
        //See if there is a lower priority topology that can be evicted from the current user
        //topologies should already be sorted in order of increasing priority.
        //Thus, topology at the front of the queue has the lowest priority
        for (TopologyDetails topo : submitter.getTopologiesRunning()) {
            //check to if there is a topology with a lower priority we can evict
            if (topo.getTopologyPriority() > td.getTopologyPriority()) {
                LOG.debug("POTENTIALLY Evicting Topology {} from user {} (itself) since topology {} has a lower priority than topology {}"
                        , topo, submitter, topo, td);
                EvictionCommon.evictTopology(topo, this.cluster, this.userMap, this.nodes);
                return true;
            }
        }
        return false;
    }

    private User findUserWithHighestAverageResourceUtilAboveGuarantee() {
        double most = 0.0;
        User mostOverUser = null;
        for (User user : this.userMap.values()) {
            double over = user.getResourcePoolAverageUtilization() - 1.0;
            if ((over > most) && (!user.getTopologiesRunning().isEmpty())) {
                most = over;
                mostOverUser = user;
            }
        }
        return mostOverUser;
    }
}
