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
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DefaultEvictionStrategy implements IEvictionStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultEvictionStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;
    private RAS_Nodes nodes;
    private List<TopologyDetails> topologyDetailsList;

    @Override
    public void prepare(SchedulingState schedulingState, List<TopologyDetails> topologyDetailsList) {
        this.cluster = schedulingState.cluster;
        this.userMap = schedulingState.userMap;
        this.nodes = schedulingState.nodes;
        this.topologyDetailsList = ImmutableList.copyOf(topologyDetailsList).reverse();;
    }

    @Override
    public boolean makeSpaceForTopo(TopologyDetails td) {
        LOG.debug("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
        User submitter = this.userMap.get(td.getTopologySubmitter());
        if (submitter.getCPUResourceGuaranteed() == null || submitter.getMemoryResourceGuaranteed() == null
            || submitter.getCPUResourceGuaranteed() == 0.0 || submitter.getMemoryResourceGuaranteed() == 0.0) {
            return false;
        }

        // Do not evict any topology for user who is requesting more CPU/Memory than other
        if(submitter.getCPUResourceRequest() > submitter.getCPUResourceGuaranteed()
            || submitter.getMemoryResourceRequest() > submitter.getMemoryResourceGuaranteed()) {
            return false;
        }

        double cpuNeeded = td.getTotalRequestedCpu() / submitter.getCPUResourceGuaranteed();
        double memoryNeeded = (td.getTotalRequestedMemOffHeap() + td.getTotalRequestedMemOnHeap()) / submitter.getMemoryResourceGuaranteed();

        int tdIndex = this.topologyDetailsList.indexOf(td);

        for(int index = 0; index < tdIndex; index++) {
            TopologyDetails topologyEvict = topologyDetailsList.get(index);
            User tUser = this.userMap.get(topologyEvict.getTopologySubmitter());

            //Do not another evict topology for same user.
            if (topologyEvict.getTopologySubmitter().equals(td.getTopologySubmitter()))
                continue;

            if (tUser.getTopologiesRunning().contains(topologyEvict)) {
                // Can not remove for topology for user with abundant guarantees.
                if(tUser.getMemoryResourceGuaranteed() > tUser.getMemoryResourceRequest() &&
                    tUser.getCPUResourceGuaranteed() > tUser.getCPUResourceRequest()) {
                    continue;
                }
                return EvictionCommon.evictTopology(topologyEvict, this.cluster, this.userMap, this.nodes);
            }
        }
        return false;
    }
}
