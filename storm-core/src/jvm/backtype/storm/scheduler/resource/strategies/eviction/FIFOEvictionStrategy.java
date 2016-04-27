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
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

/**
 * This eviction strategy will evict topologies in FIFO order based on uptime.  Thus, in a situation when there is not enough resources to schedule
 * an incoming topology, the oldest scheduled existing topology will be evicted.  We will only evict topologies from users that are over
 * their resource guarantee
 */
public class FIFOEvictionStrategy implements IEvictionStrategy{
    private static final Logger LOG = LoggerFactory
            .getLogger(FIFOEvictionStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;
    private RAS_Nodes nodes;
    private Topologies topologies;

    @Override
    public void prepare(SchedulingState schedulingState) {
        this.cluster = schedulingState.cluster;
        this.userMap = schedulingState.userMap;
        this.nodes = schedulingState.nodes;
        this.topologies = schedulingState.topologies;
    }

    @Override
    public boolean makeSpaceForTopo(TopologyDetails td) {
        LOG.debug("attempting to make space for topo {} from user {}", td.getName(), td.getTopologySubmitter());
        User submitter = this.userMap.get(td.getTopologySubmitter());
        TreeSet<TopologyDetails> topos = getTopoOrderedByUptime();
        if (topos.size() > 0 ) {
            TopologyDetails topoToEvict = topos.first();
            if (topoToEvict.getUpTime() > td.getUpTime()) {
                EvictionCommon.evictTopology(topos.first(), this.cluster, this.userMap, this.nodes);
                return true;
            }
        }
        return false;
    }

    TreeSet<TopologyDetails> getTopoOrderedByUptime() {
        TreeSet<TopologyDetails> orderedTopologies = new TreeSet<TopologyDetails>(new Comparator<TopologyDetails>() {
            @Override
            public int compare(TopologyDetails o1, TopologyDetails o2) {
                if (o1.getUpTime() > o2.getUpTime()) {
                    return -1;
                } else if (o1.getUpTime() < o2.getUpTime()) {
                    return 1;
                } else {
                    return o1.getId().compareTo(o2.getId());
                }
            }
        });
        //get topologies from users that have exceeded their resource guarantee
        for (User user : this.userMap.values()) {
            if (user.getResourcePoolAverageUtilization() > 1.0) {
                orderedTopologies.addAll(user.getTopologiesRunning());
            }
        }
        return orderedTopologies;
    }
}
