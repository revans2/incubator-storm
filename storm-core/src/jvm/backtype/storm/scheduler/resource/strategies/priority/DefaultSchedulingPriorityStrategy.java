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

package backtype.storm.scheduler.resource.strategies.priority;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DefaultSchedulingPriorityStrategy implements ISchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultSchedulingPriorityStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;

    @Override
    public void prepare(SchedulingState schedulingState) {
        this.cluster = schedulingState.cluster;
        this.userMap = schedulingState.userMap;
    }

    @Override
    public List<TopologyDetails> getOrderedTopologies() {
        List<TopologyDetails> allUserTopologies = new ArrayList<>();
        for (User user : userMap.values()) {
            List<TopologyDetails> topologyDetailsList = new ArrayList<>();
            topologyDetailsList.addAll(user.getTopologiesAttempted());
            topologyDetailsList.addAll(user.getTopologiesInvalid());
            topologyDetailsList.addAll(user.getTopologiesPending());
            topologyDetailsList.addAll(user.getTopologiesRunning());
            Collections.sort(topologyDetailsList, new SortByUserGuaranteePriorityAndSubmittionTime(userMap, cluster.getClusterTotalCPUResource(), cluster.getClusterTotalMemoryResource()));
            allUserTopologies.addAll(topologyDetailsList);
        }
        return allUserTopologies;
    }

    class UserByGuarantees implements Comparator<User> {

        private final double clusterTotalCPUResource;
        private final double clusterTotalMemoryResource;

        public UserByGuarantees(double clusterTotalCPUResource, double clusterTotalMemoryResource) {
            this.clusterTotalCPUResource = clusterTotalCPUResource;
            this.clusterTotalMemoryResource = clusterTotalMemoryResource;
        }

        @Override
        public int compare(User user, User otherUser) {

            Double cpuResourceGuaranteed = user.getCPUResourceGuaranteed();
            Double cpuResourceGuaranteedOther = otherUser.getCPUResourceGuaranteed();
            Double memoryResourceGuaranteed = user.getMemoryResourceGuaranteed();
            Double memoryResourceGuaranteedOther = otherUser.getMemoryResourceGuaranteed();

            if ((memoryResourceGuaranteed > 0 && memoryResourceGuaranteedOther == 0)
                || (cpuResourceGuaranteed > 0 && cpuResourceGuaranteedOther == 0)) {
                return -1;
            }

            if ((memoryResourceGuaranteed == 0 && memoryResourceGuaranteedOther > 0)
                || (cpuResourceGuaranteed == 0 && cpuResourceGuaranteedOther > 0)) {
                return 1;
            }

            double memoryRequestedPercentage = getMemoryRequestedPercentage(user);
            double memoryRequestedPercentageOther = getMemoryRequestedPercentage(otherUser);
            if (memoryRequestedPercentage < 0 && memoryRequestedPercentageOther < 0) {
                return Double.compare(memoryRequestedPercentage, memoryRequestedPercentageOther);
            }

            double cpuRequestedPercentage = getCPURequestedPercentage(user);
            double cpuRequestedPercentageOther = getCPURequestedPercentage(otherUser);
            if (cpuRequestedPercentage < 0 && cpuRequestedPercentageOther < 0) {
                return Double.compare(cpuRequestedPercentage, cpuRequestedPercentageOther);
            }

            if (memoryRequestedPercentage < 0 || cpuRequestedPercentage < 0) {
                return 1;
            }

            if (memoryRequestedPercentageOther < 0 || cpuRequestedPercentageOther < 0) {
                return -1;
            }

            double userAvgResourcePercentage = getAvgResourceGuaranteePercentage(user);
            double otherAvgResourcePercentage = getAvgResourceGuaranteePercentage(otherUser);

            if (userAvgResourcePercentage < otherAvgResourcePercentage) {
                return -1;
            } else if (userAvgResourcePercentage > otherAvgResourcePercentage) {
                return 1;
            }

            double userAvgResourceRequestPercentage = getAvgResourceRequestPercentage(user);
            double otherResourceRequestPercentage = getAvgResourceRequestPercentage(otherUser);
            return Double.compare(userAvgResourceRequestPercentage, otherResourceRequestPercentage);
        }

        private double getMemoryRequestedPercentage(User user) {
            return (user.getMemoryResourceGuaranteed() - user.getMemoryResourceRequest()) / clusterTotalMemoryResource;
        }

        private double getCPURequestedPercentage(User user) {
            return (user.getCPUResourceGuaranteed() - user.getCPUResourceRequest()) / clusterTotalCPUResource;
        }

        private double getAvgResourceGuaranteePercentage(User user) {
            double userCPUPercentage = user.getCPUResourceGuaranteed() / clusterTotalCPUResource;
            double userMemoryPercentage = user.getMemoryResourceGuaranteed() / clusterTotalMemoryResource;
            return (userCPUPercentage + userMemoryPercentage) / 2.0;
        }

        private double getAvgResourceRequestPercentage(User user) {
            double userCPUPercentage = user.getCPUResourceRequestUtilization() / clusterTotalCPUResource;
            double userMemoryPercentage = user.getMemoryResourceRequestUtilzation() / clusterTotalMemoryResource;
            return (userCPUPercentage + userMemoryPercentage) / 2.0;
        }
    }

    /**
     * Comparator that sorts topologies by priority and then by submission time
     * First sort by Topology Priority, if there is a tie for topology priority, topology uptime is used to sort
     */
    class SortByUserGuaranteePriorityAndSubmittionTime implements Comparator<TopologyDetails> {
        private final UserByGuarantees userByGuarantees;
        private final Map<String, User> userMap;

        public SortByUserGuaranteePriorityAndSubmittionTime(Map<String, User> userMap, double clusterTotalCPUResource, double clusterTotalMemoryResource) {
            this.userMap = userMap;
            userByGuarantees = new UserByGuarantees(clusterTotalCPUResource, clusterTotalMemoryResource);
        }

        @Override
        public int compare(TopologyDetails topo1, TopologyDetails topo2) {
            User userTopo1 = userMap.get(topo1.getTopologySubmitter());
            User userTopo2 = userMap.get(topo2.getTopologySubmitter());

            int res = userByGuarantees.compare(userTopo1, userTopo2);
            if(res > 0) {
                return 1;
            } else if (res < 0) {
                return -1;
            }
            if (topo1.getTopologyPriority() > topo2.getTopologyPriority()) {
                return 1;
            } else if (topo1.getTopologyPriority() < topo2.getTopologyPriority()) {
                return -1;
            } else {
                if (topo1.getUpTime() > topo2.getUpTime()) {
                    return -1;
                } else if (topo1.getUpTime() < topo2.getUpTime()) {
                    return 1;
                } else {
                    return topo1.getId().compareTo(topo2.getId());
                }
            }
        }
    }


}
