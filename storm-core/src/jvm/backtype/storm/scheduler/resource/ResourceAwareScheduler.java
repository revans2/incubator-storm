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

package backtype.storm.scheduler.resource;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.strategies.eviction.IEvictionStrategy;
import backtype.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import backtype.storm.scheduler.resource.strategies.scheduling.IStrategy;
import backtype.storm.scheduler.utils.IConfigLoader;
import backtype.storm.scheduler.utils.SchedulerUtils;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceAwareScheduler implements IScheduler {

    // Object that holds the current scheduling state
    private SchedulingState schedulingState;

    @SuppressWarnings("rawtypes")
    private Map conf;
    private IConfigLoader configLoader;

    private static final Logger LOG = LoggerFactory
            .getLogger(ResourceAwareScheduler.class);

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
        this.configLoader = SchedulerUtils.getConfigLoader(conf, Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS_LOADER,
            Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS_LOADER_PARAMS);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("\n\n\nRerunning ResourceAwareScheduler...");
        //initialize data structures
        initialize(topologies, cluster);
        //logs everything that is currently scheduled and the location at which they are scheduled
        LOG.debug("Cluster scheduling:\n{}", ResourceUtils.printScheduling(cluster, topologies));
        //logs the resources available/used for every node
        LOG.debug("Nodes:\n{}", this.schedulingState.nodes);
        //logs the detailed info about each user
        for (User user : getUserMap().values()) {
            LOG.debug(user.getDetailedInfo());
        }

        ISchedulingPriorityStrategy schedulingPrioritystrategy = null;

        if (schedulingPrioritystrategy == null) {
            String strategyClassName = (String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY);
            schedulingPrioritystrategy = Utils.newInstance(strategyClassName);
        }
        schedulingPrioritystrategy.prepare(schedulingState);

        List<TopologyDetails> orderedTopologies = Collections.unmodifiableList(schedulingPrioritystrategy.getOrderedTopologies());
        LOG.info("Ordered list of topologies is: {}", orderedTopologies.stream().map(t -> t.getId()).toArray());

        for(TopologyDetails td : orderedTopologies) {
            User submitter = getUser(td.getTopologySubmitter());
            if(submitter.getTopologiesRunning().contains(td) ||
                submitter.getTopologiesInvalid().contains(td) ||
                submitter.getTopologiesAttempted().contains(td)) {
                continue;
            }
            scheduleTopology(td, orderedTopologies);
            LOG.debug("Nodes after scheduling:\n{}", this.schedulingState.nodes);
        }

        //update changes to cluster
        updateChanges(cluster, topologies);
    }

    private void updateChanges(Cluster cluster, Topologies topologies) {
        cluster.setAssignments(schedulingState.cluster.getAssignments(), false);
        cluster.setBlacklistedHosts(schedulingState.cluster.getBlacklistedHosts());
        cluster.setStatusMap(schedulingState.cluster.getStatusMap());
    }

    public void scheduleTopology(TopologyDetails td, List<TopologyDetails> orderedTopologies) {
        User topologySubmitter = this.schedulingState.userMap.get(td.getTopologySubmitter());
        if (this.schedulingState.cluster.getUnassignedExecutors(td).size() > 0) {
            LOG.info("/********Scheduling topology {} from User {}************/", td.getName(), topologySubmitter);

            SchedulingState schedulingState = checkpointSchedulingState();
            IStrategy rasStrategy = null;
            try {
                rasStrategy = (IStrategy) Utils.newInstance((String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY));
            } catch (RuntimeException e) {
                LOG.error("failed to create instance of IStrategy: {} with error: {}! Topology {} will not be scheduled.",
                        td.getName(), td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), e.getMessage());
                topologySubmitter = cleanup(schedulingState, td);
                topologySubmitter.moveTopoFromPendingToInvalid(td);
                this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - failed to create instance of topology strategy "
                        + td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY) + ". Please check logs for details");
                return;
            }
            IEvictionStrategy evictionStrategy = null;

            int maxSchedulingAttempts = ((Number) this.conf.getOrDefault(Config.RESOURCE_AWARE_SCHEDULER_MAX_TOPOLOGY_SCHEDULING_ATTEMPTS, 5)).intValue();

            LOG.debug("Will attempt to schedule topology {} maximum of {} times using strategy {}",
                    td.getName(), maxSchedulingAttempts, rasStrategy.getClass().getName());

            for(int schedulingAttemptsSoFar = 0; schedulingAttemptsSoFar < maxSchedulingAttempts; schedulingAttemptsSoFar++)
            {
                SchedulingResult result = null;
                try {
                    // Need to re prepare scheduling strategy with cluster and topologies in case scheduling state was restored
                    // Pass in a copy of scheduling state since the scheduling strategy should not be able to be able to make modifications to
                    // the state of cluster directly
                    rasStrategy.prepare(new SchedulingState(this.schedulingState));
                    result = rasStrategy.schedule(td);
                } catch (Exception ex) {
                    LOG.error(String.format("Exception thrown when running strategy %s to schedule topology %s. Topology will not be scheduled!"
                            , rasStrategy.getClass().getName(), td.getName()), ex);
                    topologySubmitter = cleanup(schedulingState, td);
                    topologySubmitter.moveTopoFromPendingToInvalid(td);
                    this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - Exception thrown when running strategy {}"
                            + rasStrategy.getClass().getName() + ". Please check logs for details");
                }
                LOG.debug("scheduling result: {}", result);
                if (result != null && result.isValid()) {
                    if (result.isSuccess()) {
                        try {
                            if (mkAssignment(td, result.getSchedulingResultMap())) {
                                topologySubmitter.moveTopoFromPendingToRunning(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                            } else {
                                topologySubmitter = this.cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - Unable to assign executors to nodes. Please check logs for details");
                            }
                        } catch (Exception ex) {
                            LOG.error("Unsuccessful in scheduling - Exception thrown when attempting to assign executors to nodes.", ex);
                            topologySubmitter = cleanup(schedulingState, td);
                            topologySubmitter.moveTopoFromPendingToAttempted(td);
                            this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - IllegalStateException thrown when attempting to assign executors to nodes. Please check log for details.");
                        }
                        break;
                    } else {
                        if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                            if (evictionStrategy == null) {
                                try {
                                    evictionStrategy = (IEvictionStrategy) Utils.newInstance((String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY));
                                } catch (RuntimeException e) {
                                    LOG.error("failed to create instance of eviction strategy: {} with error: {}! No topology eviction will be done.",
                                            this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY), e.getMessage());
                                    topologySubmitter.moveTopoFromPendingToAttempted(td);
                                    break;
                                }
                            }
                            boolean madeSpace = false;
                            try {
                                //need to re prepare since scheduling state might have been restored
                                evictionStrategy.prepare(this.schedulingState, orderedTopologies);
                                madeSpace = evictionStrategy.makeSpaceForTopo(td);
                            } catch (Exception ex) {
                                LOG.error(String.format("Exception thrown when running eviction strategy %s to schedule topology %s. No evictions will be done! Error: %s"
                                        , evictionStrategy.getClass().getName(), td.getName(), ex.getClass().getName()), ex);
                                topologySubmitter = cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                break;
                            }
                            if (!madeSpace) {
                                LOG.debug("Could not make space for topo {} will move to attempted", td);
                                topologySubmitter = cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Not enough resources to schedule - " + result.getErrorMessage());
                                break;
                            }
                            if (schedulingAttemptsSoFar < maxSchedulingAttempts) {
                                LOG.debug("Attempt {} of {} to schedule topology {}", schedulingAttemptsSoFar, maxSchedulingAttempts, td.getName());
                                continue;
                            } else {
                                LOG.debug("Attempt {} of {} to schedule topology {}, moving to attempted", schedulingAttemptsSoFar, maxSchedulingAttempts, td.getName());
                                cleanUpAttempted(schedulingState, td);
                                break;
                            }
                        } else if (result.getStatus() == SchedulingStatus.FAIL_INVALID_TOPOLOGY) {
                            cleanUpInvalid(schedulingState, td);
                            break;
                        } else {
                            cleanUpAttempted(schedulingState, td);
                            break;
                        }
                    }
                } else {
                    LOG.warn("Scheduling results returned from topology {} is not vaild! Topology with be ignored.", td.getName());
                    cleanUpInvalid(schedulingState, td);
                    break;
                }
            }
        } else {
            LOG.warn("Topology {} is already fully scheduled!", td.getName());
            topologySubmitter.moveTopoFromPendingToRunning(td);
            if (this.schedulingState.cluster.getStatusMap().get(td.getId()) == null || this.schedulingState.cluster.getStatusMap().get(td.getId()).equals("")) {
                this.schedulingState.cluster.setStatus(td.getId(), "Fully Scheduled");
            }
        }
    }

    private void cleanUpInvalid(SchedulingState schedulingState, TopologyDetails td) {
        User topologySubmitter = cleanup(schedulingState, td);
        topologySubmitter.moveTopoFromPendingToInvalid(td, this.schedulingState.cluster);
    }

    private void cleanUpAttempted(SchedulingState schedulingState, TopologyDetails td) {
        User topologySubmitter = cleanup(schedulingState, td);
        topologySubmitter.moveTopoFromPendingToAttempted(td, this.schedulingState.cluster);
    }

    private User cleanup(SchedulingState schedulingState, TopologyDetails td) {
        restoreCheckpointSchedulingState(schedulingState);
        //since state is restored need the update User topologySubmitter to the new User object in userMap
        return this.schedulingState.userMap.get(td.getTopologySubmitter());
    }

    private boolean mkAssignment(TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap) {
        if (schedulerAssignmentMap != null) {
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                WorkerSlot targetSlot = workerToTasksEntry.getKey();
                Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                RAS_Node targetNode = this.schedulingState.nodes.getNodeById(targetSlot.getNodeId());
                
                targetNode.assign(targetSlot, td, execsNeedScheduling);

                LOG.info("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To {} with {} MB and {} % CPU left",
                        td.getName(), execsNeedScheduling, targetSlot, targetNode.getAvailableMemoryResources(),
                        targetNode.getAvailableCpuResources());
            }
            return true;
        } else {
            LOG.warn("schedulerAssignmentMap for topo {} is null. This shouldn't happen!", td.getName());
            return false;
        }
    }

    @Override
    public Map<String, Object> config() {
        return (Map) getUserResourcePools();
    }

    public User getUser(String user) {
        return this.schedulingState.userMap.get(user);
    }

    public Map<String, User> getUserMap() {
        return this.schedulingState.userMap;
    }

    /**
     * Initialize scheduling and running queues
     *
     * @param topologies
     * @param cluster
     */
    private Map<String, User> getUsers(Topologies topologies, Cluster cluster) {
        Map<String, User> userMap = new HashMap<String, User>();
        Map<String, Map<String, Double>> userResourcePools = getUserResourcePools();
        LOG.debug("userResourcePools: {}", userResourcePools);

        for (TopologyDetails td : topologies.getTopologies()) {

            String topologySubmitter = td.getTopologySubmitter();
            //additional safety check to make sure that topologySubmitter is going to be a valid value
            if (topologySubmitter == null || topologySubmitter.equals("")) {
                LOG.error("Cannot determine user for topology {}.  Will skip scheduling this topology", td.getName());
                continue;
            }
            if (!userMap.containsKey(topologySubmitter)) {
                userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
            if (cluster.getUnassignedExecutors(td).size() > 0) {
                LOG.debug("adding td: {} to pending queue", td.getName());
                userMap.get(topologySubmitter).addTopologyToPendingQueue(td);
            } else {
                LOG.debug("adding td: {} to running queue with existing status: {}", td.getName(), cluster.getStatusMap().get(td.getId()));
                userMap.get(topologySubmitter).addTopologyToRunningQueue(td);
                if (cluster.getStatusMap().get(td.getId()) == null || cluster.getStatusMap().get(td.getId()).equals("")) {
                    cluster.setStatus(td.getId(), "Fully Scheduled");
                }
            }
        }
        return userMap;
    }

    private void initialize(Topologies topologies, Cluster cluster) {
        Map<String, User> userMap = getUsers(topologies, cluster);
        this.schedulingState = new SchedulingState(userMap, cluster, topologies, this.conf);
    }

    private Map readFromLoader() {
        // If loader plugin is not configured, then leave and fall back
        if (this.configLoader == null) {
            return null;
        }

        return configLoader.load();
    }

    private Map<String, Map<String, Double>> convertToDouble(Map<String, Map<String, Number>> raw) {

        Map<String, Map<String, Double>> ret = new HashMap<String, Map<String, Double>>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : raw.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        return ret;
    }

    /**
     * Get resource guarantee configs
     *
     * @return a map that contains resource guarantees of every user of the following format
     * {userid->{resourceType->amountGuaranteed}}
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {

        Object raw = readFromLoader();
        if (raw != null) {
            return convertToDouble((Map<String, Map<String, Number>>) raw);
        }

        raw = this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS); 

        Map<String, Map<String, Double>> ret = convertToDouble((Map<String, Map<String, Number>>) raw);

        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        Map<String, Map<String, Number>> tmp = (Map<String, Map<String, Number>>) fromFile.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (tmp != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : tmp.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }
        return ret;
    }

    public SchedulingState checkpointSchedulingState() {
        LOG.debug("/*********Checkpoint scheduling state************/");
        for (User user : this.schedulingState.userMap.values()) {
            LOG.debug(user.getDetailedInfo());
        }
        LOG.debug(ResourceUtils.printScheduling(this.schedulingState.cluster, this.schedulingState.topologies));
        LOG.debug("nodes:\n{}", this.schedulingState.nodes);
        LOG.debug("/*********End************/");
        return new SchedulingState(this.schedulingState);
    }

    private void restoreCheckpointSchedulingState(SchedulingState schedulingState) {
        LOG.debug("/*********restoring scheduling state************/");
        //reseting cluster
        this.schedulingState = schedulingState;
        for (User user : this.schedulingState.userMap.values()) {
            LOG.debug(user.getDetailedInfo());
        }
        LOG.debug(ResourceUtils.printScheduling(this.schedulingState.cluster, this.schedulingState.topologies));
        LOG.debug("nodes:\n{}", this.schedulingState.nodes);
        LOG.debug("/*********End************/");
    }
}
