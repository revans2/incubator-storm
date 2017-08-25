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

import org.apache.storm.utils.DisallowedStrategyException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        Map<String, User> userMap = getUsers(topologies, cluster);
        SchedulingState schedulingState = new SchedulingState(userMap, cluster, topologies, this.conf);
        //logs everything that is currently scheduled and the location at which they are scheduled
        LOG.debug("Cluster scheduling:\n{}", ResourceUtils.printScheduling(cluster, topologies));
        //logs the resources available/used for every node
        LOG.debug("Nodes:\n{}", schedulingState.nodes);
        //logs the detailed info about each user
        for (User user : getUserMap(schedulingState).values()) {
            LOG.debug(user.getDetailedInfo());
        }

        String strategyClassName = (String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY);
        ISchedulingPriorityStrategy schedulingPrioritystrategy = Utils.newInstance(strategyClassName);
        schedulingPrioritystrategy.prepare(schedulingState);

        Set<TopologyDetails> alreadyScheduled = new HashSet<>();
        List<TopologyDetails> orderedTopologies = Collections.unmodifiableList(schedulingPrioritystrategy.getOrderedTopologies());
        LOG.info("Ordered list of topologies is: {}", orderedTopologies.stream().map(t -> t.getId()).toArray());

        for(TopologyDetails td : orderedTopologies) {
            schedulingState = scheduleTopology(schedulingState, td, orderedTopologies);
            alreadyScheduled.add(td);
            LOG.debug("Nodes after scheduling:\n{}", schedulingState.nodes);
        }

        //update changes to cluster
        updateChanges(cluster, topologies, schedulingState);
        this.schedulingState = schedulingState;
    }

    private void updateChanges(Cluster cluster, Topologies topologies, SchedulingState schedulingState) {
        cluster.setAssignments(schedulingState.cluster.getAssignments(), false);
        cluster.setBlacklistedHosts(schedulingState.cluster.getBlacklistedHosts());
        cluster.setStatusMap(schedulingState.cluster.getStatusMap());
    }

    private void handleSchedulingError(TopologyDetails td, User topologySubmitter, Exception e) {
        LOG.error("failed to create instance of IStrategy: {} with error: {}! Topology {} will not be scheduled.",
                  td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), e.getMessage(), td.getName());
        topologySubmitter.moveTopoFromPendingToInvalid(td);
    }
    
    public SchedulingState scheduleTopology(SchedulingState schedulingState, TopologyDetails td, List<TopologyDetails> orderedTopologies) {
        int schedulingAttemptsSoFar = 0;
        User topologySubmitter = schedulingState.userMap.get(td.getTopologySubmitter());
        if (schedulingState.cluster.getUnassignedExecutors(td).size() <= 0) {
            LOG.warn("Topology {} is already fully scheduled!", td.getName());
            topologySubmitter.moveTopoFromPendingToRunning(td);
            if (schedulingState.cluster.getStatusMap().get(td.getId()) == null || schedulingState.cluster.getStatusMap().get(td.getId()).equals("")) {
                schedulingState.cluster.setStatus(td.getId(), "Fully Scheduled");
            }
            return schedulingState;
        }

        LOG.info("/********Scheduling topology {} from User {}************/", td.getName(), topologySubmitter);
        SchedulingState newSchedulingState = copySchedulingState(schedulingState);
        IStrategy rasStrategy = null;
        try {
            rasStrategy = (IStrategy) Utils.newSchedulerStrategyInstance((String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), conf);
        } catch (DisallowedStrategyException e) {
            handleSchedulingError(td, topologySubmitter, e);
            schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - " + e.getAttemptedClass()
                                                   + " is not an allowed strategy. Please make sure your " + Config.TOPOLOGY_SCHEDULER_STRATEGY
                                                   + " config is one of the allowed strategies: " + e.getAllowedStrategies().toString());
            return schedulingState;
        } catch (RuntimeException e) {
            handleSchedulingError(td, topologySubmitter, e);
            schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - failed to create instance of topology strategy "
                                              + td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY) + ". Please check logs for details");
            return schedulingState;
        }

        int maxSchedulingAttempts = ((Number) this.conf.getOrDefault(Config.RESOURCE_AWARE_SCHEDULER_MAX_TOPOLOGY_SCHEDULING_ATTEMPTS, 5)).intValue();
        LOG.debug("Will attempt to schedule topology {} maximum of {} times using strategy {}",
                  td.getName(), maxSchedulingAttempts, rasStrategy.getClass().getName());

        Result result = scheduleTopologyRecursively(newSchedulingState, rasStrategy, topologySubmitter, td, orderedTopologies, maxSchedulingAttempts, schedulingAttemptsSoFar);
        if(result.success()) {
            schedulingState = result.getSchedulingState();
            topologySubmitter = this.getUser(topologySubmitter.getId(), schedulingState);
            topologySubmitter.moveTopoFromPendingToRunning(td);
        } else {
            schedulingState.cluster.setStatus(td.getId(), result.getSchedulingError());
            if(result.getFailType() == Result.FailType.ATTEMPTED) {
                topologySubmitter = this.getUser(topologySubmitter.getId(), schedulingState);
                topologySubmitter.moveTopoFromPendingToAttempted(td);
            } else if (result.getFailType() == Result.FailType.INVALID) {
                topologySubmitter = this.getUser(topologySubmitter.getId(), schedulingState);
                topologySubmitter.moveTopoFromPendingToInvalid(td);
            }
        }
        return schedulingState;
    }

    private static class Result {
        enum FailType {
            ATTEMPTED,
            INVALID
        };

        private final SchedulingState schedulingState;
        private final String schedulingError;
        private final FailType failType;

        Result(SchedulingState schedulingState) {
            this.schedulingState = schedulingState;
            this.schedulingError = null;
            this.failType = null;
        }

        Result(String error, FailType failType) {
            this.schedulingState = null;
            this.schedulingError = error;
            this.failType = failType;
        }

        boolean success() {
            return schedulingState != null;
        }

        public SchedulingState getSchedulingState() {
            return schedulingState;
        }

        public String getSchedulingError() {
            return schedulingError;
        }

        public FailType getFailType() {
            return failType;
        }
    }

    /**
     * scheduleTopologyRecursively makes up to maxSchedulingAttempts - schedulingAttemptsSoFar attempts to schedule the topology.
     * The only condition that will cause it to recur is when eviction is used to make room on the cluster.
     */
    private Result scheduleTopologyRecursively(SchedulingState schedulingState, IStrategy rasStrategy, User topologySubmitter, TopologyDetails td, List<TopologyDetails> orderedTopologies, int maxSchedulingAttempts, Integer schedulingAttemptsSoFar) {
        schedulingAttemptsSoFar++;
        LOG.debug("Attempt {} of {} to schedule topology {}", schedulingAttemptsSoFar, maxSchedulingAttempts, td.getName());
        SchedulingResult result = null;
        String schedulingError = "Unable to schedule the topology due to an unknown error.";
        Result.FailType failType = Result.FailType.ATTEMPTED;
        try {
            // Need to re prepare scheduling strategy with cluster and topologies in case scheduling state was restored
            // Pass in a copy of scheduling state since the scheduling strategy should not be able to be able to make modifications to
            // the state of cluster directly
            rasStrategy.prepare(new SchedulingState(schedulingState));
            result = rasStrategy.schedule(td);
            if (result == null || !result.isValid()) {
                // Invalid result
                LOG.warn("Scheduling results returned from topology {} is not vaild! Topology with be ignored.", td.getName());
                schedulingError = "Scheduling strategy returned an invalid result. Topology will not be scheduled.";
                failType = Result.FailType.INVALID;
            } else if (result.isSuccess()) {
                // Success
                if (mkAssignment(schedulingState, td, result.getSchedulingResultMap())) {
                    topologySubmitter.moveTopoFromPendingToRunning(td);
                    schedulingState.cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                    return new Result(schedulingState);
                } else {
                    schedulingError = "Unsuccessful in scheduling - Unable to assign executors to nodes. Please check logs for details";
                }
            } else if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                // Failure with possibility of eviction.
                if(schedulingAttemptsSoFar >= maxSchedulingAttempts) {
                    schedulingError = "Attempted to schedule " + Integer.toString(schedulingAttemptsSoFar) + " times and failed due to lack of cluster resources.";
                } else {
                    boolean madeSpace = false;
                    IEvictionStrategy evictionStrategy = null;
                    String strategyName =  (String)this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY);
                    try {
                        evictionStrategy = (IEvictionStrategy) Utils.newInstance(strategyName);
                        evictionStrategy.prepare(schedulingState, orderedTopologies);
                        madeSpace = evictionStrategy.makeSpaceForTopo(td);
                        if (!madeSpace) {
                            LOG.info("Could not make space for topo {} will move to attempted", td);
                            schedulingError = "Not enough resources to schedule - " + result.getErrorMessage();

                        } else {
                            return scheduleTopologyRecursively(schedulingState, rasStrategy, topologySubmitter, td, orderedTopologies, maxSchedulingAttempts, schedulingAttemptsSoFar);
                        }
                    } catch (Exception ex) {
                        LOG.error(String.format("Exception thrown when running eviction strategy %s to schedule topology %s. No evictions will be done! Error: %s",
                                strategyName, td.getName(), ex.getClass().getName()), ex);
                    }
                }
            }
            else if (result.getStatus() == SchedulingStatus.FAIL_INVALID_TOPOLOGY) {
                // Failure with bad configuration or some such.
                schedulingError = "The scheduling strategy determined that the topology is invalid.";
                failType = Result.FailType.INVALID;
            }
        } catch (Exception ex) {
            LOG.error(String.format("Exception thrown when running strategy %s to schedule topology %s. Topology will not be scheduled!",
                                    rasStrategy.getClass().getName(), td.getName()), ex);
            schedulingError = String.format("Unsuccessful in scheduling - Exception thrown when running strategy %s . Please check logs for details.", rasStrategy.getClass().getName());
            failType = Result.FailType.INVALID;
        }
        return new Result(schedulingError, failType);
    }

    private boolean mkAssignment(SchedulingState schedulingState, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap) {
        if (schedulerAssignmentMap != null) {
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                WorkerSlot targetSlot = workerToTasksEntry.getKey();
                Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                RAS_Node targetNode = schedulingState.nodes.getNodeById(targetSlot.getNodeId());
                
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

    public User getUser(String user, SchedulingState schedulingState) {
        return schedulingState.userMap.get(user);
    }

    public Map<String, User> getUserMap(SchedulingState schedulingState) {
        return schedulingState.userMap;
    }

    public SchedulingState getLastSchedulingState() {
        return this.schedulingState;
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

    private SchedulingState copySchedulingState(SchedulingState schedulingState) {
        LOG.debug("/*********Checkpoint scheduling state************/");
        for (User user : schedulingState.userMap.values()) {
            LOG.debug(user.getDetailedInfo());
        }
        LOG.debug(ResourceUtils.printScheduling(schedulingState.cluster, schedulingState.topologies));
        LOG.debug("nodes:\n{}", schedulingState.nodes);
        LOG.debug("/*********End************/");
        return new SchedulingState(schedulingState);
    }
}
