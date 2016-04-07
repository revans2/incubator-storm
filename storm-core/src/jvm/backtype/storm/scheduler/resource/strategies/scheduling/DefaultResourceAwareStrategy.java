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

package backtype.storm.scheduler.resource.strategies.scheduling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.resource.RAS_Node;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.SchedulingStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.Component;

public class DefaultResourceAwareStrategy implements IStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceAwareStrategy.class);
    private Cluster _cluster;
    private Topologies _topologies;
    private RAS_Node refNode = null;
    private Map<String, List<String>> _clusterInfo;
    private RAS_Nodes _nodes;

    private final double CPU_WEIGHT = 1.0;
    private final double MEM_WEIGHT = 1.0;
    private final double NETWORK_WEIGHT = 1.0;

    public void prepare (SchedulingState schedulingState) {
        _cluster = schedulingState.cluster;
        _topologies = schedulingState.topologies;
        _nodes = schedulingState.nodes;
        _clusterInfo = schedulingState.cluster.getNetworkTopography();
        LOG.debug(this.getClusterInfo());
    }

    //the returned TreeMap keeps the Components sorted
    private TreeMap<Integer, List<ExecutorDetails>> getPriorityToExecutorDetailsListMap(
            Queue<Component> ordered__Component_list, Collection<ExecutorDetails> unassignedExecutors) {
        TreeMap<Integer, List<ExecutorDetails>> retMap = new TreeMap<>();
        Integer rank = 0;
        for (Component ras_comp : ordered__Component_list) {
            retMap.put(rank, new ArrayList<ExecutorDetails>());
            for(ExecutorDetails exec : ras_comp.execs) {
                if(unassignedExecutors.contains(exec)) {
                    retMap.get(rank).add(exec);
                }
            }
            rank++;
        }
        return retMap;
    }

    public SchedulingResult schedule(TopologyDetails td) {
        if (_nodes.getNodes().size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "No available nodes to schedule tasks on!");
        }
        Collection<ExecutorDetails> unassignedExecutors = _cluster.getUnassignedExecutors(td);
        Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap = new HashMap<>();
        LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<>();
        List<Component> spouts = this.getSpouts(td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return SchedulingResult.failure(SchedulingStatus.FAIL_INVALID_TOPOLOGY, "Cannot find a Spout!");
        }

        Queue<Component> ordered__Component_list = bfs(td, spouts);

        Map<Integer, List<ExecutorDetails>> priorityToExecutorMap = getPriorityToExecutorDetailsListMap(ordered__Component_list, unassignedExecutors);
        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<>(unassignedExecutors);
        Integer longestPriorityListSize = this.getLongestPriorityListSize(priorityToExecutorMap);
        //Pick the first executor with priority one, then the 1st exec with priority 2, so on an so forth. 
        //Once we reach the last priority, we go back to priority 1 and schedule the second task with priority 1.
        for (int i = 0; i < longestPriorityListSize; i++) {
            for (Entry<Integer, List<ExecutorDetails>> entry : priorityToExecutorMap.entrySet()) {
                Iterator<ExecutorDetails> it = entry.getValue().iterator();
                if (it.hasNext()) {
                    ExecutorDetails exec = it.next();
                    LOG.debug("\n\nAttempting to schedule: {} of component {}[ REQ {} ] with rank {}",
                            new Object[] { exec, td.getExecutorToComponent().get(exec),
                    td.getTaskResourceReqList(exec), entry.getKey() });
                    scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
                    it.remove();
                }
            }
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        LOG.debug("/* Scheduling left over task (most likely sys tasks) */");
        // schedule left over system tasks
        for (ExecutorDetails exec : executorsNotScheduled) {
            scheduleExecutor(exec, td, schedulerAssignmentMap, scheduledTasks);
        }

        SchedulingResult result;
        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}",
                    executorsNotScheduled);
            schedulerAssignmentMap = null;
            result = SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES,
                    (td.getExecutors().size() - unassignedExecutors.size()) + "/" + td.getExecutors().size() + " executors scheduled");
        } else {
            LOG.debug("All resources successfully scheduled!");
            result = SchedulingResult.successWithMsg(schedulerAssignmentMap, "Fully Scheduled by DefaultResourceAwareStrategy");
        }
        if (schedulerAssignmentMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }
        return result;
    }

    private void scheduleExecutor(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot,
            Collection<ExecutorDetails>> schedulerAssignmentMap, Collection<ExecutorDetails> scheduledTasks) {
        WorkerSlot targetSlot = this.findWorkerForExec(exec, td, schedulerAssignmentMap);
        if (targetSlot != null) {
            RAS_Node targetNode = this.idToNode(targetSlot.getNodeId());
            if (!schedulerAssignmentMap.containsKey(targetSlot)) {
                schedulerAssignmentMap.put(targetSlot, new LinkedList<ExecutorDetails>());
            }

            schedulerAssignmentMap.get(targetSlot).add(exec);
            targetNode.consumeResourcesforTask(exec, td);
            scheduledTasks.add(exec);
            LOG.debug("TASK {} assigned to Node: {} avail [ mem: {} cpu: {} ] total [ mem: {} cpu: {} ] on slot: {} on Rack: {}", exec,
                    targetNode.getHostname(), targetNode.getAvailableMemoryResources(),
                    targetNode.getAvailableCpuResources(), targetNode.getTotalMemoryResources(),
                    targetNode.getTotalCpuResources(), targetSlot, nodeToRack(targetNode));
        } else {
            LOG.error("Not Enough Resources to schedule Task {}", exec);
        }
    }

    private WorkerSlot findWorkerForExec(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        WorkerSlot ws;
        // first scheduling
        if (this.refNode == null) {
            String bestRack = null;
            // iterate through an ordered list of all racks available to make sure we cannot schedule the first executor in any rack before we "give up"
            // the list is ordered in decreasing order of effective resources. With the rack in the front of the list having the most effective resources.
            for (RackResources rack : sortRacks(td.getId())) {
                ws = this.getBestWorker(exec, td, rack.id, scheduleAssignmentMap);
                if (ws != null) {
                    bestRack = rack.id;
                    LOG.debug("best rack: {}", rack.id);
                    break;
                }
            }
            ws = this.getBestWorker(exec, td, bestRack, scheduleAssignmentMap);
        } else {
            ws = this.getBestWorker(exec, td, scheduleAssignmentMap);
        }
        if (ws != null) {
            this.refNode = this.idToNode(ws.getNodeId());
        }
        LOG.debug("reference node for the resource aware scheduler is: {}", this.refNode);
        return ws;
    }

    private WorkerSlot getBestWorker(ExecutorDetails exec, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        return this.getBestWorker(exec, td, null, scheduleAssignmentMap);
    }

    private WorkerSlot getBestWorker(ExecutorDetails exec, TopologyDetails td, String rackId, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        double taskMem = td.getTotalMemReqTask(exec);
        double taskCPU = td.getTotalCpuReqTask(exec);
        List<RAS_Node> nodes;
        if(rackId != null) {
            nodes = this.getAvailableNodesFromRack(rackId);
            
        } else {
            nodes = this.getAvailableNodes();
        }
        //First sort nodes by distance
        TreeMap<Double, RAS_Node> nodeRankMap = new TreeMap<>();
        for (RAS_Node n : nodes) {
            if(n.getFreeSlots().size()>0) {
                if (n.getAvailableMemoryResources() >= taskMem
                        && n.getAvailableCpuResources() >= taskCPU) {
                    double a = Math.pow(((taskCPU - n.getAvailableCpuResources())/(n.getAvailableCpuResources() + 1))
                            * this.CPU_WEIGHT, 2);
                    double b = Math.pow(((taskMem - n.getAvailableMemoryResources())/(n.getAvailableMemoryResources() + 1))
                            * this.MEM_WEIGHT, 2);
                    double c = 0.0;
                    if(this.refNode != null) {
                        c = Math.pow(this.distToNode(this.refNode, n)
                                * this.NETWORK_WEIGHT, 2);
                    }
                    double distance = Math.sqrt(a + b + c);
                    nodeRankMap.put(distance, n);
                }
            }
        }
        //Then, pick worker from closest node that satisfy constraints
        for(Map.Entry<Double, RAS_Node> entry : nodeRankMap.entrySet()) {
            RAS_Node n = entry.getValue();
            for(WorkerSlot ws : n.getFreeSlots()) {
                if(checkWorkerConstraints(exec, ws, td, scheduleAssignmentMap)) {
                    return ws;
                }
            }
        }
        return null;
    }

    /**
     * class to keep track of resources on a rack
     */
     class RackResources {
        String id;
        double availMem = 0.0;
        double totalMem = 0.0;
        double availCpu = 0.0;
        double totalCpu = 0.0;
        int freeSlots = 0;
        int totalSlots = 0;
        double effectiveResources = 0.0;
        public RackResources(String id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    /**
     *
     * @param topoId
     * @return a sorted list of racks
     * Racks are sorted by two criteria. 1) the number executors of the topology that needs to be scheduled is already on the rack in descending order.
     * The reasoning to sort based on  criterion 1 is so we schedule the rest of a topology on the same rack as the existing executors of the topology.
     * 2) the subordinate/subservient resource availability percentage of a rack in descending order
     * We calculate the resource availability percentage by dividing the resource availability on the rack by the resource availability of the entire cluster
     * By doing this calculation, racks that have exhausted or little of one of the resources mentioned above will be ranked after racks that have more balanced resource availability.
     * So we will be less likely to pick a rack that have a lot of one resource but a low amount of another.
     */
      TreeSet<RackResources> sortRacks(final String topoId) {
        List<RackResources> racks = new LinkedList<RackResources>();
        final Map<String, String> nodeIdToRackId = new HashMap<String, String>();
        double availMemResourcesOverall = 0.0;
        double totalMemResourcesOverall = 0.0;

        double availCpuResourcesOverall = 0.0;
        double totalCpuResourcesOverall = 0.0;

        int freeSlotsOverall = 0;
        int totalSlotsOverall = 0;

        for (Entry<String, List<String>> entry : _clusterInfo.entrySet()) {
            String rackId = entry.getKey();
            List<String> nodeIds = entry.getValue();
            RackResources rack = new RackResources(rackId);
            racks.add(rack);
            for (String nodeId : nodeIds) {
                RAS_Node node = _nodes.getNodeById(this.NodeHostnameToId(nodeId));
                double availMem = node.getAvailableMemoryResources();
                double availCpu = node.getAvailableCpuResources();
                double freeSlots = node.totalSlotsFree();
                double totalMem = node.getTotalMemoryResources();
                double totalCpu = node.getTotalCpuResources();
                double totalSlots = node.totalSlots();

                rack.availMem += availMem;
                rack.totalMem += totalMem;
                rack.availCpu += availCpu;
                rack.totalCpu += totalCpu;
                rack.freeSlots += freeSlots;
                rack.totalSlots += totalSlots;
                nodeIdToRackId.put(nodeId, rack.id);

                availMemResourcesOverall += availMem;
                availCpuResourcesOverall += availCpu;
                freeSlotsOverall += freeSlots;

                totalMemResourcesOverall += totalMem;
                totalCpuResourcesOverall += totalCpu;
                totalSlotsOverall += totalSlots;
            }
        }

        LOG.debug("Cluster Overall Avail [ CPU {} MEM {} Slots {} ] Total [ CPU {} MEM {} Slots {} ]",
                availCpuResourcesOverall, availMemResourcesOverall, freeSlotsOverall, totalCpuResourcesOverall, totalMemResourcesOverall, totalSlotsOverall);
        for (RackResources rack : racks) {
            if (availCpuResourcesOverall <= 0.0 || availMemResourcesOverall <= 0.0 || freeSlotsOverall <= 0.0) {
                rack.effectiveResources = 0.0;
            } else {
                rack.effectiveResources = Math.min(Math.min((rack.availCpu / availCpuResourcesOverall), (rack.availMem / availMemResourcesOverall)), ((double) rack.freeSlots / (double) freeSlotsOverall));
            }
            LOG.debug("rack: {} Avail [ CPU {}({}%) MEM {}({}%) Slots {}({}%) ] Total [ CPU {} MEM {} Slots {} ] effective resources: {}",
                    rack.id, rack.availCpu, rack.availCpu/availCpuResourcesOverall * 100.0, rack.availMem, rack.availMem/availMemResourcesOverall * 100.0,
                    rack.freeSlots, ((double) rack.freeSlots / (double) freeSlotsOverall) * 100.0, rack.totalCpu, rack.totalMem, rack.totalSlots, rack.effectiveResources);
        }

        TreeSet<RackResources> sortedRacks = new TreeSet<RackResources>(new Comparator<RackResources>() {
            @Override
            public int compare(RackResources o1, RackResources o2) {

                int execsScheduledInRack1 = getExecutorsScheduledinRack(topoId, o1.id, nodeIdToRackId).size();
                int execsScheduledInRack2 = getExecutorsScheduledinRack(topoId, o2.id, nodeIdToRackId).size();
                if (execsScheduledInRack1 > execsScheduledInRack2) {
                    return -1;
                } else if (execsScheduledInRack1 < execsScheduledInRack2) {
                    return 1;
                } else {
                    if (o1.effectiveResources > o2.effectiveResources) {
                        return -1;
                    } else if (o1.effectiveResources < o2.effectiveResources) {
                        return 1;
                    }
                    else {
                        return o1.id.compareTo(o2.id);
                    }
                }
            }
        });
        sortedRacks.addAll(racks);
        LOG.info("Sorted rack: {}", sortedRacks);
        return sortedRacks;
    }

    private Collection<ExecutorDetails> getExecutorsScheduledinRack(String topoId, String rackId, Map<String, String> nodeToRack) {
        Collection<ExecutorDetails> execs = new LinkedList<ExecutorDetails>();
        if (_cluster.getAssignmentById(topoId) != null) {
            for (Entry<ExecutorDetails, WorkerSlot> entry : _cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
                String nodeId = entry.getValue().getNodeId();
                String hostname = idToNode(nodeId).getHostname();
                ExecutorDetails exec = entry.getKey();
                if (nodeToRack.get(hostname) != null && nodeToRack.get(hostname).equals(rackId)) {
                    execs.add(exec);
                }
            }
        }
        return execs;
    }

    private Double distToNode(RAS_Node src, RAS_Node dest) {
        if (src.getId().equals(dest.getId())) {
            return 0.0;
        } else if (this.nodeToRack(src).equals(this.nodeToRack(dest))) {
            return 0.5;
        } else {
            return 1.0;
        }
    }

    private String nodeToRack(RAS_Node node) {
        for (Entry<String, List<String>> entry : _clusterInfo
                .entrySet()) {
            if (entry.getValue().contains(node.getHostname())) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any racks", node.getHostname());
        return null;
    }
    
    private List<RAS_Node> getAvailableNodes() {
        LinkedList<RAS_Node> nodes = new LinkedList<>();
        for (String rackId : _clusterInfo.keySet()) {
            nodes.addAll(this.getAvailableNodesFromRack(rackId));
        }
        return nodes;
    }

    private List<RAS_Node> getAvailableNodesFromRack(String rackId) {
        List<RAS_Node> retList = new ArrayList<>();
        for (String node_id : _clusterInfo.get(rackId)) {
            retList.add(_nodes.getNodeById(this
                    .NodeHostnameToId(node_id)));
        }
        return retList;
    }

    private List<WorkerSlot> getAvailableWorkersFromRack(String rackId) {
        List<RAS_Node> nodes = this.getAvailableNodesFromRack(rackId);
        List<WorkerSlot> workers = new LinkedList<>();
        for(RAS_Node node : nodes) {
            workers.addAll(node.getFreeSlots());
        }
        return workers;
    }

    private List<WorkerSlot> getAvailableWorker() {
        List<WorkerSlot> workers = new LinkedList<>();
        for (String rackId : _clusterInfo.keySet()) {
            workers.addAll(this.getAvailableWorkersFromRack(rackId));
        }
        return workers;
    }

    /**
     * Breadth first traversal of the topology DAG
     * @param td
     * @param spouts
     * @return A partial ordering of components
     */
    private Queue<Component> bfs(TopologyDetails td, List<Component> spouts) {
        // Since queue is a interface
        Queue<Component> ordered__Component_list = new LinkedList<Component>();
        HashSet<String> visited = new HashSet<>();

        /* start from each spout that is not visited, each does a breadth-first traverse */
        for (Component spout : spouts) {
            if (!visited.contains(spout.id)) {
                Queue<Component> queue = new LinkedList<>();
                visited.add(spout.id);
                queue.offer(spout);
                while (!queue.isEmpty()) {
                    Component comp = queue.poll();
                    ordered__Component_list.add(comp);
                    List<String> neighbors = new ArrayList<>();
                    neighbors.addAll(comp.children);
                    neighbors.addAll(comp.parents);
                    for (String nbID : neighbors) {
                        if (!visited.contains(nbID)) {
                            Component child = td.getComponents().get(nbID);
                            visited.add(nbID);
                            queue.offer(child);
                        }
                    }
                }
            }
        }
        return ordered__Component_list;
    }

    private List<Component> getSpouts(TopologyDetails td) {
        List<Component> spouts = new ArrayList<>();

        for (Component c : td.getComponents().values()) {
            if (c.type == Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    private Integer getLongestPriorityListSize(Map<Integer, List<ExecutorDetails>> priorityToExecutorMap) {
        Integer mostNum = 0;
        for (List<ExecutorDetails> execs : priorityToExecutorMap.values()) {
            Integer numExecs = execs.size();
            if (mostNum < numExecs) {
                mostNum = numExecs;
            }
        }
        return mostNum;
    }

    /**
     * Get the remaining amount memory that can be assigned to a worker given the set worker max heap size
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return The remaining amount of memory
     */
    private Double getWorkerScheduledMemoryAvailable(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double memScheduleUsed = this.getWorkerScheduledMemoryUse(ws, td, scheduleAssignmentMap);
        return td.getTopologyWorkerMaxHeapSize() - memScheduleUsed;
    }

    /**
     * Get the amount of memory already assigned to a worker
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return the amount of memory
     */
    private Double getWorkerScheduledMemoryUse(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        Double totalMem = 0.0;
        Collection<ExecutorDetails> execs = scheduleAssignmentMap.get(ws);
        if(execs != null) {
            for(ExecutorDetails exec : execs) {
                totalMem += td.getTotalMemReqTask(exec);
            }
        } 
        return totalMem;
    }

    /**
     * Checks whether we can schedule an Executor exec on the worker slot ws
     * Only considers memory currently.  May include CPU in the future
     * @param exec
     * @param ws
     * @param td
     * @param scheduleAssignmentMap
     * @return a boolean: True denoting the exec can be scheduled on ws and false if it cannot
     */
    private boolean checkWorkerConstraints(ExecutorDetails exec, WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> scheduleAssignmentMap) {
        boolean retVal = false;
        if(this.getWorkerScheduledMemoryAvailable(ws, td, scheduleAssignmentMap) >= td.getTotalMemReqTask(exec)) {
            retVal = true;
        }
        return retVal;
    }

    /**
     * Get the amount of resources available and total for each node
     * @return a String with cluster resource info for debug
     */
    private String getClusterInfo() {
        String retVal = "Cluster info:\n";
        for(Entry<String, List<String>> clusterEntry : _clusterInfo.entrySet()) {
            String clusterId = clusterEntry.getKey();
            retVal += "Rack: " + clusterId + "\n";
            for(String nodeHostname : clusterEntry.getValue()) {
                RAS_Node node = this.idToNode(this.NodeHostnameToId(nodeHostname));
                retVal += "-> Node: " + node.getHostname() + " " + node.getId() + "\n";
                retVal += "--> Avail Resources: {Mem " + node.getAvailableMemoryResources() + ", CPU " + node.getAvailableCpuResources() + " Slots: " + node.totalSlotsFree() + "}\n";
                retVal += "--> Total Resources: {Mem " + node.getTotalMemoryResources() + ", CPU " + node.getTotalCpuResources() + " Slots: " + node.totalSlots() + "}\n";
            }
        }
        return retVal;
    }

    /**
     * hostname to Id
     * @param hostname
     * @return the id of a node
     */
    public String NodeHostnameToId(String hostname) {
        for (RAS_Node n : _nodes.getNodes()) {
            if (n.getHostname() == null) {
                continue;
            }
            if (n.getHostname().equals(hostname)) {
                return n.getId();
            }
        }
        LOG.error("Cannot find Node with hostname {}", hostname);
        return null;
    }

    /**
     * Find RAS_Node for specified node id
     * @param id
     * @return a RAS_Node object
     */
    public RAS_Node idToNode(String id) {
        RAS_Node ret = _nodes.getNodeById(id);
        if(ret == null) {
            LOG.error("Cannot find Node with Id: {}", id);
        }
        return ret;
    }
}
