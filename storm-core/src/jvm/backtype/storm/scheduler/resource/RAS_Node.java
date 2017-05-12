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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class RAS_Node {
    private static final Logger LOG = LoggerFactory.getLogger(RAS_Node.class);

    //A map consisting of all workers on the node.
    //The key of the map is the worker id and the value is the corresponding workerslot object
    private Map<String, WorkerSlot> _slots = new HashMap<String, WorkerSlot>();

    // A map describing which topologies are using which slots on this node.  The format of the map is the following:
    // {TopologyId -> {WorkerId -> {Executors}}}
    private Map<String, Map<String, Collection<ExecutorDetails>>> _topIdToUsedSlots = new HashMap<String, Map<String, Collection<ExecutorDetails>>>();

    private final double _totalMemory;
    private final double _totalCPU;
    private final String _nodeId;
    private String _hostname;
    private boolean _isAlive;
    private SupervisorDetails _sup;
    private final Cluster _cluster;
    private final Topologies _topologies;
    private final Set<WorkerSlot> _originallyFreeSlots;

    public RAS_Node(String nodeId, SupervisorDetails sup, Cluster cluster, Topologies topologies, Map<String, WorkerSlot> workerIdToWorker, Map<String, Map<String, Collection<ExecutorDetails>>> assignmentMap) {
        //Node ID and supervisor ID are the same.
        _nodeId = nodeId;
        if (sup == null) {
            _isAlive = false;
        } else {
            _isAlive = !cluster.isBlackListed(_nodeId);
        }

        _cluster = cluster;
        _topologies = topologies;

        // initialize slots for this node
        if (workerIdToWorker != null) {
            _slots = workerIdToWorker;
        }

        //initialize assignment map
        if (assignmentMap != null) {
            _topIdToUsedSlots = assignmentMap;
        }
        
        //check if node is alive
        if (_isAlive && sup != null) {
            _hostname = sup.getHost();
            _sup = sup;
        }
        
        _totalMemory = _isAlive ? getTotalMemoryResources() : 0.0;
        _totalCPU = _isAlive ? getTotalCpuResources() : 0.0;
        HashSet<String> freeById = new HashSet<>(_slots.keySet());
        if (assignmentMap != null) {
            for (Map<String, Collection<ExecutorDetails>> assignment: assignmentMap.values()) {
                freeById.removeAll(assignment.keySet());
            }
        }
        _originallyFreeSlots = new HashSet<>();
        for (WorkerSlot slot : _slots.values()) {
            if (freeById.contains(slot.getId())) {
                _originallyFreeSlots.add(slot);
            }
        }
    }

    public String getId() {
        return _nodeId;
    }

    public String getHostname() {
        return _hostname;
    }

    private Collection<WorkerSlot> workerIdsToWorkers(Collection<String> workerIds) {
        Collection<WorkerSlot> ret = new LinkedList<WorkerSlot>();
        for (String workerId : workerIds) {
            ret.add(_slots.get(workerId));
        }
        return ret;
    }

    public Collection<String> getFreeSlotsId() {
        if (!_isAlive) {
            return new HashSet<String>();
        }
        Collection<String> usedSlotsId = getUsedSlotsId();
        Set<String> ret = new HashSet<>();
        ret.addAll(_slots.keySet());
        ret.removeAll(usedSlotsId);
        return ret;
    }

    public Collection<WorkerSlot> getSlotsAvailbleTo(TopologyDetails td) {
        //Try to reuse a slot if possible....
        HashSet<WorkerSlot> ret = new HashSet<>();
        Map<String, Collection<ExecutorDetails>> assigned = _topIdToUsedSlots.get(td.getId());
        if (assigned != null)
        {
            ret.addAll(workerIdsToWorkers(assigned.keySet()));
        }
        ret.addAll(getFreeSlots());
        ret.retainAll(_originallyFreeSlots); //RAS does not let you move things or modify existing assignments
        return ret;
    }
    
    public Collection<WorkerSlot> getFreeSlots() {
        return workerIdsToWorkers(getFreeSlotsId());
    }

    private Collection<String> getUsedSlotsId() {
        Collection<String> ret = new LinkedList<String>();
        for (Map<String, Collection<ExecutorDetails>> entry : _topIdToUsedSlots.values()) {
            ret.addAll(entry.keySet());
        }
        return ret;
    }

    public Collection<WorkerSlot> getUsedSlots() {
        return workerIdsToWorkers(getUsedSlotsId());
    }

    public Collection<WorkerSlot> getUsedSlots(String topId) {
        Collection<WorkerSlot> ret = null;
        if (_topIdToUsedSlots.get(topId) != null) {
            ret = workerIdsToWorkers(_topIdToUsedSlots.get(topId).keySet());
        }
        return ret;
    }

    public boolean isAlive() {
        return _isAlive;
    }

    /**
     * @return a collection of the topology ids currently running on this node
     */
    public Collection<String> getRunningTopologies() {
        return _topIdToUsedSlots.keySet();
    }

    public boolean isTotallyFree() {
        return getUsedSlots().isEmpty();
    }

    public int totalSlotsFree() {
        return getFreeSlots().size();
    }

    public int totalSlotsUsed() {
        return getUsedSlots().size();
    }

    public int totalSlots() {
        return _slots.size();
    }

    public int totalSlotsUsed(String topId) {
        return getUsedSlots(topId).size();
    }

    /**
     * Free all slots on this node.  This will update the Cluster too.
     */
    public void freeAllSlots() {
        if (!_isAlive) {
            LOG.warn("Freeing all slots on a dead node {} ", _nodeId);
        }
        _cluster.freeSlots(_slots.values());
        //clearing assignments
        _topIdToUsedSlots.clear();
    }

    /**
     * frees a single executor
     * @param exec is the executor to free
     * @param topo the topology the executor is a part of
     */
    public void freeSingleExecutor(ExecutorDetails exec, TopologyDetails topo) {
        Map<String, Collection<ExecutorDetails>> usedSlots = _topIdToUsedSlots.get(topo.getId());
        if (usedSlots == null) {
            throw new IllegalArgumentException("Topology " + topo + " is not assigned");
        }
        WorkerSlot ws = null;
        Set<ExecutorDetails> updatedAssignment = new HashSet<>();
        for (Entry<String, Collection<ExecutorDetails>> entry: usedSlots.entrySet()) {
            if (entry.getValue().contains(exec)) {
                ws = _slots.get(entry.getKey());
                updatedAssignment.addAll(entry.getValue());
                updatedAssignment.remove(exec);
                break;
            }
        }
        
        if (ws == null) {
            throw new IllegalArgumentException("Executor " + exec + " is not assinged on this node to " + topo);
        }
        free(ws);
        if (!updatedAssignment.isEmpty()) {
            assign(ws, topo, updatedAssignment);
        }
    }

    /**
     * Frees a single slot in this node
     * @param ws the slot to free
     */
    public void free(WorkerSlot ws) {
        LOG.debug("freeing WorkerSlot {} on node {}", ws, _hostname);
        if (!_slots.containsKey(ws.getId())) {
            throw new IllegalArgumentException("Tried to free a slot " + ws + " that was not" +
                    " part of this node " + _nodeId);
        }

        TopologyDetails topo = findTopologyUsingWorker(ws);
        if (topo == null) {
            throw new IllegalArgumentException("Tried to free a slot " + ws + " that was already free!");
        }

        //free slot
        _cluster.freeSlot(ws);
        //cleanup internal assignments
        _topIdToUsedSlots.get(topo.getId()).remove(ws.getId());
    }

    /**
     * Find a which topology is running on a worker slot
     * @return the topology using the worker slot.  If worker slot is free then return null
     */
    private TopologyDetails findTopologyUsingWorker(WorkerSlot ws) {
        for (Entry<String, Map<String, Collection<ExecutorDetails>>> entry : _topIdToUsedSlots.entrySet()) {
            String topoId = entry.getKey();
            Set<String> workerIds = entry.getValue().keySet();
            for (String workerId : workerIds) {
                if(ws.getId().equals(workerId)) {
                    return _topologies.getById(topoId);
                }
            }
        }
        return null;
    }

    /**
     * Assigns a worker to a node
     * @param target the worker slot to assign the executors
     * @param td the topology the executors are from
     * @param executors executors to assign to the specified worker slot
     */
    public void assign(WorkerSlot target, TopologyDetails td, Collection<ExecutorDetails> executors) {
        if (!_isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
        }
        Collection<WorkerSlot> freeSlots = getFreeSlots();
        if (freeSlots.isEmpty()) {
            throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
        }
        if (executors.size() == 0) {
            LOG.warn("Trying to assign nothing from " + td.getId() + " to " + _nodeId + " (Ignored)");
        }
        if (target == null) {
            target = getFreeSlots().iterator().next();
        }
        if (!freeSlots.contains(target)) {
            throw new IllegalStateException("Trying to assign already used slot " + target.getPort() + " on node " + _nodeId);
        }
        LOG.debug("target slot: {}", target);

        _cluster.assign(target, td.getId(), executors);

        //assigning internally
        if (!_topIdToUsedSlots.containsKey(td.getId())) {
            _topIdToUsedSlots.put(td.getId(), new HashMap<String, Collection<ExecutorDetails>>());
        }

        if (!_topIdToUsedSlots.get(td.getId()).containsKey(target.getId())) {
            _topIdToUsedSlots.get(td.getId()).put(target.getId(), new LinkedList<ExecutorDetails>());
        }
        _topIdToUsedSlots.get(td.getId()).get(target.getId()).addAll(executors);
    }
    
    public void assignSingleExecutor(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td) {
        if (!_isAlive) {
            throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
        }
        Collection<WorkerSlot> freeSlots = getFreeSlots();
        Set<ExecutorDetails> toAssign = new HashSet<>();
        toAssign.add(exec);
        if (!freeSlots.contains(ws)) {
            Map<String, Collection<ExecutorDetails>> usedSlots = _topIdToUsedSlots.get(td.getId());
            if (usedSlots == null) {
                throw new IllegalArgumentException("Slot " + ws + " is not availble to schedue " + exec + " on");
            }
            Collection<ExecutorDetails> alreadyHere = usedSlots.get(ws.getId());
            if (alreadyHere == null) {
                throw new IllegalArgumentException("Slot " + ws + " is not availble to schedue " + exec + " on");
            }
            toAssign.addAll(alreadyHere);
            free(ws);
        }
        assign(ws, td, toAssign);
    }
    
    /**
     * Would scheduling exec in ws fit with the current resource constraints
     * @param ws the slot to possibly put exec in
     * @param exec the executor to possibly place in ws
     * @param td the topology exec is a part of
     * @return true if it would fit else false
     */
    public boolean wouldFit(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td) {
        if (!_nodeId.equals(ws.getNodeId())) {
            throw new IllegalStateException("Slot " + ws + " is not a part of this node " + _nodeId);
        }
        return _isAlive && _cluster.wouldFit(ws, exec, td, td.getTopologyWorkerMaxHeapSize(), getAvailableMemoryResources(), getAvailableCpuResources());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RAS_Node) {
            return _nodeId.equals(((RAS_Node) other)._nodeId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return _nodeId.hashCode();
    }

    @Override
    public String toString() {
        return "{Node: " + ((_sup == null) ? "null (possibly down)" : _sup.getHost())
                + ", Avail [ Mem: " + getAvailableMemoryResources()
                + ", CPU: " + getAvailableCpuResources() + ", Slots: " + this.getFreeSlots()
                + "] Total [ Mem: " + ((_sup == null) ? "N/A" : this.getTotalMemoryResources())
                + ", CPU: " + ((_sup == null) ? "N/A" : this.getTotalCpuResources()) + ", Slots: "
                + this._slots.values() + " ]}";
    }

    public static int countFreeSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlotsFree();
            }
        }
        return total;
    }

    public static int countTotalSlotsAlive(Collection<RAS_Node> nodes) {
        int total = 0;
        for (RAS_Node n : nodes) {
            if (n.isAlive()) {
                total += n.totalSlots();
            }
        }
        return total;
    }

    /**
     * Gets the available memory resources for this node
     * @return the available memory for this node
     */
    public Double getAvailableMemoryResources() {
        double used = _cluster.getScheduledMemoryForNode(_nodeId);
        return _totalMemory - used;
    }

    /**
     * Gets the total memory resources for this node
     * @return the total memory for this node
     */
    public Double getTotalMemoryResources() {
        if (_sup != null) {
            return _sup.getTotalMemory();
        } else {
            return 0.0;
        }
    }

    /**
     * Gets the available cpu resources for this node
     * @return the available cpu for this node
     */
    public double getAvailableCpuResources() {
        return _totalCPU - _cluster.getScheduledCpuForNode(_nodeId);
    }

    /**
     * Gets the total cpu resources for this node
     * @return the total cpu for this node
     */
    public Double getTotalCpuResources() {
        if (_sup != null) {
            return _sup.getTotalCPU();
        } else {
            return 0.0;
        }
    }
}
