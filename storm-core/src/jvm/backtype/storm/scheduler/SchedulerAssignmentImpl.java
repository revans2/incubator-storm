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
package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.WorkerResources;

public class SchedulerAssignmentImpl implements SchedulerAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerAssignmentImpl.class);
    /**
     * topology-id this assignment is for.
     */
    private final String topologyId;
    /**
     * assignment detail, a mapping from executor to <code>WorkerSlot</code>
     */
    private final Map<ExecutorDetails, WorkerSlot> executorToSlot = new HashMap<>();
    private final Map<WorkerSlot, WorkerResources> resources = new HashMap<>();
    private final Map<String, Double> totalSharedOffHeap = new HashMap<>();
    
    public SchedulerAssignmentImpl(String topologyId, Map<ExecutorDetails, WorkerSlot> executorToSlot,
            Map<WorkerSlot, WorkerResources> resources, Map<String, Double> totalSharedOffHeap) {
        this.topologyId = topologyId;       
        if (executorToSlot != null) {
            if (executorToSlot.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create a scheduling with a null in it " + executorToSlot);
            }
            this.executorToSlot.putAll(executorToSlot);
        }
        if (resources != null) {
            if (resources.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create resources with a null in it " + resources);
            }
            this.resources.putAll(resources);
        }
        if (totalSharedOffHeap != null) {
            if (totalSharedOffHeap.entrySet().stream().anyMatch((entry) -> entry.getKey() == null || entry.getValue() == null)) {
                throw new RuntimeException("Cannot create off heap with a null in it " + totalSharedOffHeap);
            }
            this.totalSharedOffHeap.putAll(totalSharedOffHeap);
        }
    }

    public SchedulerAssignmentImpl(String topologyId) {
        this(topologyId, null, null, null);
    }
    
    public SchedulerAssignmentImpl(SchedulerAssignment assignment) {
        this(assignment.getTopologyId(), assignment.getExecutorToSlot(), 
                assignment.getScheduledResources(), assignment.getTotalSharedOffHeapMemory());
    }

    @Override
    public String toString() {
        return "SchedulerAssignmentImpl: RESOURCES: " + resources + " EXECS: " + executorToSlot;
    }
    
    @Override
    public Set<WorkerSlot> getSlots() {
        return new HashSet<>(executorToSlot.values());
    }    
    
    @Deprecated
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors) {
        assign(slot, executors, null);
    }
    
    /**
     * Assign the slot to executors.
     */
    public void assign(WorkerSlot slot, Collection<ExecutorDetails> executors, WorkerResources slotResources) {
        assert(slot != null);
        for (ExecutorDetails executor : executors) {
            this.executorToSlot.put(executor, slot);
        }
        if (slotResources != null) {
            resources.put(slot, slotResources);
        } else {
            resources.remove(slot);
        }
    }
    
    /**
     * Release the slot occupied by this assignment.
     */
    public void unassignBySlot(WorkerSlot slot) {
        List<ExecutorDetails> executors = new ArrayList<>();
        for (ExecutorDetails executor : executorToSlot.keySet()) {
            WorkerSlot ws = executorToSlot.get(executor);
            if (ws.equals(slot)) {
                executors.add(executor);
            }
        }

        // remove
        for (ExecutorDetails executor : executors) {
            executorToSlot.remove(executor);
        }

        resources.remove(slot);

        String node = slot.getNodeId();
        boolean isFound = false;
        for (WorkerSlot ws: executorToSlot.values()) {
            if (node.equals(ws.getNodeId())) {
                isFound = true;
                break;
            }
        }
        if (!isFound) {
            totalSharedOffHeap.remove(node);
        }
    }

    /**
     * @param slot
     * @return true if slot is occupied by this assignment
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        return this.executorToSlot.containsValue(slot);
    }

    public boolean isExecutorAssigned(ExecutorDetails executor) {
        return this.executorToSlot.containsKey(executor);
    }
    
    public String getTopologyId() {
        return this.topologyId;
    }

    public Map<ExecutorDetails, WorkerSlot> getExecutorToSlot() {
        return this.executorToSlot;
    }

    /**
     * @return the executors covered by this assignments
     */
    public Set<ExecutorDetails> getExecutors() {
        return this.executorToSlot.keySet();
    }

    public Map<WorkerSlot, Collection<ExecutorDetails>> getSlotToExecutors() {
        Map<WorkerSlot, Collection<ExecutorDetails>> ret = new HashMap<WorkerSlot, Collection<ExecutorDetails>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot ws = entry.getValue();
            if (!ret.containsKey(ws)) {
                ret.put(ws, new LinkedList<ExecutorDetails>());
            }
            ret.get(ws).add(exec);
        }
        return ret;
    }

    @Override
    public Map<WorkerSlot, WorkerResources> getScheduledResources() {
        return resources;
    }

    public void setTotalSharedOffHeapMemory(String node, double value) {
        totalSharedOffHeap.put(node, value);
    }
    
    @Override
    public Map<String, Double> getTotalSharedOffHeapMemory() {
        return totalSharedOffHeap;
    }

    /**
     * Update the resources for this assignment (This should go aware when the RAS-MT bridge goes away
     * @param ws
     * @param onHeap
     * @param offHeap
     * @param cpu
     */
    @Deprecated
    public void updateResources(WorkerSlot ws, double onHeap, double offHeap, double cpu) {
        WorkerResources wr = resources.get(ws);
        boolean shouldAdd = (wr == null);
        if (shouldAdd){
            wr = new WorkerResources();
        }
        wr.set_mem_on_heap(onHeap);
        wr.set_mem_off_heap(offHeap);
        wr.set_cpu(cpu);
        wr.set_shared_mem_off_heap(0);
        wr.set_shared_mem_on_heap(0);
        if (shouldAdd){
            resources.put(ws, wr);
        }
    }
    
    public boolean equalsIgnoreResources(Object other) {
        if (!(other instanceof SchedulerAssignmentImpl)) {
            return false;
        }
        SchedulerAssignmentImpl o = (SchedulerAssignmentImpl) other;
        
        return this.topologyId.equals(o.topologyId) &&
                this.executorToSlot.equals(o.executorToSlot);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topologyId == null) ? 0 : topologyId.hashCode());
        result = prime * result + ((executorToSlot == null) ? 0 : executorToSlot.hashCode());
        result = prime * result + ((resources == null) ? 0 : resources.hashCode());
        result = prime * result + ((totalSharedOffHeap == null) ? 0 : totalSharedOffHeap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (!equalsIgnoreResources(other)) {
            return false;
        }
        SchedulerAssignmentImpl o = (SchedulerAssignmentImpl) other;
        //Normalize some things
        Map<WorkerSlot, WorkerResources> selfResources = this.resources;
        if (selfResources == null) selfResources = Collections.emptyMap();
        Map<WorkerSlot, WorkerResources> otherResources = o.resources;
        if (otherResources == null) otherResources = Collections.emptyMap();

        Map<String, Double> selfOffHeap = this.totalSharedOffHeap;
        if (selfOffHeap == null) selfOffHeap = Collections.emptyMap();
        Map<String, Double> otherOffHeap = o.totalSharedOffHeap;
        if (otherOffHeap == null) otherOffHeap = Collections.emptyMap();

        return selfResources.equals(otherResources) &&
                selfOffHeap.equals(otherOffHeap);
    }
}
