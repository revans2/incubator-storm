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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import backtype.storm.Config;
import backtype.storm.generated.SharedMemory;
import backtype.storm.generated.WorkerResources;
import backtype.storm.networktopography.DNSToSwitchMapping;
import backtype.storm.utils.Utils;

public class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    public static class SupervisorResources {
        private final double totalMem;
        private final double totalCpu;
        private final double usedMem;
        private final double usedCpu;
        
        public SupervisorResources(double totalMem, double totalCpu, double usedMem, double usedCpu) {
            this.totalMem = totalMem;
            this.totalCpu = totalCpu;
            this.usedMem = usedMem;
            this.usedCpu = usedCpu;
        }

        public double getUsedMem() {
            return usedMem;
        }

        public double getUsedCpu() {
            return usedCpu;
        }

        public double getTotalMem() {
            return totalMem;
        }

        public double getTotalCpu() {
            return totalCpu;
        }

        private SupervisorResources add(WorkerResources wr) {
            return new SupervisorResources(totalMem,
                    totalCpu,
                    usedMem + wr.get_mem_off_heap() + wr.get_mem_on_heap(),
                    usedCpu + wr.get_cpu());
        }

        public SupervisorResources addMem(Double value) {
            return new SupervisorResources(totalMem,
                    totalCpu,
                    usedMem + value,
                    usedCpu);
        }
    }
    
    /**
     * key: supervisor id, value: supervisor details
     */
    private final Map<String, SupervisorDetails> supervisors = new HashMap<>();

    /**
     * key: rack, value: nodes in that rack
     */
    private final Map<String, List<String>> networkTopography = new HashMap<>();

    /**
     * key: topologyId, value: topology's current assignments.
     */
    private final Map<String, SchedulerAssignmentImpl> assignments = new HashMap<>();
    /**
     * key topologyId, Value: scheduler's status.
     */
    private final Map<String, String> status = new HashMap<>();

    private final Topologies topologies;

    /**
     * A map from hostname to supervisor ids.
     */
    private final Map<String, List<String>> hostToId = new HashMap<>();

    private final Map<String, Object> conf;

    private final Set<String> blackListedHosts = new HashSet<String>();
    private final INimbus inimbus;

    public Cluster(INimbus nimbus, Map<String, SupervisorDetails> supervisors, 
            Map<String, SchedulerAssignmentImpl> assignments, Topologies topologies, 
            Map<String, Object> conf) {
        this(nimbus, supervisors, assignments, topologies, conf, null, null, null);
    }
    
    /**
     * Copy constructor
     */
    public Cluster(Cluster src) {
        this(src.inimbus, src.supervisors, src.assignments, src.topologies, new HashMap<>(src.conf), src.status, src.blackListedHosts, src.networkTopography);
    }
    
    private Cluster(INimbus nimbus, Map<String, SupervisorDetails> supervisors,
            Map<String, SchedulerAssignmentImpl> assignments, Topologies topologies, Map<String, Object> conf,
            Map<String, String> status, Set<String> blackListedHosts, Map<String, List<String>> networkTopography) {
        this.inimbus = nimbus;
        this.supervisors.putAll(supervisors);

        for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
            String nodeId = entry.getKey();
            SupervisorDetails supervisor = entry.getValue();
            String host = supervisor.getHost();
            List<String> ids = hostToId.get(host);
            if (ids == null) {
                ids = new ArrayList<>();
                hostToId.put(host, ids);
            }
            ids.add(nodeId);
        }
        this.conf = conf;
        this.topologies = topologies;

        ArrayList<String> supervisorHostNames = new ArrayList<String>();
        for (SupervisorDetails s : supervisors.values()) {
            supervisorHostNames.add(s.getHost());
        }

        if (networkTopography == null || networkTopography.isEmpty()) {
            //Initialize the network topography
            String clazz = (String) conf.get(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN);
            if (clazz != null && !clazz.isEmpty()) {
                DNSToSwitchMapping topographyMapper = (DNSToSwitchMapping) Utils.newInstance(clazz);

                Map<String, String> resolvedSuperVisors = topographyMapper.resolve(supervisorHostNames);
                for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
                    String hostName = entry.getKey();
                    String rack = entry.getValue();
                    List<String> nodesForRack = this.networkTopography.get(rack);
                    if (nodesForRack == null) {
                        nodesForRack = new ArrayList<String>();
                        this.networkTopography.put(rack, nodesForRack);
                    }
                    nodesForRack.add(hostName);
                }
            }
        } else {
            this.networkTopography.putAll(networkTopography);
        }
        
        if (status != null) {
            this.status.putAll(status);
        }
        
        if (blackListedHosts != null) {
            this.blackListedHosts.addAll(blackListedHosts);
        }

        setAssignments(assignments, true);
    }

    public void setBlacklistedHosts(Set<String> hosts) {
        blackListedHosts.clear();
        blackListedHosts.addAll(hosts);
    }

    public Set<String> getBlacklistedHosts() {
        return blackListedHosts;
    }

    public void blacklistHost(String host) {
        blackListedHosts.add(host);
    }

    public boolean isBlackListed(String supervisorId) {
        return blackListedHosts.contains(getHost(supervisorId));
    }

    public boolean isBlacklistedHost(String host) {
        return blackListedHosts.contains(host);
    }

    public String getHost(String supervisorId) {
        return inimbus.getHostName(supervisors, supervisorId);
    }

    /**
     * @return all the topologies which needs scheduling.
     */
    public List<TopologyDetails> needsSchedulingTopologies(Topologies topologies) {
        List<TopologyDetails> ret = new ArrayList<TopologyDetails>();
        for (TopologyDetails topology : topologies.getTopologies()) {
            if (needsScheduling(topology)) {
                ret.add(topology);
            }
        }

        return ret;
    }

    /**
     * Does the topology need scheduling?
     *
     * A topology needs scheduling if one of the following conditions holds:
     * <ul>
     *   <li>Although the topology is assigned slots, but is squeezed. i.e. the topology is assigned less slots than desired.</li>
     *   <li>There are unassigned executors in this topology</li>
     * </ul>
     */
    public boolean needsScheduling(TopologyDetails topology) {
        int desiredNumWorkers = topology.getNumWorkers();
        int assignedNumWorkers = this.getAssignedNumWorkers(topology);
        return desiredNumWorkers > assignedNumWorkers || getUnassignedExecutors(topology).size() > 0;
    }

    /**
     * @param topology
     * @return a executor -> component-id map which needs scheduling in this topology.
     */
    public Map<ExecutorDetails, String> getNeedsSchedulingExecutorToComponents(TopologyDetails topology) {
        Collection<ExecutorDetails> allExecutors = new HashSet<>(topology.getExecutors());

        SchedulerAssignment assignment = this.assignments.get(topology.getId());
        if (assignment != null) {
            Collection<ExecutorDetails> assignedExecutors = assignment.getExecutors();
            allExecutors.removeAll(assignedExecutors);
        }

        return topology.selectExecutorToComponent(allExecutors);
    }

    /**
     * @param topology
     * @return a component-id -> executors map which needs scheduling in this topology.
     */
    public Map<String, List<ExecutorDetails>> getNeedsSchedulingComponentToExecutors(TopologyDetails topology) {
        Map<ExecutorDetails, String> executorToComponents = this.getNeedsSchedulingExecutorToComponents(topology);
        Map<String, List<ExecutorDetails>> componentToExecutors = new HashMap<String, List<ExecutorDetails>>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponents.entrySet()) {
            ExecutorDetails executor = entry.getKey();
            String component = entry.getValue();
            if (!componentToExecutors.containsKey(component)) {
                componentToExecutors.put(component, new ArrayList<ExecutorDetails>());
            }

            componentToExecutors.get(component).add(executor);
        }

        return componentToExecutors;
    }


    /**
     * Get all the used ports of this supervisor.
     */
    public Set<Integer> getUsedPorts(SupervisorDetails supervisor) {
        Set<Integer> usedPorts = new HashSet<>();

        for (SchedulerAssignment assignment : assignments.values()) {
            for (WorkerSlot slot : assignment.getExecutorToSlot().values()) {
                if (slot.getNodeId().equals(supervisor.getId())) {
                    usedPorts.add(slot.getPort());
                }
            }
        }

        return usedPorts;
    }

    /**
     * Return the available ports of this supervisor.
     */
    public Set<Integer> getAvailablePorts(SupervisorDetails supervisor) {
        Set<Integer> usedPorts = this.getUsedPorts(supervisor);

        Set<Integer> ret = new HashSet<>();
        ret.addAll(getAssignablePorts(supervisor));
        ret.removeAll(usedPorts);

        return ret;
    }

    public Set<Integer> getAssignablePorts(SupervisorDetails supervisor) {
        if (isBlackListed(supervisor.id)) {
            return Collections.emptySet();
        }
        return supervisor.allPorts;
    }

    /**
     * Return all the available slots on this supervisor.
     */
    public List<WorkerSlot> getAvailableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAvailablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    public List<WorkerSlot> getAssignableSlots(SupervisorDetails supervisor) {
        Set<Integer> ports = this.getAssignablePorts(supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }

        return slots;
    }

    /**
     * get the unassigned executors of the topology.
     */
    public Collection<ExecutorDetails> getUnassignedExecutors(TopologyDetails topology) {
        if (topology == null) {
            return new ArrayList<ExecutorDetails>(0);
        }

        Collection<ExecutorDetails> ret = new HashSet<>(topology.getExecutors());

        SchedulerAssignment assignment = getAssignmentById(topology.getId());
        if (assignment != null) {
            Set<ExecutorDetails> assignedExecutors = assignment.getExecutors();
            ret.removeAll(assignedExecutors);
        }

        return ret;
    }

    /**
     * @param topology
     * @return the number of workers assigned to this topology.
     */
    public int getAssignedNumWorkers(TopologyDetails topology) {
        SchedulerAssignment assignment = topology != null ? this.getAssignmentById(topology.getId()) : null;
        if (assignment == null) {
            return 0;
        }

        Set<WorkerSlot> slots = new HashSet<WorkerSlot>();
        slots.addAll(assignment.getExecutorToSlot().values());
        return slots.size();
    }

    private WorkerResources calculateWorkerResources(TopologyDetails td, Collection<ExecutorDetails> executors) {
        double onHeapMem = 0.0;
        double offHeapMem = 0.0;
        double cpu = 0.0;
        double sharedOn = 0.0;
        double sharedOff = 0.0;
        for (ExecutorDetails exec : executors) {
            Double onHeapMemForExec = td.getOnHeapMemoryRequirement(exec);
            if (onHeapMemForExec != null) {
                onHeapMem += onHeapMemForExec;
            }
            Double offHeapMemForExec = td.getOffHeapMemoryRequirement(exec);
            if (offHeapMemForExec != null) {
                offHeapMem += offHeapMemForExec;
            }
            Double cpuForExec = td.getTotalCpuReqTask(exec);
            if (cpuForExec != null) {
                cpu += cpuForExec;
            }
        }
        
        for (SharedMemory shared : td.getSharedMemoryRequests(executors)) {
            onHeapMem += shared.get_on_heap();
            sharedOn += shared.get_on_heap();
            offHeapMem += shared.get_off_heap_worker();
            sharedOff += shared.get_off_heap_worker();
        }
        
        WorkerResources ret = new WorkerResources();
        ret.set_cpu(cpu);
        ret.set_mem_on_heap(onHeapMem);
        ret.set_mem_off_heap(offHeapMem);
        ret.set_shared_mem_on_heap(sharedOn);
        ret.set_shared_mem_off_heap(sharedOff);
        return ret;
    }
    
    /**
     * Would scheduling exec on ws fit?  With a heap <= maxHeap total memory added <= memoryAvailable and cpu added <= cpuAvailable
     * @param ws the slot to put it in
     * @param exec the executor to investigate
     * @param td the topology detains for this executor
     * @param maxHeap the maximum heap size for ws
     * @param memoryAvailable the amount of memory available
     * @param cpuAvailable the amount of CPU available
     * @return true it fits else false
     */
    public boolean wouldFit(WorkerSlot ws, ExecutorDetails exec, TopologyDetails td, double maxHeap,
            double memoryAvailable, double cpuAvailable) {
        //NOTE this is called lots and lots by schedulers, so anything we can do to make it faster is going to help a lot.
        //CPU is simplest because it does not have odd interactions.
        double cpuNeeded = td.getTotalCpuReqTask(exec);
        if (cpuNeeded > cpuAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough CPU {} > {}", td.getName(), exec, ws, cpuNeeded, cpuAvailable);
            }
            //Not enough CPU no need to try any more
            return false;
        }

        //Lets see if we can make the Memory one fast too, at least in the failure case.
        //The totalMemReq is not really that accurate because it does not include shared memory, but if it does not fit we know
        // Even with shared it will not work
        double minMemNeeded = td.getTotalMemReqTask(exec);
        if (minMemNeeded > memoryAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough Mem {} > {}", td.getName(), exec, ws, minMemNeeded, memoryAvailable);
            }
            //Not enough minimum MEM no need to try any more
            return false;
        }
        
        double currentTotal = 0.0;
        double afterTotal = 0.0;
        double afterOnHeap = 0.0;
        Set<ExecutorDetails> wouldBeAssigned = new HashSet<>();
        wouldBeAssigned.add(exec);
        SchedulerAssignmentImpl assignment = assignments.get(td.getId());
        if (assignment != null) {
            Collection<ExecutorDetails> currentlyAssigned = assignment.getSlotToExecutors().get(ws);
            if (currentlyAssigned != null) {
                wouldBeAssigned.addAll(currentlyAssigned);
                WorkerResources wrCurrent = calculateWorkerResources(td, currentlyAssigned);
                currentTotal = wrCurrent.get_mem_off_heap() + wrCurrent.get_mem_on_heap();
            }
            WorkerResources wrAfter = calculateWorkerResources(td, wouldBeAssigned);
            afterTotal = wrAfter.get_mem_off_heap() + wrAfter.get_mem_on_heap();
            afterOnHeap = wrAfter.get_mem_on_heap();
            
            currentTotal += calculateSharedOffHeapMemory(ws.getNodeId(), assignment);
            afterTotal += calculateSharedOffHeapMemory(ws.getNodeId(), assignment, exec);
        }

        double memoryAdded = afterTotal - currentTotal;
        if (memoryAdded > memoryAvailable) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} not enough Mem {} > {}", td.getName(), exec, ws, memoryAdded, memoryAvailable);
            }
            return false;
        }
        if (afterOnHeap > maxHeap) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Could not schedule {}:{} on {} HEAP would be too large {} > {}", td.getName(), exec, ws, afterOnHeap, maxHeap);
            }
            return false;
        }
        return true;
    }
    
    /**
     * Assign the slot to the executors for this topology.
     *
     * @throws RuntimeException if the specified slot is already occupied.
     */
    public void assign(WorkerSlot slot, String topologyId, Collection<ExecutorDetails> executors) {
        if (isSlotOccupied(slot)) {
            throw new RuntimeException("slot: [" + slot.getNodeId() + ", " + slot.getPort() + "] is already occupied.");
        }

        TopologyDetails td = topologies.getById(topologyId);
        if (td == null) {
            throw new IllegalArgumentException("Trying to schedule for topo " + topologyId + " but that is not a known topology " + topologies.getAllIds()); 
        }
        WorkerResources resources = calculateWorkerResources(td, executors);
        SchedulerAssignmentImpl assignment = assignments.get(topologyId);
        if (assignment == null) {
            assignment = new SchedulerAssignmentImpl(topologyId);
            assignments.put(topologyId, assignment);
        } else {
            for (ExecutorDetails executor : executors) {
                 if (assignment.isExecutorAssigned(executor)) {
                     throw new RuntimeException("Attempting to assign executor: " + executor + " of topology: "+ topologyId
                             + " to workerslot: " + slot + ". The executor is already assigned to workerslot: " + assignment.getExecutorToSlot().get(executor)
                             + ". The executor must unassigned before it can be assigned to another slot!");
                 }
            }
        }

        assignment.assign(slot, executors, resources);
        String nodeId = slot.getNodeId();
        assignment.setTotalSharedOffHeapMemory(nodeId, calculateSharedOffHeapMemory(nodeId, assignment));
        scheduledCPUCache.remove(nodeId);
        scheduledMemoryCache.remove(nodeId);
    }
    
    /**
     * Calculate the amount of shared off heap memory on a given nodes with the given assignment
     * @param nodeId the id of the node
     * @param assignment the current assignment
     * @return the amount of shared off heap memory for that node in MB
     */
    private double calculateSharedOffHeapMemory(String nodeId, SchedulerAssignmentImpl assignment) {
        return calculateSharedOffHeapMemory(nodeId, assignment, null);
    }
    
    private double calculateSharedOffHeapMemory(String nodeId, SchedulerAssignmentImpl assignment, ExecutorDetails extra) {
        TopologyDetails td = topologies.getById(assignment.getTopologyId());
        Set<ExecutorDetails> executorsOnNode = new HashSet<>();
        for (Entry<WorkerSlot, Collection<ExecutorDetails>> entry: assignment.getSlotToExecutors().entrySet()) {
            if (nodeId.equals(entry.getKey().getNodeId())) {
                executorsOnNode.addAll(entry.getValue());
            }
        }
        if (extra != null) {
            executorsOnNode.add(extra);
        }
        double memorySharedWithinNode = 0.0;
        //Now check for overlap on the node
        for (SharedMemory shared : td.getSharedMemoryRequests(executorsOnNode)) {
            memorySharedWithinNode += shared.get_off_heap_node();
        }
        return memorySharedWithinNode;
    }

    /**
     * @return all the available worker slots in the cluster.
     */
    public List<WorkerSlot> getAvailableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAvailableSlots(supervisor));
        }

        return slots;
    }

    public List<WorkerSlot> getAssignableSlots() {
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor : this.supervisors.values()) {
            slots.addAll(this.getAssignableSlots(supervisor));
        }

        return slots;
    }

    /**
     * Free the specified slot.
     *
     * @param slot
     */
    public void freeSlot(WorkerSlot slot) {
        // remove the slot from the existing assignments
        for (SchedulerAssignmentImpl assignment : this.assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                assignment.unassignBySlot(slot);

                String nodeId = slot.getNodeId();
                assignment.setTotalSharedOffHeapMemory(nodeId, calculateSharedOffHeapMemory(nodeId, assignment));
                scheduledCPUCache.remove(nodeId);
                scheduledMemoryCache.remove(nodeId);
            }
        }
    }

    /**
     * free the slots.
     *
     * @param slots
     */
    public void freeSlots(Collection<WorkerSlot> slots) {
        if (slots != null) {
            for (WorkerSlot slot : slots) {
                freeSlot(slot);
            }
        }
    }

    /**
     * @param slot the slot be to checked.
     * @return true if the specified slot is occupied.
     */
    public boolean isSlotOccupied(WorkerSlot slot) {
        for (SchedulerAssignment assignment : assignments.values()) {
            if (assignment.isSlotOccupied(slot)) {
                return true;
            }
        }

        return false;
    }

    /**
     * get the current assignment for the topology.
     */
    public SchedulerAssignment getAssignmentById(String topologyId) {
        if (assignments.containsKey(topologyId)) {
            return assignments.get(topologyId);
        }

        return null;
    }

    /**
     * get slots used by a topology
     */
    public Collection<WorkerSlot> getUsedSlotsByTopologyId(String topologyId) {
        SchedulerAssignmentImpl assignment = assignments.get(topologyId);
        if (assignment == null) {
            return Collections.emptySet();
        }
        return assignment.getSlots();
    }

    /**
     * Get a specific supervisor with the <code>nodeId</code>
     */
    public SupervisorDetails getSupervisorById(String nodeId) {
        return supervisors.get(nodeId);
    }

    public Collection<WorkerSlot> getUsedSlots() {
        Set<WorkerSlot> ret = new HashSet<>();
        for(SchedulerAssignmentImpl s: assignments.values()) {
            ret.addAll(s.getExecutorToSlot().values());
        }
        return ret;
    }

    /**
     * Get all the supervisors on the specified <code>host</code>.
     *
     * @param host hostname of the supervisor
     * @return the <code>SupervisorDetails</code> object.
     */
    public List<SupervisorDetails> getSupervisorsByHost(String host) {
        List<String> nodeIds = this.hostToId.get(host);
        List<SupervisorDetails> ret = new ArrayList<SupervisorDetails>();

        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                ret.add(this.getSupervisorById(nodeId));
            }
        }

        return ret;
    }

    /**
     * Get all the assignments.
     */
    public Map<String, SchedulerAssignment> getAssignments() {
        return new HashMap<String, SchedulerAssignment>(assignments);
    }

    /**
     * Set assignments for cluster
     */
    public void setAssignments(Map<String, ? extends SchedulerAssignment> newAssignments, boolean ignoreSingleExceptions) {
        assignments.clear();
        for (SchedulerAssignment assignment : newAssignments.values()) {
            assign(assignment, ignoreSingleExceptions);
        }
    }

    /**
     * Get all the supervisors.
     */
    public Map<String, SupervisorDetails> getSupervisors() {
        return this.supervisors;
    }

    /**
     * Get the total amount of CPU resources in cluster
     */
    public double getClusterTotalCPUResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : supervisors.values()) {
            sum += sup.getTotalCPU();
        }
        return sum;
    }

    /**
     * Get the total amount of memory resources in cluster
     */
    public double getClusterTotalMemoryResource() {
        double sum = 0.0;
        for (SupervisorDetails sup : supervisors.values()) {
            sum += sup.getTotalMemory();
        }
        return sum;
    }

    /*
    * Note: Make sure the proper conf was passed into the Cluster constructor before calling this function
    * It tries to load the proper network topography detection plugin specified in the config.
    */
    public Map<String, List<String>> getNetworkTopography() {
        return networkTopography;
    }

    @VisibleForTesting
    public void setNetworkTopography (Map<String, List<String>> networkTopography) {
        this.networkTopography.clear();
        this.networkTopography.putAll(networkTopography);
    }

    @SuppressWarnings("unchecked")
    private String getStringFromStringList(Object o) {
        StringBuilder sb = new StringBuilder();
        for (String s : (List<String>) o) {
            sb.append(s);
            sb.append(" ");
        }
        return sb.toString();
    }

    /*
     * Get heap memory usage for a worker's main process and logwriter process
     */
    public double getAssignedMemoryForSlot(Map<String, Object> topConf) {
        double totalWorkerMemory = 0.0;
        final Integer TOPOLOGY_WORKER_DEFAULT_MEMORY_ALLOCATION = 768;

        String topologyWorkerGcChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS) instanceof List) {
            topologyWorkerGcChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS));
        } else {
            topologyWorkerGcChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_GC_CHILDOPTS), null);
        }

        String workerGcChildopts = null;
        if (topConf.get(Config.WORKER_GC_CHILDOPTS) instanceof List) {
            workerGcChildopts = getStringFromStringList(topConf.get(Config.WORKER_GC_CHILDOPTS));
        } else {
            workerGcChildopts = Utils.getString(topConf.get(Config.WORKER_GC_CHILDOPTS), null);
        }

        Double memGcChildopts = null;
        memGcChildopts = Utils.parseJvmHeapMemByChildOpts(topologyWorkerGcChildopts, null);
        if (memGcChildopts == null) {
            memGcChildopts = Utils.parseJvmHeapMemByChildOpts(workerGcChildopts, null);
        }
        String topologyWorkerChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS) instanceof List) {
            topologyWorkerChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS));
        } else {
            topologyWorkerChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_CHILDOPTS), null);
        }
        Double memTopologyWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(topologyWorkerChildopts, null);

        String workerChildopts = null;
        if (topConf.get(Config.WORKER_CHILDOPTS) instanceof List) {
            workerChildopts = getStringFromStringList(topConf.get(Config.WORKER_CHILDOPTS));
        } else {
            workerChildopts = Utils.getString(topConf.get(Config.WORKER_CHILDOPTS), null);
        }
        Double memWorkerChildopts = Utils.parseJvmHeapMemByChildOpts(workerChildopts, null);

        if (memGcChildopts != null) {
            totalWorkerMemory += memGcChildopts;
        } else if (memTopologyWorkerChildopts != null) {
            totalWorkerMemory += memTopologyWorkerChildopts;
        } else if (memWorkerChildopts != null) {
            totalWorkerMemory += memWorkerChildopts;
        } else {
            totalWorkerMemory += Utils.getInt(topConf.get(Config.WORKER_HEAP_MEMORY_MB), TOPOLOGY_WORKER_DEFAULT_MEMORY_ALLOCATION);
        }

        String topoWorkerLwChildopts = null;
        if (topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS) instanceof List) {
            topoWorkerLwChildopts = getStringFromStringList(topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS));
        } else {
            topoWorkerLwChildopts = Utils.getString(topConf.get(Config.TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS), null);
        }
        if (topoWorkerLwChildopts != null) {
            totalWorkerMemory += Utils.parseJvmHeapMemByChildOpts(topoWorkerLwChildopts, 0.0);
        }
        return totalWorkerMemory;
    }

    private static final double PER_WORKER_CPU_SWAG = 100.0;

    /**
     * Update Assignments for the passed in topologies with estimates on the resources they are consuming
     * assuming that they are Multi-tenant topologies and not RAS ones.
     * @param topologies the topologies to schedule for
     */
    @SuppressWarnings("deprecation")
    public void addMTResourceEstimates(Topologies topologies) {
        for (Entry<String, SchedulerAssignmentImpl> entry : assignments.entrySet()) {
            SchedulerAssignmentImpl assignment = entry.getValue();
            String topId = assignment.getTopologyId();
            TopologyDetails td = topologies.getById(topId);
            if (td == null) {
                continue;
            }
            Map<String, Object> topConf = td.getConf();
            double assignedMemPerSlot = getAssignedMemoryForSlot(topConf);

            for (WorkerSlot ws: assignment.getSlots()) {
                assignment.updateResources(ws, assignedMemPerSlot, 0, PER_WORKER_CPU_SWAG);
            }
        }
    }

    /**
     * set scheduler status for a topology
     */
    public void setStatus(String topologyId, String status) {
        this.status.put(topologyId, status);
    }

    /**
     * Get all topology scheduler statuses
     */
    public Map<String, String> getStatusMap() {
        return this.status;
    }

    /**
     * set scheduler status map
     */
    public void setStatusMap(Map<String, String> statusMap) {
        this.status.clear();
        this.status.putAll(statusMap);
    }

    /**
     * Get the amount of resources used by topologies.  Used for displaying resource information on the UI
     * @return  a map that contains multiple topologies and the resources the topology requested and assigned.
     * Key: topology id Value: an array that describes the resources the topology requested and assigned in the following format:
     *  {requestedMemOnHeap, requestedMemOffHeap, requestedCpu, assignedMemOnHeap, assignedMemOffHeap, assignedCpu}
     */
    public Map<String, TopologyResources> getTopologyResourcesMap() {
        Map<String, TopologyResources> ret = new HashMap<>(assignments.size());
        for (TopologyDetails td : topologies.getTopologies()) {
            String topoId = td.getId();
            SchedulerAssignmentImpl assignment = assignments.get(topoId);
            ret.put(topoId, new TopologyResources(td, assignment));
        }
        return ret;
    }

    /**
     * Get the amount of used and free resources on a supervisor.  Used for displaying resource information on the UI
     * @return  a map where the key is the supervisor id and the value is a map that represents
     * resource usage for a supervisor in the following format: {totalMem, totalCpu, usedMem, usedCpu}
     */
    public Map<String, SupervisorResources> getSupervisorsResourcesMap() {
        Map<String, SupervisorResources> ret = new HashMap<>();
        for (SupervisorDetails sd: supervisors.values()) {
            ret.put(sd.getId(), new SupervisorResources(sd.getTotalMemory(), sd.getTotalMemory(), 0, 0));
        }
        for (SchedulerAssignmentImpl assignment : assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry : assignment.getScheduledResources().entrySet()) {
                String id = entry.getKey().getNodeId();
                SupervisorResources sr = ret.get(id);
                if (sr == null) {
                    sr = new SupervisorResources(0, 0, 0, 0);
                }
                sr = sr.add(entry.getValue());
                ret.put(id, sr);
            }
            Map<String, Double> sharedOffHeap = assignment.getTotalSharedOffHeapMemory();
            if (sharedOffHeap != null) {
                for (Entry<String, Double> entry: sharedOffHeap.entrySet()) {
                    String id = entry.getKey();
                    SupervisorResources sr = ret.get(id);
                    if (sr == null) {
                        sr = new SupervisorResources(0, 0, 0, 0);
                    }
                    sr = sr.addMem(entry.getValue());
                    ret.put(id, sr);
                }
            }
        }
        return ret;
    }

    /**
     * Gets the reference to the full topology->worker resource map.
     * @return map of topology -> map of worker slot ->resources for that worker
     */
    public Map<String, Map<WorkerSlot, WorkerResources>> getWorkerResourcesMap() {
        HashMap<String, Map<WorkerSlot, WorkerResources>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignmentImpl> entry: assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().getScheduledResources());
        }
        return ret;
    }
    
    public WorkerResources getWorkerResources(WorkerSlot ws) {
        WorkerResources ret = null;
        for (SchedulerAssignmentImpl assignment: assignments.values()) {
            ret = assignment.getScheduledResources().get(ws);
            if (ret != null) {
                break;
            }
        }
        return ret;
    }

    private final Map<String, Double> scheduledMemoryCache = new HashMap<>();

    public double getScheduledMemoryForNode(String nodeId) {
        Double ret = scheduledMemoryCache.get(nodeId);
        if (ret != null) {
            return ret;
        }
        double totalMemory = 0.0;
        for (SchedulerAssignmentImpl assignment: assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry: assignment.getScheduledResources().entrySet()) {
                if (nodeId.equals(entry.getKey().getNodeId())) {
                    WorkerResources resources = entry.getValue();
                    totalMemory += resources.get_mem_off_heap() + resources.get_mem_on_heap();
                }
            }
            Double sharedOffHeap = assignment.getTotalSharedOffHeapMemory().get(nodeId);
            if (sharedOffHeap != null) {
                totalMemory += sharedOffHeap;
            }
        }
        scheduledMemoryCache.put(nodeId, totalMemory);
        return totalMemory;
    }

    private final Map<String, Double> scheduledCPUCache = new HashMap<>();

    public double getScheduledCpuForNode(String nodeId) {
        Double ret = scheduledCPUCache.get(nodeId);
        if (ret != null) {
            return ret;
        }
        double totalCPU = 0.0;
        for (SchedulerAssignmentImpl assignment: assignments.values()) {
            for (Entry<WorkerSlot, WorkerResources> entry: assignment.getScheduledResources().entrySet()) {
                if (nodeId.equals(entry.getKey().getNodeId())) {
                    WorkerResources resources = entry.getValue();
                    totalCPU += resources.get_cpu();
                }
            }
        }
        scheduledCPUCache.put(nodeId, totalCPU);
        return totalCPU;
    }

    public INimbus getINimbus() {
        return this.inimbus;
    }

    public Map<String, Object> getConf() {
        return this.conf;
    }

    /**
     * Unassign everything for the given topology id
     * @param topoId
     */
    public void unassign(String topoId) {
        freeSlots(getUsedSlotsByTopologyId(topoId));
    }
    
    /**
     * Assign everything for the given topology
     * @param assignment the new assignment to make
     */
    public void assign(SchedulerAssignment assignment, boolean ignoreSingleExceptions) {
        String id = assignment.getTopologyId();
        Map<WorkerSlot, Collection<ExecutorDetails>> slotToExecs = assignment.getSlotToExecutors();
        for (Entry<WorkerSlot, Collection<ExecutorDetails>> entry: slotToExecs.entrySet()) {
            try {
                assign(entry.getKey(), id, entry.getValue());
            } catch (RuntimeException e) {
                if (!ignoreSingleExceptions) {
                    throw e;
                }
            }
        }
    }
}
