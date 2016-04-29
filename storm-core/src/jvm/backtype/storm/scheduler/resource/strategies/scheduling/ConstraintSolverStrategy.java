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

package backtype.storm.scheduler.resource.strategies.scheduling;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.RAS_Node;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.SchedulingStatus;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

public class ConstraintSolverStrategy implements IStrategy{

    private Map<String, RAS_Node> nodes;
    private Cluster cluster;
    private ArrayList<WorkerSlot> workerSlots;
    private Map<ExecutorDetails, String> execToComp = new HashMap<ExecutorDetails, String>();
    private Map<String, HashSet<ExecutorDetails>> compToExecs = new HashMap<String, HashSet<ExecutorDetails>>();
    private ArrayList<ExecutorDetails> sortedExecs = new ArrayList<ExecutorDetails>();
    private Map<WorkerSlot, RAS_Node> workerToNodes = new LinkedHashMap<WorkerSlot, RAS_Node>();
    private int numBacktrack = 0;
    private int traversalDepth = 0;

    //holds assignments
    private Map<ExecutorDetails, WorkerSlot> execToWorker = new HashMap<ExecutorDetails, WorkerSlot>();
    private Map<WorkerSlot, Set<String>> workerCompAssignment = new HashMap<WorkerSlot, Set<String>>();
    private Map<RAS_Node, Set<String>> nodeCompAssignment = new HashMap<RAS_Node, Set<String>>();
    private Map<WorkerSlot, Set<ExecutorDetails>> workerToExecs = new HashMap<WorkerSlot, Set<ExecutorDetails>>();

    //constraints and spreads
    private Map<String, Map<String, Integer>> constraintMatrix;
    private HashSet<String> spreadComps = new HashSet<String>();

    private int stackFrames = 0;

    private int maxTraversalDepth = 0;

    //hard coded max recursion depth to prevent stack overflow errors from crashing nimbus
    public static final int MAX_RECURSIVE_DEPTH = 1000000;

    private TopologyDetails topo;

    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverStrategy.class);

    @Override
    public void prepare(SchedulingState schedulingState) {
        this.nodes = schedulingState.nodes.getNodeMap();
        this.cluster = schedulingState.cluster;
    }

    @Override
    public SchedulingResult schedule(TopologyDetails td) {
        initialize(td);
        Map<ExecutorDetails, WorkerSlot> result = findScheduling();

        if (result == null) {
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, "Cannot find Scheduling that satisfy constraints");
        } else {
            Map<WorkerSlot, Collection<ExecutorDetails>> resultOrganized = new HashMap<>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
                ExecutorDetails exec = entry.getKey();
                WorkerSlot workerSlot = entry.getValue();
                if (this.cluster.getUnassignedExecutors(td).contains(exec)) {
                    if (!resultOrganized.containsKey(workerSlot)) {
                        resultOrganized.put(workerSlot, new LinkedList<ExecutorDetails>());
                    }
                    resultOrganized.get(workerSlot).add(exec);
                }
            }
            return SchedulingResult.successWithMsg(resultOrganized, "Fully Scheduled by ConstraintSolverStrategy (" + this.getTraversalDepth() + " states traversed)");
        }
    }

    public Map<ExecutorDetails, WorkerSlot> findScheduling() {
        //early detection/early fail
        if (!this.checkSchedulingFeasibility()) {
            return null;
        }
        return this.backtrackSearch(this.sortedExecs, 0);
    }

    /**
     * checks if a scheduling is even feasible
     */
    private boolean checkSchedulingFeasibility() {
        if (this.workerSlots.isEmpty()) {
            LOG.error("No Valid Slots specified");
            return false;
        }
        for (String comp : this.spreadComps) {
            int numExecs = this.compToExecs.get(comp).size();
            if (numExecs > this.nodes.size()) {
                LOG.error("Unsatisfiable constraint: Component: {} marked as spread has {} executors which is larger than number of nodes: {}", comp, numExecs, nodes.size());
                return false;
            }
        }
        if (this.execToComp.size() >= MAX_RECURSIVE_DEPTH) {
            LOG.error("Number of executors is greater than the MAX_RECURSION_DEPTH.  " +
                    "Either reduce number of executors or increase jvm stack size and increase MAX_RECURSION_DEPTH size. " +
                    "# of executors: {} Max recursive depth: {}", this.execToComp.size(), MAX_RECURSIVE_DEPTH);
            return false;
        }
        return true;
    }

    /**
     * Constructor initializes some structures for fast lookups
     */
    public void initialize(TopologyDetails topo) {
        this.topo = topo;
        //set max traversal depth
        this.maxTraversalDepth = Utils.getInt(topo.getConf().get(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL));

        //get worker to node mapping
        this.workerToNodes = getFreeWorkerToNodeMapping(this.nodes.values());

        //get all workerslots to use
        this.workerSlots = new ArrayList<WorkerSlot>(this.workerToNodes.keySet());

        //get mapping of execs to components
        this.execToComp = topo.getExecutorToComponent();
        //get mapping of components to executors
        this.compToExecs = getCompToExecs(this.execToComp);

        //get topology constraints
        List<List<String>> constraints = (List<List<String>>) topo.getConf().get(Config.TOPOLOGY_CONSTRAINTS);
        this.constraintMatrix = getConstraintMap(constraints, this.compToExecs.keySet());

        //get spread components
        if (topo.getConf().get(Config.TOPOLOGY_SPREAD_COMPONENTS) != null) {
            this.spreadComps = getSpreadComps((List<String>) topo.getConf()
                    .get(Config.TOPOLOGY_SPREAD_COMPONENTS), this.compToExecs.keySet());
        }

        //get a sorted list of unassigned executors based on number of constraints
        Set<ExecutorDetails> unassignedExecutors = new HashSet<ExecutorDetails>(this.cluster.getUnassignedExecutors(topo));
        for (ExecutorDetails exec : getSortedExecs(this.spreadComps, this.constraintMatrix, this.compToExecs)) {
            if (unassignedExecutors.contains(exec)) {
                this.sortedExecs.add(exec);
            }
        }

        //initialize structures
        for (RAS_Node node : this.nodes.values()) {
            this.nodeCompAssignment.put(node, new HashSet<String>());
        }
        for (WorkerSlot worker : this.workerSlots) {
            this.workerCompAssignment.put(worker, new HashSet<String>());
            this.workerToExecs.put(worker, new HashSet<ExecutorDetails>());
        }
        //populate with existing assignments
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topo.getId());
        if (existingAssignment != null) {
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : existingAssignment.getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = entry.getKey();
                String compId = this.execToComp.get(exec);
                WorkerSlot ws = entry.getValue();
                RAS_Node node = this.nodes.get(ws.getNodeId());
                //populate node to component Assignments
                this.nodeCompAssignment.get(node).add(compId);
                //populate worker to comp assignments
                if (!this.workerCompAssignment.containsKey(ws)) {
                    this.workerCompAssignment.put(ws, new HashSet<String>());
                    this.workerToExecs.put(ws, new HashSet<ExecutorDetails>());
                }
                this.workerCompAssignment.get(ws).add(compId);
                this.workerToExecs.get(ws).add(exec);
                //populate executor to worker assignments
                this.execToWorker.put(exec, ws);
            }
        }
        printDebugMessages(constraints);
    }

    /**
     * Backtracking algorithm does not take into account the ordering of executors in worker to reduce traversal space
     */

    private Map<ExecutorDetails, WorkerSlot> backtrackSearch(ArrayList<ExecutorDetails> execs, int execIndex) {

        if (this.traversalDepth % 1000 == 0) {
            LOG.debug("Traversal Depth: {}", this.traversalDepth);
            LOG.debug("stack frames: {}", this.stackFrames);
            LOG.debug("backtrack: {}", this.numBacktrack);
        }
        if (this.traversalDepth > this.maxTraversalDepth || this.stackFrames >= MAX_RECURSIVE_DEPTH) {
            LOG.warn("Exceeded max depth");
            return null;
        }
        this.traversalDepth++;

        if (this.isValidAssignment(this.execToWorker)) {
            return this.execToWorker;
        }

        for (int i = 0; i< this.workerSlots.size(); i++) {
            if (execIndex >= execs.size()) {
                break;
            }
            WorkerSlot workerSlot = this.workerSlots.get(i);
            ExecutorDetails exec = execs.get(execIndex);
            if (this.isExecAssignmentToWorkerValid(exec, workerSlot)) {
                RAS_Node node = this.workerToNodes.get(workerSlot);
                String comp = this.execToComp.get(exec);

                this.workerCompAssignment.get(workerSlot).add(comp);

                this.nodeCompAssignment.get(node).add(comp);

                this.execToWorker.put(exec, workerSlot);

                this.workerToExecs.get(workerSlot).add(exec);
                node.consumeResourcesforTask(exec, this.topo);

                this.stackFrames++;
                execIndex ++;
                Map<ExecutorDetails, WorkerSlot> results = this.backtrackSearch(execs, execIndex);;

                if (results != null) {
                    return results;
                }

                //backtracking
                this.workerCompAssignment.get(workerSlot).remove(comp);
                this.nodeCompAssignment.get(node).remove(comp);
                this.execToWorker.remove(exec);
                this.workerToExecs.get(workerSlot).remove(exec);
                node.freeResourcesForTask(exec, this.topo);
                this.numBacktrack++;
                execIndex --;
            }
        }
        return null;
    }

    /**
     * check if any constraints are violated if exec is scheduled on worker
     * Return true if scheduling exec on worker does not violate any constraints, returns false if it does
     */
    public boolean isExecAssignmentToWorkerValid(ExecutorDetails exec, WorkerSlot worker) {
        //check if we have already scheduled this exec
        if (this.execToWorker.containsKey(exec)) {
            return false;
        }

        //check resources
        RAS_Node node = this.workerToNodes.get(worker);
        double taskMem = this.topo.getTotalMemReqTask(exec);
        double taskCPU = this.topo.getTotalCpuReqTask(exec);
        if (!(node.getAvailableCpuResources() >= taskCPU && node.getAvailableMemoryResources() >= taskMem)) {
            return false;
        }

        //check max heap memory worker
        if(!(this.getWorkerScheduledMemoryAvailable(worker, this.topo, this.workerToExecs) >= taskMem)) {
            return false;
        }

        //check if exec can be on worker based on user defined component exclusions
        String execComp = this.execToComp.get(exec);
        for (String comp : this.workerCompAssignment.get(worker)) {
            if (this.constraintMatrix.get(execComp).get(comp) !=0) {
                return false;
            }
        }

        //check if exec satisfy spread
        if (this.spreadComps.contains(execComp)) {
            if (this.nodeCompAssignment.get(node).contains(execComp)) {
                return false;
            }
        }
        return true;
    }

    private Double getWorkerScheduledMemoryAvailable(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Set<ExecutorDetails>> scheduleAssignmentMap) {
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
    private Double getWorkerScheduledMemoryUse(WorkerSlot ws, TopologyDetails td, Map<WorkerSlot, Set<ExecutorDetails>> scheduleAssignmentMap) {
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
     * Checks if we are done with computing the scheduling
     */
    public boolean isValidAssignment(Map<ExecutorDetails, WorkerSlot> execWorkerAssignment) {
        return execWorkerAssignment.size() == this.execToComp.size();
    }

    Map<String, Map<String, Integer>> getConstraintMap(List<List<String>> constraints, Set<String> comps) {
        Map<String, Map<String, Integer>> matrix = new HashMap<String, Map<String, Integer>>();
        for (String comp : comps) {
            matrix.put(comp, new HashMap<String, Integer>());
            for (String comp2 : comps) {
                matrix.get(comp).put(comp2, 0);
            }
        }
        if (constraints!=null ) {
            for (List<String> constraintPair : constraints) {
                String comp1 = constraintPair.get(0);
                String comp2 = constraintPair.get(1);
                if (!matrix.containsKey(comp1)) {
                    LOG.warn("Comp: {} declared in constraints is not valid!", comp1);
                    continue;
                }
                if (!matrix.containsKey(comp2)) {
                    LOG.warn("Comp: {} declared in constraints is not valid!", comp2);
                    continue;
                }
                matrix.get(comp1).put(comp2, 1);
                matrix.get(comp2).put(comp1, 1);
            }
        }
        return matrix;
    }

    public int getNumBacktrack() {
        return this.numBacktrack;
    }

    public int getTraversalDepth() {
        return this.traversalDepth;
    }

    public int getRecursionDepth() {
        return this.stackFrames;
    }

    /**
     * Determines is a scheduling is valid and all constraints are satisfied
     */
    public boolean validateSolution(Map<ExecutorDetails, WorkerSlot> result) {
        if (result == null) {
            return false;
        }
        return this.checkSpreadSchedulingValid(result) && this.checkConstraintsSatisfied(result) && this.checkResourcesCorrect(result);
    }

    /**
     * check if constraints are satisfied
     */
    private boolean checkConstraintsSatisfied(Map<ExecutorDetails, WorkerSlot> result) {
        Map<WorkerSlot, List<String>> workerCompMap = new HashMap<WorkerSlot, List<String>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
            WorkerSlot worker = entry.getValue();
            ExecutorDetails exec = entry.getKey();
            String comp = this.execToComp.get(exec);
            if (!workerCompMap.containsKey(worker)) {
                workerCompMap.put(worker, new LinkedList<String>());
            }
            workerCompMap.get(worker).add(comp);
        }
        for (Map.Entry<WorkerSlot, List<String>> entry : workerCompMap.entrySet()) {
            List<String> comps = entry.getValue();
            for (int i=0; i<comps.size(); i++) {
                for (int j=0; j<comps.size(); j++) {
                    if (i != j && this.constraintMatrix.get(comps.get(i)).get(comps.get(j)) == 1) {
                        LOG.error("Incorrect Scheduling: worker exclusion for Component {} and {} not satisfied on WorkerSlot: {}", comps.get(i), comps.get(j), entry.getKey());
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * checks if spread scheduling is satisfied
     */
    private boolean checkSpreadSchedulingValid(Map<ExecutorDetails, WorkerSlot> result) {
        Map<WorkerSlot, HashSet<ExecutorDetails>> workerExecMap = new HashMap<WorkerSlot, HashSet<ExecutorDetails>>();
        Map<WorkerSlot, HashSet<String>> workerCompMap = new HashMap<WorkerSlot, HashSet<String>>();
        Map<RAS_Node, HashSet<String>> nodeCompMap = new HashMap<RAS_Node, HashSet<String>>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            String comp = this.execToComp.get(exec);
            RAS_Node node = this.workerToNodes.get(worker);

            if (!workerExecMap.containsKey(worker)) {
                workerExecMap.put(worker, new HashSet<ExecutorDetails>());
                workerCompMap.put(worker, new HashSet<String>());
            }

            if (!nodeCompMap.containsKey(node)) {
                nodeCompMap.put(node, new HashSet<String>());
            }
            if (workerExecMap.get(worker).contains(exec)) {
                LOG.error("Incorrect Scheduling: Found duplicate in scheduling");
                return false;
            }
            workerExecMap.get(worker).add(exec);
            workerCompMap.get(worker).add(comp);
            if (this.spreadComps.contains(comp)) {
                if (nodeCompMap.get(node).contains(comp)) {
                    LOG.error("Incorrect Scheduling: Spread for Component: {} not satisfied", comp);
                    return false;
                }
            }
            nodeCompMap.get(node).add(comp);
        }
        return true;
    }

    /**
     * Check if resource constraints satisfied
     */
    private boolean checkResourcesCorrect(Map<ExecutorDetails, WorkerSlot> result) {

        Map<RAS_Node, Collection<ExecutorDetails>> nodeToExecs = new HashMap<RAS_Node, Collection<ExecutorDetails>>();
        Map<ExecutorDetails, WorkerSlot> mergedExecToWorker = new HashMap<ExecutorDetails, WorkerSlot>();
        //merge with existing assignments
        if (this.cluster.getAssignmentById(this.topo.getId()) != null
                && this.cluster.getAssignmentById(this.topo.getId()).getExecutorToSlot() != null) {
            mergedExecToWorker.putAll(this.cluster.getAssignmentById(this.topo.getId()).getExecutorToSlot());
        }
        mergedExecToWorker.putAll(result);

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : mergedExecToWorker.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RAS_Node node = this.nodes.get(worker.getNodeId());

            if (node.getAvailableMemoryResources() < 0.0 && node.getAvailableCpuResources() < 0.0) {
                LOG.error("Incorrect Scheduling: found node that negative available resources");
                return false;
            }
            if (!nodeToExecs.containsKey(node)) {
                nodeToExecs.put(node, new LinkedList<ExecutorDetails>());
            }
            nodeToExecs.get(node).add(exec);
        }

        for (Map.Entry<RAS_Node, Collection<ExecutorDetails>> entry : nodeToExecs.entrySet()) {
            RAS_Node node = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            double cpuUsed = 0.0;
            double memoryUsed = 0.0;
            for (ExecutorDetails exec : execs) {
                cpuUsed += this.topo.getTotalCpuReqTask(exec);
                memoryUsed += this.topo.getTotalMemReqTask(exec);
            }
            if (node.getAvailableCpuResources() != (node.getTotalCpuResources() - cpuUsed)) {
                LOG.error("Incorrect Scheduling: node {} has consumed incorrect amount of cpu. Expected: {} Actual: {} Executors scheduled on node: {}",
                        node.getId(), (node.getTotalCpuResources() - cpuUsed), node.getAvailableCpuResources(), execs);
                return false;
            }
            if (node.getAvailableMemoryResources() != (node.getTotalMemoryResources() - memoryUsed)) {
                LOG.error("Incorrect Scheduling: node {} has consumed incorrect amount of memory. Expected: {} Actual: {} Executors scheduled on node: {}",
                        node.getId(), (node.getTotalMemoryResources() - memoryUsed), node.getAvailableMemoryResources(), execs);
                return false;
            }
        }
        return true;
    }

    private Map<WorkerSlot, RAS_Node> getFreeWorkerToNodeMapping(Collection<RAS_Node> nodes) {
        Map<WorkerSlot, RAS_Node> workers = new LinkedHashMap<WorkerSlot, RAS_Node>();
        Map<RAS_Node, Stack<WorkerSlot>> nodeWorkerMap = new HashMap<RAS_Node, Stack<WorkerSlot>>();
        for (RAS_Node node : nodes) {
            nodeWorkerMap.put(node, new Stack<WorkerSlot>());
            nodeWorkerMap.get(node).addAll(node.getFreeSlots());
        }

        for (Map.Entry<RAS_Node, Stack<WorkerSlot>> entry : nodeWorkerMap.entrySet()) {
            Stack<WorkerSlot> slots = entry.getValue();
            RAS_Node node = entry.getKey();
            for (WorkerSlot slot : slots) {
                workers.put(slot, node);
            }
        }
        return workers;
    }

    private Map<String, HashSet<ExecutorDetails>> getCompToExecs(Map<ExecutorDetails, String> executorToComp) {
        Map<String, HashSet<ExecutorDetails>> retMap = new HashMap<String, HashSet<ExecutorDetails>>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComp.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            String comp = entry.getValue();
            if (!retMap.containsKey(comp)) {
                retMap.put(comp, new HashSet<ExecutorDetails>());
            }
            retMap.get(comp).add(exec);
        }
        return retMap;
    }

    private ArrayList<ExecutorDetails> getSortedExecs(HashSet<String> spreadComps, Map<String, Map<String, Integer>> constraintMatrix, Map<String, HashSet<ExecutorDetails>> compToExecs) {
        ArrayList<ExecutorDetails> retList = new ArrayList<ExecutorDetails>();
        //find number of constraints per component
        //Key->Comp Value-># of constraints
        Map<String, Integer> compConstraintCountMap = new HashMap<String, Integer>();
        for (Map.Entry<String, Map<String, Integer>> constraintEntry1 : constraintMatrix.entrySet()) {
            int count = 0;
            String comp = constraintEntry1.getKey();
            for (Map.Entry<String, Integer> constraintEntry2 : constraintEntry1.getValue().entrySet()) {
                if (constraintEntry2.getValue() == 1) {
                    count++;
                }
            }
            //check component is declared for spreading
            if (spreadComps.contains(constraintEntry1.getKey())) {
                count++;
            }
            compConstraintCountMap.put(comp, count);
        }
        //Sort comps by number of constraints
        TreeMap<String, Integer> sortedCompConstraintCountMap = (TreeMap<String, Integer>) sortByValues(compConstraintCountMap);
        //sort executors based on component constraints
        for (String comp : sortedCompConstraintCountMap.keySet()) {
            retList.addAll(compToExecs.get(comp));
        }
        return retList;
    }

    private HashSet<String> getSpreadComps(List<String> spreads, Set<String> comps) {
        HashSet<String> retSet = new HashSet<String>();
        for (String comp : spreads) {
            if (comps.contains(comp)) {
                retSet.add(comp);
            } else {
                LOG.warn("Comp {} declared for spread not valid", comp);
            }
        }
        return retSet;
    }

    private void printDebugMessages(List<List<String>> constraints) {
        LOG.debug("maxTraversalDepth: {}", maxTraversalDepth);
        LOG.debug("Components to Spread: {}", this.spreadComps);
        LOG.debug("Constraints: {}", constraints);
        for (Map.Entry<String, Map<String, Integer>> entry : this.constraintMatrix.entrySet()) {
            LOG.debug(entry.getKey() + " -> " + entry.getValue());
        }
        for (Map.Entry<String, HashSet<ExecutorDetails>> entry : this.compToExecs.entrySet()) {
            LOG.debug("{} -> {}", entry.getKey(), entry.getValue());
        }
        LOG.debug("Size: {} Sorted Executors: {}", this.sortedExecs.size(), this.sortedExecs);
        LOG.debug("Size: {} nodes: {}", this.nodes.size(), this.nodes.values());
        LOG.debug("Size: {} workers: {}", this.workerSlots.size(), this.workerSlots);
    }

    /**
     * For sorting tree map by value
     */
    public static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator = new Comparator<K>() {
            public int compare(K k1, K k2) {
                int compare = map.get(k2).compareTo(map.get(k1));
                if (compare == 0) {
                    return k2.compareTo(k1);
                } else {
                    return compare;
                }
            }
        };
        Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }
}
