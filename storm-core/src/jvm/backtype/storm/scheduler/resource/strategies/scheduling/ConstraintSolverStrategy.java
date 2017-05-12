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
    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverStrategy.class);
    
    private Map<String, RAS_Node> nodes;
    private Cluster cluster;
    private ArrayList<WorkerSlot> workerSlots;
    private Map<ExecutorDetails, String> execToComp;
    private Map<String, HashSet<ExecutorDetails>> compToExecs;
    private ArrayList<ExecutorDetails> sortedExecs;
    private Map<WorkerSlot, RAS_Node> workerToNodes;
    private int numBacktrack = 0;
    private int traversalDepth = 0;

    //holds assignments
    private Map<ExecutorDetails, WorkerSlot> execToWorker;
    private Map<WorkerSlot, Set<String>> workerCompAssignment;
    private Map<RAS_Node, Set<String>> nodeCompAssignment;
    private Map<WorkerSlot, Set<ExecutorDetails>> workerToExecs;

    //constraints and spreads
    private Map<String, Map<String, Integer>> constraintMatrix;
    private HashSet<String> spreadComps = new HashSet<String>();

    private int stackFrames = 0;

    private int maxTraversalDepth = 0;

    //hard coded max recursion depth to prevent stack overflow errors from crashing nimbus
    public static final int MAX_RECURSIVE_DEPTH = 1000000;

    private TopologyDetails topo;

    @Override
    public void prepare(SchedulingState schedulingState) {
        nodes = schedulingState.nodes.getNodeMap();
        cluster = schedulingState.cluster;
        sortedExecs = new ArrayList<ExecutorDetails>();
        execToWorker = new HashMap<ExecutorDetails, WorkerSlot>();
        workerCompAssignment = new HashMap<WorkerSlot, Set<String>>();
        nodeCompAssignment = new HashMap<RAS_Node, Set<String>>();
        workerToExecs = new HashMap<WorkerSlot, Set<ExecutorDetails>>();
        numBacktrack = 0;
        traversalDepth = 0;
        stackFrames = 0;
    }

    @Override
    public SchedulingResult schedule(TopologyDetails td) {
        initialize(td);

        //early detection/early fail
        if (!checkSchedulingFeasibility()) {
            //Scheduling Status set to FAIL_OTHER so no eviction policy will be attempted to make space for this topology
            return SchedulingResult.failure(SchedulingStatus.FAIL_OTHER, "Scheduling not feasible!");
        }
        Collection<ExecutorDetails> unassigned = cluster.getUnassignedExecutors(td);
        Map<ExecutorDetails, WorkerSlot> result = findScheduling();
        if (result == null) {
            return SchedulingResult.failure(SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES, "Cannot find Scheduling that satisfy constraints");
        } else {
            Map<WorkerSlot, Collection<ExecutorDetails>> resultOrganized = new HashMap<>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
                ExecutorDetails exec = entry.getKey();
                WorkerSlot workerSlot = entry.getValue();
                if (unassigned.contains(exec)) {
                    Collection<ExecutorDetails> execs = resultOrganized.get(workerSlot);
                    if (execs == null) {
                        execs = new LinkedList<>();
                        resultOrganized.put(workerSlot, execs);
                    }
                    execs.add(exec);
                }
            }
            return SchedulingResult.successWithMsg(resultOrganized, "Fully Scheduled by ConstraintSolverStrategy (" + getTraversalDepth() + " states traversed)");
        }
    }

    private Map<ExecutorDetails, WorkerSlot> findScheduling() {
        return backtrackSearch(sortedExecs, 0);
    }

    /**
     * checks if a scheduling is even feasible
     */
    private boolean checkSchedulingFeasibility() {
        if (workerSlots.isEmpty()) {
            LOG.error("No Valid Slots specified");
            return false;
        }
        for (String comp : spreadComps) {
            int numExecs = compToExecs.get(comp).size();
            if (numExecs > nodes.size()) {
                LOG.error("Unsatisfiable constraint: Component: {} marked as spread has {} executors which is larger than number of nodes: {}", comp, numExecs, nodes.size());
                return false;
            }
        }
        if (execToComp.size() >= MAX_RECURSIVE_DEPTH) {
            LOG.error("Number of executors is greater than the MAX_RECURSION_DEPTH.  " +
                    "Either reduce number of executors or increase jvm stack size and increase MAX_RECURSION_DEPTH size. " +
                    "# of executors: {} Max recursive depth: {}", execToComp.size(), MAX_RECURSIVE_DEPTH);
            return false;
        }
        return true;
    }

    /**
     * Constructor initializes some structures for fast lookups
     */
    @SuppressWarnings("unchecked")
    public void initialize(TopologyDetails topo) {
        this.topo = topo;
        //set max traversal depth
        maxTraversalDepth = Utils.getInt(topo.getConf().get(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL));

        //get worker to node mapping
        workerToNodes = getFreeWorkerToNodeMapping(nodes.values());

        //get all workerslots to use
        workerSlots = new ArrayList<WorkerSlot>(workerToNodes.keySet());

        //get mapping of execs to components
        execToComp = topo.getExecutorToComponent();
        //get mapping of components to executors
        compToExecs = getCompToExecs(execToComp);

        //get topology constraints
        List<List<String>> constraints = (List<List<String>>) topo.getConf().get(Config.TOPOLOGY_CONSTRAINTS);
        constraintMatrix = getConstraintMap(constraints, compToExecs.keySet());

        //get spread components
        if (topo.getConf().get(Config.TOPOLOGY_SPREAD_COMPONENTS) != null) {
            spreadComps = getSpreadComps((List<String>) topo.getConf()
                    .get(Config.TOPOLOGY_SPREAD_COMPONENTS), compToExecs.keySet());
        }

        //get a sorted list of unassigned executors based on number of constraints
        Set<ExecutorDetails> unassignedExecutors = new HashSet<ExecutorDetails>(cluster.getUnassignedExecutors(topo));
        for (ExecutorDetails exec : getSortedExecs(spreadComps, constraintMatrix, compToExecs)) {
            if (unassignedExecutors.contains(exec)) {
                sortedExecs.add(exec);
            }
        }

        //initialize structures
        for (RAS_Node node : nodes.values()) {
            nodeCompAssignment.put(node, new HashSet<String>());
        }
        for (WorkerSlot worker : workerSlots) {
            workerCompAssignment.put(worker, new HashSet<String>());
            workerToExecs.put(worker, new HashSet<ExecutorDetails>());
        }
        //populate with existing assignments
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topo.getId());
        if (existingAssignment != null) {
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : existingAssignment.getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = entry.getKey();
                String compId = execToComp.get(exec);
                WorkerSlot ws = entry.getValue();
                RAS_Node node = nodes.get(ws.getNodeId());
                //populate node to component Assignments
                nodeCompAssignment.get(node).add(compId);
                //populate worker to comp assignments
                if (!workerCompAssignment.containsKey(ws)) {
                    workerCompAssignment.put(ws, new HashSet<String>());
                    workerToExecs.put(ws, new HashSet<ExecutorDetails>());
                }
                workerCompAssignment.get(ws).add(compId);
                workerToExecs.get(ws).add(exec);
                //populate executor to worker assignments
                execToWorker.put(exec, ws);
            }
        }
        printDebugMessages(constraints);
    }

    /**
     * Backtracking algorithm does not take into account the ordering of executors in worker to reduce traversal space
     */
    private Map<ExecutorDetails, WorkerSlot> backtrackSearch(ArrayList<ExecutorDetails> execs, int execIndex) {

        if (traversalDepth % 100000 == 0) {
            LOG.debug("Traversal Depth: {}", traversalDepth);
            LOG.debug("stack frames: {}", stackFrames);
            LOG.debug("backtrack: {}", numBacktrack);
        }
        if (traversalDepth > maxTraversalDepth || stackFrames >= MAX_RECURSIVE_DEPTH) {
            LOG.warn("Exceeded max depth");
            return null;
        }
        traversalDepth++;

        if (isValidAssignment(execToWorker)) {
            return execToWorker;
        }

        for (WorkerSlot workerSlot : workerSlots) {
            if (execIndex >= execs.size()) {
                break;
            }
            ExecutorDetails exec = execs.get(execIndex);
            if (isExecAssignmentToWorkerValid(exec, workerSlot)) {
                RAS_Node node = workerToNodes.get(workerSlot);
                String comp = execToComp.get(exec);

                workerCompAssignment.get(workerSlot).add(comp);

                nodeCompAssignment.get(node).add(comp);

                execToWorker.put(exec, workerSlot);

                workerToExecs.get(workerSlot).add(exec);
                node.assignSingleExecutor(workerSlot, exec, topo);

                stackFrames++;
                execIndex ++;
                Map<ExecutorDetails, WorkerSlot> results = backtrackSearch(execs, execIndex);;

                if (results != null) {
                    return results;
                }

                //backtracking
                workerCompAssignment.get(workerSlot).remove(comp);
                nodeCompAssignment.get(node).remove(comp);
                execToWorker.remove(exec);
                workerToExecs.get(workerSlot).remove(exec);
                node.freeSingleExecutor(exec, topo);
                numBacktrack++;
                execIndex--;
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
        if (execToWorker.containsKey(exec)) {
            return false;
        }

        //check resources
        RAS_Node node = workerToNodes.get(worker);
        if (!node.wouldFit(worker, exec, topo)) {
            return false;
        }

        //check if exec can be on worker based on user defined component exclusions
        String execComp = execToComp.get(exec);
        for (String comp : workerCompAssignment.get(worker)) {
            if (constraintMatrix.get(execComp).get(comp) !=0) {
                return false;
            }
        }

        //check if exec satisfy spread
        if (spreadComps.contains(execComp)) {
            if (nodeCompAssignment.get(node).contains(execComp)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if we are done with computing the scheduling
     */
    public boolean isValidAssignment(Map<ExecutorDetails, WorkerSlot> execWorkerAssignment) {
        return execWorkerAssignment.size() == execToComp.size();
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
        return numBacktrack;
    }

    public int getTraversalDepth() {
        return traversalDepth;
    }

    public int getRecursionDepth() {
        return stackFrames;
    }

    /**
     * Determines is a scheduling is valid and all constraints are satisfied
     */
    public boolean validateSolution(Map<ExecutorDetails, WorkerSlot> result) {
        if (result == null) {
            return false;
        }
        return checkSpreadSchedulingValid(result) && checkConstraintsSatisfied(result) && checkResourcesCorrect(result);
    }

    /**
     * check if constraints are satisfied
     */
    private boolean checkConstraintsSatisfied(Map<ExecutorDetails, WorkerSlot> result) {
        Map<WorkerSlot, List<String>> workerCompMap = new HashMap<WorkerSlot, List<String>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
            WorkerSlot worker = entry.getValue();
            ExecutorDetails exec = entry.getKey();
            String comp = execToComp.get(exec);
            if (!workerCompMap.containsKey(worker)) {
                workerCompMap.put(worker, new LinkedList<String>());
            }
            workerCompMap.get(worker).add(comp);
        }
        for (Map.Entry<WorkerSlot, List<String>> entry : workerCompMap.entrySet()) {
            List<String> comps = entry.getValue();
            for (int i=0; i<comps.size(); i++) {
                for (int j=0; j<comps.size(); j++) {
                    if (i != j && constraintMatrix.get(comps.get(i)).get(comps.get(j)) == 1) {
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
            String comp = execToComp.get(exec);
            RAS_Node node = workerToNodes.get(worker);

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
            if (spreadComps.contains(comp)) {
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
        if (cluster.getAssignmentById(topo.getId()) != null
                && cluster.getAssignmentById(topo.getId()).getExecutorToSlot() != null) {
            mergedExecToWorker.putAll(cluster.getAssignmentById(topo.getId()).getExecutorToSlot());
        }
        mergedExecToWorker.putAll(result);

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : mergedExecToWorker.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            RAS_Node node = nodes.get(worker.getNodeId());

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
                cpuUsed += topo.getTotalCpuReqTask(exec);
                memoryUsed += topo.getTotalMemReqTask(exec);
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
        Map<WorkerSlot, RAS_Node> workers = new LinkedHashMap<>();
        Map<RAS_Node, Stack<WorkerSlot>> nodeWorkerMap = new HashMap<>();
        for (RAS_Node node : nodes) {
            nodeWorkerMap.put(node, new Stack<>());
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
        LOG.debug("Components to Spread: {}", spreadComps);
        LOG.debug("Constraints: {}", constraints);
        for (Map.Entry<String, Map<String, Integer>> entry : constraintMatrix.entrySet()) {
            LOG.debug(entry.getKey() + " -> " + entry.getValue());
        }
        for (Map.Entry<String, HashSet<ExecutorDetails>> entry : compToExecs.entrySet()) {
            LOG.debug("{} -> {}", entry.getKey(), entry.getValue());
        }
        LOG.debug("Size: {} Sorted Executors: {}", sortedExecs.size(), sortedExecs);
        LOG.debug("Size: {} nodes: {}", nodes.size(), nodes.values());
        LOG.debug("Size: {} workers: {}", workerSlots.size(), workerSlots);
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
