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

package backtype.storm.scheduler.multitenant;

import backtype.storm.Config;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

/**
 * Constraint Solver for multitenant
 * The constraint solver will compute a scheduling that satisfy user declared constraints for scheduling
 */
public class ConstraintSolverForMultitenant {
    private Set<Node> nodes;
    private ArrayList<WorkerSlot> workerSlots;
    private Map<ExecutorDetails, String> execToComp = new HashMap<ExecutorDetails, String>();
    private Map<String, HashSet<ExecutorDetails>> compToExecs = new HashMap<String, HashSet<ExecutorDetails>>();
    private ArrayList<ExecutorDetails> sortedExecs = new ArrayList<ExecutorDetails>();
    private Map<WorkerSlot, Node> workerToNodes = new LinkedHashMap<WorkerSlot, Node>();
    private int numBacktrack = 0;
    private int traversalDepth = 0;

    //holds assignments
    private HashMap<ExecutorDetails, WorkerSlot> execToWorker = new HashMap<ExecutorDetails, WorkerSlot>();
    private Map<WorkerSlot, HashSet<String>> workerCompAssignment = new HashMap<WorkerSlot, HashSet<String>>();
    private Map<Node, HashSet<String>> nodeCompAssignment = new HashMap<Node, HashSet<String>>();

    //constraints and spreads
    private Map<String, Map<String, Integer>> constraintMatrix;
    private HashSet<String> spreadComps = new HashSet<String>();

    private int workerTraversalIndex = 0;

    private int stackFrames = 0;

    private int maxTraversalDepth = 0;

    //hard coded max recursion depth to prevent stack overflow errors from crashing nimbus
    public static final int MAX_RECURSIVE_DEPTH = 1800;

    private static final Logger LOG = LoggerFactory.getLogger(ConstraintSolverForMultitenant.class);

    /**
     * Constructor initializes some structures for fast lookups
     */
    public ConstraintSolverForMultitenant(TopologyDetails topo, int slotsToUse, Set<Node> nodes) {
        //set max traversal depth
        maxTraversalDepth = Utils.getInt(topo.getConf().get(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL));
        //get nodes to use
        this.nodes = nodes;

        //get worker to node mapping
        this.workerToNodes = getWorkerToNodeMapping(slotsToUse, this.nodes);

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

        //get a sorted list of executors based on number of constraints
        this.sortedExecs = getSortedExecs(this.spreadComps, this.constraintMatrix, this.compToExecs);


        //initialize structures
        for (Node node : this.nodes) {
            this.nodeCompAssignment.put(node, new HashSet<String>());
        }
        for (WorkerSlot worker : this.workerSlots) {
            this.workerCompAssignment.put(worker, new HashSet<String>());
        }

        printDebugMessages(constraints);
    }

    public Map<ExecutorDetails, WorkerSlot> findScheduling() {
        //early detection/early fail
        if (!this.checkSchedulingFeasibility()) {
            return null;
        }
        return this.backtrackSearch();
    }

    /**
     * Backtracking algorithm
     */
    private Map<ExecutorDetails, WorkerSlot> backtrackSearch() {

        if (this.traversalDepth % 10000 == 0) {
            LOG.debug("Traversal Depth: {}", this.traversalDepth);
            LOG.debug("stack frames: {}", this.stackFrames);
        }
        if (this.traversalDepth > this.maxTraversalDepth || this.stackFrames >= MAX_RECURSIVE_DEPTH) {
            return null;
        }
        this.traversalDepth++;

        if (this.isValidAssignment(this.execToWorker)) {
            return this.execToWorker;
        }

        for (ExecutorDetails exec : this.sortedExecs) {
            if (this.workerTraversalIndex >= this.workerSlots.size()) {
                this.workerTraversalIndex = 0;
            }
            WorkerSlot workerSlot = this.workerSlots.get(this.workerTraversalIndex);
            //check constraints
            if (this.isExecAssignmentToWorkerValid(exec, workerSlot)) {
                Node node = this.workerToNodes.get(workerSlot);
                String comp = this.execToComp.get(exec);

                this.workerTraversalIndex++;

                this.workerCompAssignment.get(workerSlot).add(comp);

                this.nodeCompAssignment.get(node).add(comp);

                this.execToWorker.put(exec, workerSlot);

                this.stackFrames++;

                Map<ExecutorDetails, WorkerSlot> results = this.backtrackSearch();
                if (results != null) {
                    return results;
                }

                //backtracking
                this.workerCompAssignment.get(workerSlot).remove(comp);
                this.nodeCompAssignment.get(node).remove(comp);
                this.execToWorker.remove(exec);
                this.numBacktrack++;
            }
        }
        return null;
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
     * check if any constraints are violated if exec is scheduled on worker
     * Return true if scheduling exec on worker does not violate any constraints, returns false if it does
     */
    public boolean isExecAssignmentToWorkerValid(ExecutorDetails exec, WorkerSlot worker) {
        //check if we have already scheduled this exec
        if (this.execToWorker.containsKey(exec)) {
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
            Node node = this.workerToNodes.get(worker);
            if (this.nodeCompAssignment.get(node).contains(execComp)) {
                return false;
            }
        }
        return true;
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
        return this.checkSpreadSchedulingValid(result) && this.checkConstraintsSatisfied(result);
    }

    /**
     * check if constraints are satisfied
     */
    private boolean checkConstraintsSatisfied(Map<ExecutorDetails, WorkerSlot> result) {
        for (Map.Entry<WorkerSlot, HashSet<String>> entry : this.workerCompAssignment.entrySet()) {
            for (String comp : entry.getValue()) {
                for (String checkComp : entry.getValue()) {
                    if (this.constraintMatrix.get(comp).get(checkComp) == 1) {
                        LOG.error("Incorrect Scheduling: worker exclusion for Component {} and {} not satisfied on WorkerSlot: {}", comp, checkComp, entry.getKey());
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
        Map<Node, HashSet<String>> nodeCompMap = new HashMap<Node, HashSet<String>>();

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : result.entrySet()) {
            ExecutorDetails exec = entry.getKey();
            WorkerSlot worker = entry.getValue();
            String comp = this.execToComp.get(exec);
            Node node = this.workerToNodes.get(worker);

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

    private Map<WorkerSlot, Node> getWorkerToNodeMapping(int slotsToUse, Set<Node> nodes) {
        Map<WorkerSlot, Node> workers = new LinkedHashMap<WorkerSlot, Node>();
        Map<Node, Stack<WorkerSlot>> nodeWorkerMap = new HashMap<Node, Stack<WorkerSlot>>();
        for (Node node : nodes) {
            nodeWorkerMap.put(node, new Stack<WorkerSlot>());
            nodeWorkerMap.get(node).addAll(node.getFreeSlots());
        }

        Iterator<Map.Entry<Node, Stack<WorkerSlot>>> iterator = nodeWorkerMap.entrySet().iterator();
        while(workers.size() < slotsToUse) {
            WorkerSlot ws = null;
            if (!iterator.hasNext()) {
                iterator = nodeWorkerMap.entrySet().iterator();
            }
            Map.Entry<Node, Stack<WorkerSlot>> entry = iterator.next();
            Stack<WorkerSlot> slots = entry.getValue();
            Node node = entry.getKey();
            if (slots.size() > 0) {
                ws = slots.pop();
            }
            if (ws != null) {
                workers.put(ws, node);
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
        LOG.debug("Size: {} nodes: {}", this.nodes.size(), this.nodes);
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
