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

package backtype.storm.scheduler.resource.strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.RAS_Component;
import backtype.storm.scheduler.resource.Node;

public class ResourceAwareStrategy implements IStrategy {
    protected Logger LOG = null;
    protected Topologies _topologies;
    protected TopologyDetails _topo;
    protected Collection<Node> _availNodes;
    protected Node refNode = null;
    /**
     * supervisor id -> Node
     */
    private Map<String, Node> _nodes;
    private Map<String, List<String>> _clusterInfo;

    private final double CPU_WEIGHT = 1.0;
    private final double MEM_WEIGHT = 1.0;
    private final double NETWORK_WEIGHT = 1.0;

    public ResourceAwareStrategy(
            TopologyDetails topo, Cluster cluster, Topologies topologies) {
        this._topologies = topologies;
        this._topo = topo;
        this._nodes = Node.getAllNodesFrom(cluster, _topologies);
        this._availNodes = this.getAvaiNodes();
        this.LOG = LoggerFactory.getLogger(this.getClass());
        this._clusterInfo = cluster.getNetworkTopography();
        LOG.info(_clusterInfo.toString());
    }

    protected void prepare() {
        LOG.info("Clustering info: {}", this._clusterInfo);
    }

    //the returned TreeMap keeps the RAS_Components sorted
    protected TreeMap<Integer, List<ExecutorDetails>> getPriorityToExecutorDetailsListMap(
            Queue<RAS_Component> ordered_RAS_Component_list, Collection<ExecutorDetails> unassignedExecutors) {
        TreeMap<Integer, List<ExecutorDetails>> retMap = new TreeMap<Integer, List<ExecutorDetails>>();
        Integer rank = 0;
        for (RAS_Component ras_comp : ordered_RAS_Component_list) {
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

    public Map<Node, Collection<ExecutorDetails>> schedule(Topologies topologies, TopologyDetails td,
                                                           Collection<ExecutorDetails> unassignedExecutors) {
        if (_availNodes.size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return null;
        }

        Map<Node, Collection<ExecutorDetails>> nodeToExecutorDetailsMap = new HashMap<Node, Collection<ExecutorDetails>>();
        LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<ExecutorDetails>();
        List<RAS_Component> spouts = this.getSpouts(topologies);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return null;
        }

        Queue<RAS_Component> ordered_RAS_Component_list = bfs(topologies, spouts);

        Map<Integer, List<ExecutorDetails>> priorityToExecutorMap = getPriorityToExecutorDetailsListMap(ordered_RAS_Component_list, unassignedExecutors);
        Collection<ExecutorDetails> executorsNotScheduled = new HashSet<ExecutorDetails>(unassignedExecutors);
        Integer longestPriorityListSize = this.getLongestPriorityListSize(priorityToExecutorMap);
        //Pick the first executor with priority one, then the 1st exec with priority 2, so on an so forth. 
        //Once we reach the last priority, we go back to priority 1 and schedule the second task with priority 1.
        for (int i = 0; i < longestPriorityListSize; i++) {
            for (Entry<Integer, List<ExecutorDetails>> entry : priorityToExecutorMap.entrySet()) {
                Iterator<ExecutorDetails> it = entry.getValue().iterator();
                if (it.hasNext()) {
                    ExecutorDetails exec = it.next();
                    LOG.info("\n\nAttempting to schedule: {} of component {}[avail {}] with rank {}",
                            new Object[] { exec, this._topo.getExecutorToComponent().get(exec),
                    this._topo.getTaskResourceReqList(exec), entry.getKey() });
                    Node scheduledNode = this.scheduleNodeForAnExecutorDetail(exec);
                    if (scheduledNode != null) {
                        if (nodeToExecutorDetailsMap.containsKey(scheduledNode) == false) {
                            Collection<ExecutorDetails> newExecutorDetailsMap = new LinkedList<ExecutorDetails>();
                            nodeToExecutorDetailsMap.put(scheduledNode, newExecutorDetailsMap);
                        }
                        nodeToExecutorDetailsMap.get(scheduledNode).add(exec);
                        scheduledNode.consumeResourcesforTask(exec, td);
                        scheduledTasks.add(exec);
                        LOG.info("TASK {} assigned to Node: {} avail [mem: {} cpu: {}] total [mem: {} cpu: {}]", exec,
                                scheduledNode, scheduledNode.getAvailableMemoryResources(),
                                scheduledNode.getAvailableCpuResources(), scheduledNode.getTotalMemoryResources(),
                                scheduledNode.getTotalCpuResources());
                    } else {
                        LOG.error("Not Enough Resources to schedule Task {}", exec);
                    }
                    it.remove();
                }
            }
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        LOG.info("/* Scheduling left over task (most likely sys tasks) */");
        // schedule left over system tasks
        for (ExecutorDetails detail : executorsNotScheduled) {
            Node bestNodeForAnExecutorDetail = this.scheduleNodeForAnExecutorDetail(detail);
            if (bestNodeForAnExecutorDetail != null) {
                if (!nodeToExecutorDetailsMap.containsKey(bestNodeForAnExecutorDetail)) {
                    Collection<ExecutorDetails> newMap = new LinkedList<ExecutorDetails>();
                    nodeToExecutorDetailsMap.put(bestNodeForAnExecutorDetail, newMap);
                }
                nodeToExecutorDetailsMap.get(bestNodeForAnExecutorDetail).add(detail);
                bestNodeForAnExecutorDetail.consumeResourcesforTask(detail, td);
                scheduledTasks.add(detail);
                LOG.info("TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}",
                        detail, bestNodeForAnExecutorDetail,
                        bestNodeForAnExecutorDetail.getAvailableMemoryResources(),
                        bestNodeForAnExecutorDetail.getAvailableCpuResources());
            } else {
                LOG.error("Not Enough Resources to schedule Task {}", detail);
            }
        }

        executorsNotScheduled.removeAll(scheduledTasks);
        if (executorsNotScheduled.size() > 0) {
            LOG.error("Not all executors successfully scheduled: {}",
                    executorsNotScheduled);
            nodeToExecutorDetailsMap = null;
        } else {
            LOG.debug("All resources successfully scheduled!");
        }

        if (nodeToExecutorDetailsMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }

        return nodeToExecutorDetailsMap;
    }

    private String getBestClustering() {
        String bestCluster = null;
        Double mostRes = 0.0;
        for (Entry<String, List<String>> cluster : this._clusterInfo
                .entrySet()) {
            Double clusterTotalRes = getTotalClusterRes(cluster.getValue());
            if (clusterTotalRes > mostRes) {
                mostRes = clusterTotalRes;
                bestCluster = cluster.getKey();
            }
        }
        return bestCluster;
    }

    private Double getTotalClusterRes(List<String> cluster) {
        Double res = 0.0;
        for (String node : cluster) {
            LOG.info("node: {}", node);
            res += this._nodes.get(this.NodeHostnameToId(node))
                    .getAvailableMemoryResources()
                    + this._nodes.get(this.NodeHostnameToId(node))
                    .getAvailableCpuResources();
        }
        return res;
    }

    private Node scheduleNodeForAnExecutorDetail(ExecutorDetails exec) {
        // first scheduling
        Node n = null;
        if (this.refNode == null) {
            String clus = this.getBestClustering();
            n = this.getBestNodeInCluster_Mem_CPU(clus, exec);
            this.refNode = n;
        } else {
            n = this.getBestNode(exec);
            if(n != null) {
            	this.refNode = n; // update the refnode every time
            }
        }

        LOG.debug("reference node for the resource aware scheduler is: {}", this.refNode);
        return n;
    }

    private Node getBestNode(ExecutorDetails exec) {
        Double taskMem = _topo.getTotalMemReqTask(exec);
        Double taskCPU = _topo.getTotalCpuReqTask(exec);

        Double shortestDistance = Double.POSITIVE_INFINITY;
        Node closestNode = null;
        for (Node n : this._availNodes) {
            // hard constraint
            if (n.getAvailableMemoryResources() >= taskMem
                    && n.getAvailableCpuResources() >= taskCPU) {
                Double a = Math.pow((taskCPU - n.getAvailableCpuResources())
                        * this.CPU_WEIGHT, 2);
                Double b = Math.pow((taskMem - n.getAvailableMemoryResources())
                        * this.MEM_WEIGHT, 2);
                Double c = Math.pow(this.distToNode(this.refNode, n)
                        * this.NETWORK_WEIGHT, 2);
                Double distance = Math.sqrt(a + b + c);
                if (shortestDistance > distance) {
                    shortestDistance = distance;
                    closestNode = n;
                }
            }
        }
        return closestNode;
    }

    Double distToNode(Node src, Node dest) {
        if (this.NodeToCluster(src) == this.NodeToCluster(dest)) {
            return 1.0;
        } else {
            return 2.0;
        }
    }

    private String NodeToCluster(Node node) {
        for (Entry<String, List<String>> entry : this._clusterInfo
                .entrySet()) {
            if (entry.getValue().contains(node.hostname)) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any clusters", node.hostname);
        return null;
    }

    private List<Node> getNodesFromCluster(String clus) {
        List<Node> retList = new ArrayList<Node>();
        for (String node_id : this._clusterInfo.get(clus)) {
            retList.add(this._nodes.get(this
                    .NodeHostnameToId(node_id)));
        }
        return retList;
    }

    private Node getBestNodeInCluster_Mem_CPU(String clus, ExecutorDetails exec) {
        Double taskMem = _topo.getTotalMemReqTask(exec);
        Double taskCPU = _topo.getTotalCpuReqTask(exec);

        Collection<Node> NodeMap = this.getNodesFromCluster(clus);
        Double shortestDistance = Double.POSITIVE_INFINITY;
        String msg = "";
        LOG.info("exec: {} taskMem: {} taskCPU: {}", exec, taskMem, taskCPU);
        Node closestNode = null;
        LOG.info("NodeMap.size: {}", NodeMap.size());
        LOG.info("NodeMap: {}", NodeMap);
        for (Node n : NodeMap) {
            if (n.getAvailableMemoryResources() >= taskMem
                    && n.getAvailableCpuResources() >= taskCPU
                    && n.totalSlotsFree() > 0) {
                Double distance = Math
                        .sqrt(Math.pow(
                                (taskMem - n.getAvailableMemoryResources()), 2)
                                + Math.pow(
                                (taskCPU - n.getAvailableCpuResources()),
                                2));
                msg = msg + "{" + n.getId() + "-" + distance.toString() + "}";
                if (distance < shortestDistance) {
                    closestNode = n;
                    shortestDistance = distance;
                }
            }
        }
        if (closestNode != null) {
            LOG.info(msg);
            LOG.info("node: {} distance: {}", closestNode, shortestDistance);
            LOG.info("node availMem: {}",
                    closestNode.getAvailableMemoryResources());
            LOG.info("node availCPU: {}",
                    closestNode.getAvailableCpuResources());
        }
        return closestNode;
    }

    protected Collection<Node> getAvaiNodes() {
        return this._nodes.values();
    }

    private Queue<RAS_Component> bfs(Topologies topologies, List<RAS_Component> spouts) {
        // Since queue is a interface
        Queue<RAS_Component> ordered_RAS_Component_list = new LinkedList<RAS_Component>();
        HashMap<String, RAS_Component> visited = new HashMap<String, RAS_Component>();

        /* start from each spout that is not visited, each does a breadth-first traverse */
        for (RAS_Component spout : spouts) {
            if (!visited.containsKey(spout.id)) {
                Queue<RAS_Component> queue = new LinkedList<RAS_Component>();
                queue.offer(spout);
                while (!queue.isEmpty()) {
                    RAS_Component comp = queue.poll();
                    visited.put(comp.id, comp);
                    ordered_RAS_Component_list.add(comp);
                    List<String> neighbors = new ArrayList<String>();
                    neighbors.addAll(comp.children);
                    neighbors.addAll(comp.parents);
                    for (String nbID : neighbors) {
                        if (!visited.containsKey(nbID)) {
                            RAS_Component child = topologies.getAllRAS_Components().get(_topo.getId()).get(nbID);
                            queue.offer(child);
                        }
                    }
                }
            }
        }
        return ordered_RAS_Component_list;
    }

    private List<RAS_Component> getSpouts(Topologies topologies) {
        List<RAS_Component> spouts = new ArrayList<RAS_Component>();
        for (RAS_Component c : topologies.getAllRAS_Components().get(_topo.getId())
                .values()) {
            if (c.type == RAS_Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    private String NodeHostnameToId(String hostname) {
        for (Node n : this._nodes.values()) {
            if (n.hostname == null) {
                continue;
            }
            if (n.hostname.equals(hostname)) {
                return n.supervisor_id;
            }
        }
        LOG.error("Cannot find Node with hostname {}", hostname);
        return null;
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
}
