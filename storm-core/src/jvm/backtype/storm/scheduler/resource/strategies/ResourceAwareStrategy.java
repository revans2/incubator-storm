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
import backtype.storm.scheduler.resource.RAS_Node;

public class ResourceAwareStrategy implements IStrategy {
    private Logger LOG = null;
    private Topologies _topologies;
    private Cluster _cluster;
    private Collection<RAS_Node> _availNodes;
    private RAS_Node refNode = null;
    /**
     * supervisor id -> Node
     */
    private Map<String, RAS_Node> _nodes;
    private Map<String, List<String>> _clusterInfo;

    private final double CPU_WEIGHT = 1.0;
    private final double MEM_WEIGHT = 1.0;
    private final double NETWORK_WEIGHT = 1.0;

    public ResourceAwareStrategy(Cluster cluster, Topologies topologies) {
        this._topologies = topologies;
        this._cluster = cluster;
        this._nodes = RAS_Node.getAllNodesFrom(cluster, _topologies);
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

    public Map<RAS_Node, Collection<ExecutorDetails>> schedule(TopologyDetails td) {
        if (this._availNodes.size() <= 0) {
            LOG.warn("No available nodes to schedule tasks on!");
            return null;
        }
        Collection<ExecutorDetails> unassignedExecutors = _cluster.getUnassignedExecutors(td);
        Map<RAS_Node, Collection<ExecutorDetails>> nodeToExecutorDetailsMap = new HashMap<RAS_Node, Collection<ExecutorDetails>>();
        LOG.debug("ExecutorsNeedScheduling: {}", unassignedExecutors);
        Collection<ExecutorDetails> scheduledTasks = new ArrayList<ExecutorDetails>();
        List<RAS_Component> spouts = this.getSpouts(this._topologies, td);

        if (spouts.size() == 0) {
            LOG.error("Cannot find a Spout!");
            return null;
        }

        Queue<RAS_Component> ordered_RAS_Component_list = bfs(this._topologies, td, spouts);

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
                            new Object[] { exec, td.getExecutorToComponent().get(exec),
                    td.getTaskResourceReqList(exec), entry.getKey() });
                    RAS_Node scheduledNode = this.scheduleNodeForAnExecutorDetail(exec, td);
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
            RAS_Node bestNodeForAnExecutorDetail = this.scheduleNodeForAnExecutorDetail(detail, td);
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

    private RAS_Node scheduleNodeForAnExecutorDetail(ExecutorDetails exec, TopologyDetails td) {
        // first scheduling
        RAS_Node n = null;
        if (this.refNode == null) {
            String clus = this.getBestClustering();
            n = this.getBestNodeInCluster_Mem_CPU(clus, exec, td);
            this.refNode = n;
        } else {
            n = this.getBestNode(exec, td);
            if(n != null) {
            	this.refNode = n; // update the refnode every time
            }
        }

        LOG.debug("reference node for the resource aware scheduler is: {}", this.refNode);
        return n;
    }

    private RAS_Node getBestNode(ExecutorDetails exec, TopologyDetails td) {
        Double taskMem = td.getTotalMemReqTask(exec);
        Double taskCPU = td.getTotalCpuReqTask(exec);

        Double shortestDistance = Double.POSITIVE_INFINITY;
        RAS_Node closestNode = null;
        for (RAS_Node n : this._availNodes) {
            if(n.totalSlotsFree() > 0) {
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
        }
        return closestNode;
    }

    Double distToNode(RAS_Node src, RAS_Node dest) {
        if (src.getId().equals(dest.getId())==true) {
            return 1.0;
        }else if (this.NodeToCluster(src) == this.NodeToCluster(dest)) {
            return 2.0;
        } else {
            return 3.0;
        }
    }

    private String NodeToCluster(RAS_Node node) {
        for (Entry<String, List<String>> entry : this._clusterInfo
                .entrySet()) {
            if (entry.getValue().contains(node.hostname)) {
                return entry.getKey();
            }
        }
        LOG.error("Node: {} not found in any clusters", node.hostname);
        return null;
    }

    private List<RAS_Node> getNodesFromCluster(String clus) {
        List<RAS_Node> retList = new ArrayList<RAS_Node>();
        for (String node_id : this._clusterInfo.get(clus)) {
            retList.add(this._nodes.get(this
                    .NodeHostnameToId(node_id)));
        }
        return retList;
    }

    private RAS_Node getBestNodeInCluster_Mem_CPU(String clus, ExecutorDetails exec, TopologyDetails td) {
        Double taskMem = td.getTotalMemReqTask(exec);
        Double taskCPU = td.getTotalCpuReqTask(exec);

        Collection<RAS_Node> NodeMap = this.getNodesFromCluster(clus);
        Double shortestDistance = Double.POSITIVE_INFINITY;
        String msg = "";
        LOG.info("exec: {} taskMem: {} taskCPU: {}", exec, taskMem, taskCPU);
        RAS_Node closestNode = null;
        LOG.info("NodeMap.size: {}", NodeMap.size());
        LOG.info("NodeMap: {}", NodeMap);
        for (RAS_Node n : NodeMap) {
            if(n.totalSlotsFree()>0) {
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

    protected Collection<RAS_Node> getAvaiNodes() {
        return this._nodes.values();
    }

    private Queue<RAS_Component> bfs(Topologies topologies, TopologyDetails td, List<RAS_Component> spouts) {
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
                            RAS_Component child = topologies.getAllRAS_Components().get(td.getId()).get(nbID);
                            queue.offer(child);
                        }
                    }
                }
            }
        }
        return ordered_RAS_Component_list;
    }

    private List<RAS_Component> getSpouts(Topologies topologies, TopologyDetails td) {
        List<RAS_Component> spouts = new ArrayList<RAS_Component>();
        for (RAS_Component c : topologies.getAllRAS_Components().get(td.getId())
                .values()) {
            if (c.type == RAS_Component.ComponentType.SPOUT) {
                spouts.add(c);
            }
        }
        return spouts;
    }

    private String NodeHostnameToId(String hostname) {
        for (RAS_Node n : this._nodes.values()) {
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
