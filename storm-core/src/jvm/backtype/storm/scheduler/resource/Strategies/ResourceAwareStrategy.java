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

package backtype.storm.scheduler.resource.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import backtype.storm.scheduler.resource.GetNetworkTopography;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.RAS_Component;
import backtype.storm.scheduler.resource.Node;
import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class ResourceAwareStrategy implements IStrategy {
    protected Logger LOG = null;
    protected Cluster _cluster;
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
        this._cluster = cluster;
        this._topologies = topologies;
        this._topo = topo;
        this._nodes = Node.getAllNodesFrom(cluster, _topologies);
        this._availNodes = this.getAvaiNodes();
        this._clusterInfo = (new GetNetworkTopography()).getClusterInfo();

        this.LOG = LoggerFactory.getLogger(this.getClass());
    }

    protected void prepare() {
        LOG.info("Clustering info: {}", this._clusterInfo);
    }

    //the returned TreeMap keeps the RAS_Components sorted
    protected TreeMap<Integer, List<ExecutorDetails>> getPriorityToExecutorDetailsListMap(
            Queue<RAS_Component> ordered_RAS_Component_list) {
        TreeMap<Integer, List<ExecutorDetails>> retMap = new TreeMap<Integer, List<ExecutorDetails>>();
        Integer rank = 0;
        for (RAS_Component ras_comp : ordered_RAS_Component_list) {
            retMap.put(rank, new ArrayList<ExecutorDetails>());
            retMap.get(rank).addAll(ras_comp.execs);
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
        RAS_Component rootSpout = this.getSpout(topologies);
        if (rootSpout == null) {
            LOG.error("Cannot find a Spout!");
            return null;
        }
        Queue<RAS_Component> ordered_RAS_Component_list = bfs(topologies, rootSpout);
        TreeMap<Integer, List<ExecutorDetails>> taskPriorityMap = getPriorityToExecutorDetailsListMap(ordered_RAS_Component_list);

        for (Integer priority : taskPriorityMap.keySet()) {
            for (ExecutorDetails detail : taskPriorityMap.get(priority)) {
                LOG.info("\n\nAttempting to schedule: {} of component {} with rank {}",
                        detail, _topo.getExecutorToComponent().get(detail), priority);
                Node scheduledNode = scheduleNodeForAnExecutorDetail(detail);
                if (scheduledNode != null) {
                    if (!nodeToExecutorDetailsMap.containsKey(scheduledNode)) {
                        Collection<ExecutorDetails> newExecutorDetailsMap = new LinkedList<ExecutorDetails>();
                        nodeToExecutorDetailsMap.put(scheduledNode, newExecutorDetailsMap);
                    }
                    /* TODO: should newExecutorDetailsMap be named as nodeExecutorDetailsList? Also see newMap below */
                    nodeToExecutorDetailsMap.get(scheduledNode).add(detail);
                    scheduledNode.consumeResourcesforTask(detail, td);
                    scheduledTasks.add(detail);
                    LOG.info("TASK {} assigned to NODE {} -- AvailMem: {} AvailCPU: {}",
                            detail, scheduledNode, scheduledNode.getAvailableMemoryResources(),
                            scheduledNode.getAvailableCpuResources());
                } else {
                    LOG.error("Not Enough Resources to schedule Task {}", detail);
                }
            }
        }

        Collection<ExecutorDetails> tasksNotScheduled = new ArrayList<ExecutorDetails>(unassignedExecutors);
        tasksNotScheduled.removeAll(scheduledTasks);
        // schedule left over system tasks
        for (ExecutorDetails detail : tasksNotScheduled) {
            Node bestNodeForAnExecutorDetail = this.getBestNode(detail);
            if (bestNodeForAnExecutorDetail != null) {
                if (nodeToExecutorDetailsMap.containsKey(bestNodeForAnExecutorDetail) == false) {
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

        tasksNotScheduled = new ArrayList<ExecutorDetails>(unassignedExecutors);
        tasksNotScheduled.removeAll(scheduledTasks);
        if (tasksNotScheduled.size() > 0) {
            LOG.error("Resources not successfully scheduled: {}",
                    tasksNotScheduled);
            nodeToExecutorDetailsMap = null;
        } else {
            LOG.debug("All resources successfully scheduled!");
        }

        if (nodeToExecutorDetailsMap == null) {
            LOG.error("Topology {} not successfully scheduled!", td.getId());
        }

        return nodeToExecutorDetailsMap;
    }

    /* TODO: should cluster be renamed as? e.g., rack */
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
                    /* TODO: Here fixing an important bug */
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
            LOG.debug("refNode: {}", this.refNode.hostname);
        } else {
            n = this.getBestNode(exec);
            /* TODO: should we do "this.refnode = n" here? I think so */
        }

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
                /* TODO: network distance is either 1 or 2, this makes network weight marginal compared to CPU/mem */
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

        /* TODO rename NodeMap to nodeList */
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

    private Queue<RAS_Component> bfs(Topologies topologies, RAS_Component root) {
        // Since queue is a interface
        Queue<RAS_Component> ordered_RAS_Component_list = new LinkedList<RAS_Component>();
        HashMap<String, RAS_Component> visited = new HashMap<String, RAS_Component>();
        Queue<RAS_Component> temp_queue = new LinkedList<RAS_Component>();

        if (root == null)
            return null;

        // Adds to end of queue
        temp_queue.add(root);
        visited.put(root.id, root);
        ordered_RAS_Component_list.add(root);

        while (!temp_queue.isEmpty()) {
            // removes from front of queue
            RAS_Component topOfQueueRAS_Component = temp_queue.remove();

            // Visit child first before grandchild
            List<String> neighbors = new ArrayList<String>();
            neighbors.addAll(topOfQueueRAS_Component.children);
            neighbors.addAll(topOfQueueRAS_Component.parents);
            for (String strRAS_ComponentID : neighbors) {
                if (!visited.containsKey(strRAS_ComponentID)) {
                    RAS_Component child = topologies.getAllRAS_Components().get(_topo.getId()).get(strRAS_ComponentID);
                    temp_queue.add(child);
                    visited.put(child.id, child);
                    ordered_RAS_Component_list.add(child);
                }
            }
        }
        return ordered_RAS_Component_list;
    }

    private RAS_Component getSpout(Topologies topologies) {
        for (RAS_Component c : topologies.getAllRAS_Components().get(_topo.getId())
                .values()) {
            if (c.type == RAS_Component.ComponentType.SPOUT) {
                return c;
            }
        }
        return null;
    }

    private String NodeHostnameToId(String hostname) {
        for (Node n : this._nodes.values()) {
            if (n.hostname.equals(hostname)) {
                return n.supervisor_id;
            }
        }
        LOG.error("Cannot find Node with hostname {}", hostname);
        return null;
    }

}
