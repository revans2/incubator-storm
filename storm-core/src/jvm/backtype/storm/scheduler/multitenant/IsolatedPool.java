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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * A pool of machines that can be used to run isolated topologies
 */
public class IsolatedPool extends NodePool {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedPool.class);
  private Map<String, Set<Node>> _topologyIdToNodes = new HashMap<>();
  private HashMap<String, TopologyDetails> _tds = new HashMap<>();
  private HashSet<String> _isolated = new HashSet<>();
  private int _maxNodes;
  private int _usedNodes;

  public IsolatedPool(int maxNodes) {
    _maxNodes = maxNodes;
    _usedNodes = 0;
  }

  @Override
  public void addTopology(TopologyDetails td) {
    String topId = td.getId();
    LOG.debug("Adding in Topology {}", topId);
    SchedulerAssignment assignment = _cluster.getAssignmentById(topId);
    Set<Node> assignedNodes = new HashSet<>();
    if (assignment != null) {
      for (WorkerSlot ws: assignment.getSlots()) {
        Node n = _nodeIdToNode.get(ws.getNodeId());
        assignedNodes.add(n);
      }
    }
    _usedNodes += assignedNodes.size();
    _topologyIdToNodes.put(topId, assignedNodes);
    _tds.put(topId, td);
    if (td.getConf().get(Config.TOPOLOGY_ISOLATED_MACHINES) != null) {
      _isolated.add(topId);
    }
  }

  @Override
  public boolean canAdd(TopologyDetails td) {
    //Only add topologies that are not sharing nodes with other topologies
    String topId = td.getId();
    SchedulerAssignment assignment = _cluster.getAssignmentById(topId);
    if (assignment != null) {
      for (WorkerSlot ws: assignment.getSlots()) {
        Node n = _nodeIdToNode.get(ws.getNodeId());
        if (n.getRunningTopologies().size() > 1) {
          return false;
        }
      }
    }
    return true;
  }
  
  @Override
  public void scheduleAsNeeded(NodePool ... lesserPools) {
    LOG.debug("Existing Assignments PRIOR to new schedulings:\n{}", this.getNodeToTopology());

    for (String topId : _topologyIdToNodes.keySet()) {
      TopologyDetails td = _tds.get(topId);
      LOG.debug("Scheduling topology {} unassigned executors: {}", td.getName(), _cluster.getUnassignedExecutors(td));
      Set<Node> allNodes = _topologyIdToNodes.get(topId);
      LOG.debug("allNodes: {}", Node.getNodesDebugInfo(allNodes));

      Number nodesRequested = (Number) td.getConf().get(Config.TOPOLOGY_ISOLATED_MACHINES);
      LOG.debug("isolated nodesRequested: {}", nodesRequested);
      Integer effectiveNodesRequested = null;
      if (nodesRequested != null) {
        effectiveNodesRequested = Math.min(td.getExecutors().size(),
            nodesRequested.intValue());
      }
      LOG.debug(" effectiveNodesRequested: {}", effectiveNodesRequested);
      if (_cluster.needsScheduling(td) ||
          (effectiveNodesRequested != null &&
              allNodes.size() != effectiveNodesRequested)) {
        int slotsToUse = 0;
        if (effectiveNodesRequested == null) {
          slotsToUse = getNodesForNotIsolatedTop(td, allNodes, lesserPools);
        } else {
          slotsToUse = getNodesForIsolatedTop(td, allNodes, lesserPools,
                  effectiveNodesRequested);
        }
        LOG.debug("slotsToUse: {}", slotsToUse);
        //No slots to schedule for some reason, so skip it.
        if (slotsToUse <= 0) {
          continue;
        }
        if (td.getConf().get(Config.TOPOLOGY_CONSTRAINTS) != null) {
          LOG.debug("/* Using Constraint Scheduler */");
          ConstraintSolverForMultitenant csm = new ConstraintSolverForMultitenant(td, slotsToUse, allNodes);
          Map<ExecutorDetails, WorkerSlot> execToWorkerScheduling = csm.findScheduling();
          if (execToWorkerScheduling != null) {
            Map<WorkerSlot, List<ExecutorDetails>> workerToExecsScheduling = new HashMap<WorkerSlot, List<ExecutorDetails>>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : execToWorkerScheduling.entrySet()) {
              WorkerSlot slot = entry.getValue();
              ExecutorDetails exec = entry.getKey();
              if (!workerToExecsScheduling.containsKey(slot)) {
                workerToExecsScheduling.put(slot, new LinkedList<ExecutorDetails>());
              }
              workerToExecsScheduling.get(slot).add(exec);
            }
            for (Entry<WorkerSlot, List<ExecutorDetails>> entry : workerToExecsScheduling.entrySet()) {
              WorkerSlot slot = entry.getKey();
              List<ExecutorDetails> execs = entry.getValue();
              _nodeIdToNode.get(slot.getNodeId()).assign(td.getId(), execs, _cluster);
            }
          } else {
            if(csm.getTraversalDepth() >= Utils.getInt(td.getConf().get(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL))) {
              LOG.debug("No valid scheduling found within MAX_DEPTH_TRAVERSE: {}", Utils.getInt(td.getConf().get(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL)));
              _cluster.setStatus(topId, "No Scheduling found that satisfy all constraints within the max traversal depth");
            } else if (csm.getRecursionDepth() >= ConstraintSolverForMultitenant.MAX_RECURSIVE_DEPTH) {
              LOG.debug("No valid Scheduling found that satisfy all constraints");
              _cluster.setStatus(topId, "No Scheduling found that satisfy all constraints with in the max recursion depth");
            } else {
              LOG.debug("No Scheduling Exists that satisfy all constraints");
              _cluster.setStatus(topId, "No Scheduling exists that satisfy all constraints");
            }
            continue;
          }
        } else {
          RoundRobinSlotScheduler slotSched =
                  new RoundRobinSlotScheduler(td, slotsToUse, _cluster);
          while (true) {
            LOG.debug("Nodes sorted by free space {}", Node.getNodesDebugInfo(allNodes));
            Node n = findNodeWithMostFreeSlots(allNodes);
            if (n == null) {
              LOG.error("No nodes to use to assign topology {}", td.getName());
              break;
            }
            if (!slotSched.assignSlotTo(n)) {
              break;
            }
          }
        }
      }
      Set<Node> found = _topologyIdToNodes.get(topId);
      int nc = found == null ? 0 : found.size();
      _cluster.setStatus(topId,"Scheduled Isolated on "+nc+" Nodes");
    }
  }

  private Node findNodeWithMostFreeSlots(Collection<Node> nodes) {
    Node ret = null;
    for(Node node : nodes) {
      if(ret == null ) {
        if(node.totalSlotsFree() > 0) {
          ret = node;
        }
      } else {
        if (node.totalSlotsFree() > 0) {
          if (node.totalSlotsUsed() < ret.totalSlotsUsed()) {
            ret = node;
          } else if (node.totalSlotsUsed() == ret.totalSlotsUsed()) {
            if(node.totalSlotsFree() > ret.totalSlotsFree()) {
              ret = node;
            }
          }
        }
      }
    }
    return ret;
  }
  
  /**
   * Get the nodes needed to schedule an isolated topology.
   * @param td the topology to be scheduled
   * @param allNodes the nodes already scheduled for this topology.
   * This will be updated to include new nodes if needed. 
   * @param lesserPools node pools we can steal nodes from
   * @return the number of additional slots that should be used for scheduling.
   */
  private int getNodesForIsolatedTop(TopologyDetails td, Set<Node> allNodes,
      NodePool[] lesserPools, int nodesRequested) {
    String topId = td.getId();
    LOG.debug("Topology {} is isolated", topId);
    int nodesFromUsAvailable = nodesAvailable();
    int nodesFromOthersAvailable = NodePool.nodesAvailable(lesserPools);

    int nodesUsed = _topologyIdToNodes.get(topId).size();
    int nodesNeeded = nodesRequested - nodesUsed;
    LOG.debug("Nodes... requested {} used {} available from us {} " +
        "avail from other {} needed {}", nodesRequested,
        nodesUsed, nodesFromUsAvailable, nodesFromOthersAvailable,
        nodesNeeded);
    if ((nodesNeeded - nodesFromUsAvailable) > (_maxNodes - _usedNodes)) {
      LOG.debug("Max Nodes(" + _maxNodes + ") for this user would be exceeded. "
              + ((nodesNeeded - nodesFromUsAvailable) - (_maxNodes - _usedNodes))
              + " more nodes needed to run topology.");
      _cluster.setStatus(topId,"Max Nodes("+_maxNodes+") for this user would be exceeded. "
        + ((nodesNeeded - nodesFromUsAvailable) - (_maxNodes - _usedNodes)) 
        + " more nodes needed to run topology.");
      return 0;
    }

    //In order to avoid going over _maxNodes I may need to steal from
    // myself even though other pools have free nodes. so figure out how
    // much each group should provide
    int nodesNeededFromOthers = Math.min(Math.min(_maxNodes - _usedNodes, 
        nodesFromOthersAvailable), nodesNeeded);
    int nodesNeededFromUs = nodesNeeded - nodesNeededFromOthers; 
    LOG.debug("Nodes... needed from us {} needed from others {}",
        nodesNeededFromUs, nodesNeededFromOthers);

    if (nodesNeededFromUs > nodesFromUsAvailable) {
      LOG.debug("Not Enough Nodes Available to Schedule Topology");
      _cluster.setStatus(topId, "Not Enough Nodes Available to Schedule Topology");
      return 0;
    }

    //Get the nodes
    Collection<Node> found = NodePool.takeNodes(nodesNeededFromOthers, lesserPools);
    LOG.debug("nodes taken from others: {}", Node.getNodesDebugInfo(found));
    _usedNodes += found.size();
    allNodes.addAll(found);
    Collection<Node> foundMore = takeNodes(nodesNeededFromUs);
    LOG.debug("nodes taken from others: {}", Node.getNodesDebugInfo(foundMore));
    _usedNodes += foundMore.size();
    allNodes.addAll(foundMore);

    int totalTasks = td.getExecutors().size();
    int origRequest = td.getNumWorkers();
    int slotsRequested = Math.min(totalTasks, origRequest);
    int slotsUsed = Node.countSlotsUsed(allNodes);
    int slotsFree = Node.countFreeSlotsAlive(allNodes);
    int slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree);
    if (slotsToUse <= 0) {
      if (origRequest > slotsUsed) {
        _cluster.setStatus(topId, "Running with fewer slots than requested " + slotsUsed + "/"
                + origRequest + " on " + allNodes.size() + " with " + (slotsUsed + slotsFree) + " total slots");
      } else {
        _cluster.setStatus(topId, "Node has partially crashed, if this situation persists rebalance the topology.");
      }
    }
    return slotsToUse;
  }
  
  /**
   * Get the nodes needed to schedule a non-isolated topology.
   * @param td the topology to be scheduled
   * @param allNodes the nodes already scheduled for this topology.
   * This will be updated to include new nodes if needed. 
   * @param lesserPools node pools we can steal nodes from
   * @return the number of additional slots that should be used for scheduling.
   */
  private int getNodesForNotIsolatedTop(TopologyDetails td, Set<Node> allNodes,
      NodePool[] lesserPools) {
    String topId = td.getId();
    LOG.debug("Topology {} is not isolated",topId);
    int totalTasks = td.getExecutors().size();
    int origRequest = td.getNumWorkers();
    int slotsRequested = Math.min(totalTasks, origRequest);
    int slotsUsed = Node.countSlotsUsed(topId, allNodes);
    int slotsFree = Node.countFreeSlotsAlive(allNodes);
    //Check to see if we have enough slots before trying to get them
    int slotsAvailable = 0;
    if (slotsRequested > slotsFree) {
      slotsAvailable = NodePool.slotsAvailable(lesserPools);
    }
    int slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree + slotsAvailable);
    LOG.debug("Slots... requested {} used {} free {} available {} to be used {}",
            slotsRequested, slotsUsed, slotsFree, slotsAvailable, slotsToUse);
    if (slotsToUse <= 0) {
      _cluster.setStatus(topId, "Not Enough Slots Available to Schedule Topology");
      return 0;
    }
    int slotsNeeded = slotsToUse - slotsFree;
    int numNewNodes = NodePool.getNodeCountIfSlotsWereTaken(slotsNeeded, lesserPools);
    LOG.debug("Nodes... new {} used {} max {}",
            numNewNodes, _usedNodes, _maxNodes);
    if ((numNewNodes + _usedNodes) > _maxNodes) {
      _cluster.setStatus(topId,"Max Nodes("+_maxNodes+") for this user would be exceeded. " +
      (numNewNodes - (_maxNodes - _usedNodes)) + " more nodes needed to run topology.");
      return 0;
    }
    
    Collection<Node> found = NodePool.takeNodesBySlot(slotsNeeded, lesserPools);
    LOG.debug("nodes taken from others: {}", Node.getNodesDebugInfo(found));
    _usedNodes += found.size();
    allNodes.addAll(found);
    return slotsToUse;
  }

  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    LOG.debug("Taking {} from {}", nodesNeeded, this);
    HashSet<Node> ret = new HashSet<>();
    for (Entry<String, Set<Node>> entry: _topologyIdToNodes.entrySet()) {
      if (!_isolated.contains(entry.getKey())) {
        Iterator<Node> it = entry.getValue().iterator();
        while (it.hasNext()) {
          if (nodesNeeded <= 0) {
            return ret;
          }
          Node n = it.next();
          it.remove();
          n.freeAllSlots(_cluster);
          ret.add(n);
          nodesNeeded--;
          _usedNodes--;
        }
      }
    }
    return ret;
  }
  
  @Override
  public int nodesAvailable() {
    int total = 0;
    for (Entry<String, Set<Node>> entry: _topologyIdToNodes.entrySet()) {
      if (!_isolated.contains(entry.getKey())) {
        total += entry.getValue().size();
      }
    }
    return total;
  }
  
  @Override
  public int slotsAvailable() {
    int total = 0;
    for (Entry<String, Set<Node>> entry: _topologyIdToNodes.entrySet()) {
      if (!_isolated.contains(entry.getKey())) {
        total += Node.countTotalSlotsAlive(entry.getValue());
      }
    }
    return total;
  }

  @Override
  public Collection<Node> takeNodesBySlots(int slotsNeeded) {
    HashSet<Node> ret = new HashSet<>();
    for (Entry<String, Set<Node>> entry: _topologyIdToNodes.entrySet()) {
      if (!_isolated.contains(entry.getKey())) {
        Iterator<Node> it = entry.getValue().iterator();
        while (it.hasNext()) {
          Node n = it.next();
          if (n.isAlive()) {
            it.remove();
            _usedNodes--;
            n.freeAllSlots(_cluster);
            ret.add(n);
            slotsNeeded -= n.totalSlots();
            if (slotsNeeded <= 0) {
              return ret;
            }
          }
        }
      }
    }
    return ret;
  }
  
  @Override
  public NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slotsNeeded) {
    int nodesFound = 0;
    int slotsFound = 0;
    for (Entry<String, Set<Node>> entry: _topologyIdToNodes.entrySet()) {
      if (!_isolated.contains(entry.getKey())) {
        for (Node n : entry.getValue()) {
          if (n.isAlive()) {
            nodesFound++;
            int totalSlotsFree = n.totalSlots();
            slotsFound += totalSlotsFree;
            slotsNeeded -= totalSlotsFree;
            if (slotsNeeded <= 0) {
              return new NodeAndSlotCounts(nodesFound, slotsFound);
            }
          }
        }
      }
    }
    return new NodeAndSlotCounts(nodesFound, slotsFound);
  }
  
  @Override
  public String toString() {
    return "IsolatedPool... ";
  }
  
  /**
   * for debugging
   */
  public void printInfo() {
      LOG.debug("_isolated: {}", this._isolated);
      LOG.debug("_topologyIdToNodes: {}", this._topologyIdToNodes);
      LOG.debug("_tds: {}", this._tds);
      LOG.debug("_usedNodes: {}", this._usedNodes);
  }

  public Map<Node, List<String>> getNodeToTopology() {
    Map<Node, List<String>> nodeToTopoMapping = new HashMap<Node, List<String>>();
    for (Entry<String, Set<Node>> entry : this._topologyIdToNodes.entrySet()) {
      String topoId = entry.getKey();
      Set<Node> nodes = entry.getValue();
      for (Node node : nodes) {
        if (!nodeToTopoMapping.containsKey(node)) {
          nodeToTopoMapping.put(node, new LinkedList<String>());
        }
        nodeToTopoMapping.get(node).add(topoId);
        if (nodeToTopoMapping.get(node).size() > 1) {
          LOG.error("More than one Topology marked as isolated assigned to Node: {}", node);
        }
      }
    }
    return nodeToTopoMapping;
  }
}
