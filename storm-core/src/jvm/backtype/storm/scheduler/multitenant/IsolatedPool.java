package backtype.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
  private Map<String, Set<Node>> _topologyIdToNodes = new HashMap<String, Set<Node>>();
  private HashMap<String, TopologyDetails> _tds = new HashMap<String, TopologyDetails>();
  private HashSet<String> _isolated = new HashSet<String>();
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
    Set<Node> assignedNodes = new HashSet<Node>();
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
    for (String topId : _topologyIdToNodes.keySet()) {
      TopologyDetails td = _tds.get(topId);
      if (_cluster.needsScheduling(td)) {
        LOG.debug("Scheduling topology {}",topId);
        Set<Node> allNodes = _topologyIdToNodes.get(topId);
        Number nodesRequested = (Number) td.getConf().get(Config.TOPOLOGY_ISOLATED_MACHINES);
        int slotsToUse = 0;
        if (nodesRequested == null) {
          slotsToUse = getNodesForNotIsolatedTop(td, allNodes, lesserPools);
        } else {
          slotsToUse = getNodesForIsolatedTop(td, allNodes, lesserPools, 
              nodesRequested.intValue());
        }
        //No slots to schedule for some reason, so skip it.
        if (slotsToUse <= 0) {
          continue;
        }
        
        RoundRobinSlotScheduler slotSched = 
          new RoundRobinSlotScheduler(td, slotsToUse, _cluster);
        
        LinkedList<Node> sortedNodes = new LinkedList<Node>(allNodes);
        Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);

        LOG.debug("Nodes sorted by free space {}", sortedNodes);
        while (true) {
          Node n = sortedNodes.remove();
          if (!slotSched.assignSlotTo(n)) {
            break;
          }
          int freeSlots = n.totalSlotsFree();
          for (int i = 0; i < sortedNodes.size(); i++) {
            if (freeSlots >= sortedNodes.get(i).totalSlotsFree()) {
              sortedNodes.add(i, n);
              n = null;
              break;
            }
          }
          if (n != null) {
            sortedNodes.add(n);
          }
        }
      }
    }
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
        "avail from other {} needed {}", new Object[] {nodesRequested, 
        nodesUsed, nodesFromUsAvailable, nodesFromOthersAvailable,
        nodesNeeded});
    if ((nodesNeeded - nodesFromUsAvailable) > (_maxNodes - _usedNodes)) {
      LOG.warn("Unable to schedule topology {} because it needs {} " +
          "more nodes which would exced the limit of {}",
          new Object[] {topId, nodesNeeded - nodesFromUsAvailable, _maxNodes});
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
      LOG.warn("There are not enough free nodes to schedule {}", topId);
      return 0;
    }

    //Get the nodes
    Collection<Node> found = NodePool.takeNodes(nodesNeededFromOthers, lesserPools);
    _usedNodes += found.size();
    allNodes.addAll(found);
    Collection<Node> foundMore = takeNodes(nodesNeededFromUs);
    _usedNodes += foundMore.size();
    allNodes.addAll(foundMore);

    int slotsRequested = td.getNumWorkers();
    int slotsUsed = Node.countSlotsUsed(allNodes);
    int slotsFree = Node.countFreeSlotsAlive(allNodes);
    int slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree);
    if (slotsToUse <= 0) {
      LOG.warn("An Isolated topology {} has had both a supervisor and" +
          " a worker crash, but not all workers. Let's hope it comes " +
          "back up soon.", topId);
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
    int slotsRequested = td.getNumWorkers();
    int slotsUsed = Node.countSlotsUsed(topId, allNodes);
    int slotsFree = Node.countFreeSlotsAlive(allNodes);
    //Check to see if we have enough slots before trying to get them
    int slotsAvailable = 0;
    if (slotsRequested > slotsFree) {
      slotsAvailable = NodePool.slotsAvailable(lesserPools);
    }
    int slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree + slotsAvailable);
    LOG.debug("Slots... requested {} used {} free {} available {} to be used {}", 
        new Object[] {slotsRequested, slotsUsed, slotsFree, slotsAvailable, slotsToUse});
    if (slotsToUse <= 0) {
      LOG.warn("The cluster appears to be full no slots left to schedule {}", topId);
      return 0;
    }
    int slotsNeeded = slotsToUse - slotsFree;
    int numNewNodes = NodePool.getNodeCountIfSlotsWereTaken(slotsNeeded, lesserPools);
    LOG.debug("Nodes... new {} used {} max {}",
        new Object[]{numNewNodes, _usedNodes, _maxNodes});
    if ((numNewNodes + _usedNodes) > _maxNodes) {
      LOG.warn("Unable to schedule topology {} because it needs {} " +
          "more nodes which exceed the limit of {}",
          new Object[] {topId, numNewNodes, _maxNodes});
      return 0;
    }
    
    Collection<Node> found = NodePool.takeNodesBySlot(slotsNeeded, lesserPools);
    _usedNodes += found.size();
    allNodes.addAll(found);
    return slotsToUse;
  }

  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    LOG.debug("Taking {} from {}", nodesNeeded, this);
    HashSet<Node> ret = new HashSet<Node>();
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
    HashSet<Node> ret = new HashSet<Node>();
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
        Iterator<Node> it = entry.getValue().iterator();
        while (it.hasNext()) {
          Node n = it.next();
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
}
