package backtype.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * A pool of machines that anyone can use, but topologies are not isolated
 */
public class DefaultPool extends NodePool {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPool.class);
  private Set<Node> _nodes = new HashSet<Node>();
  private HashMap<String, TopologyDetails> _tds = new HashMap<String, TopologyDetails>();
  
  @Override
  public void addTopology(TopologyDetails td) {
    String topId = td.getId();
    LOG.debug("Adding in Topology {}", topId);
    _tds.put(topId, td);
    SchedulerAssignment assignment = _cluster.getAssignmentById(topId);
    if (assignment != null) {
      for (WorkerSlot ws: assignment.getSlots()) {
        Node n = _nodeIdToNode.get(ws.getNodeId());
        _nodes.add(n);
      }
    }
  }

  @Override
  public boolean canAdd(TopologyDetails td) {
    return true;
  }

  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    HashSet<Node> ret = new HashSet<Node>();
    LinkedList<Node> sortedNodes = new LinkedList<Node>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (nodesNeeded <= ret.size()) {
        break;
      }
      if (n.isAlive()) {
        n.freeAllSlots(_cluster);
        _nodes.remove(n);
        ret.add(n);
      }
    }
    return ret;
  }
  
  @Override
  public int nodesAvailable() {
    int total = 0;
    for (Node n: _nodes) {
      if (n.isAlive()) total++;
    }
    return total;
  }
  
  @Override
  public int slotsAvailable() {
    return Node.countTotalSlotsAlive(_nodes);
  }

  @Override
  public NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slotsNeeded) {
    int nodesFound = 0;
    int slotsFound = 0;
    LinkedList<Node> sortedNodes = new LinkedList<Node>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (slotsNeeded <= 0) {
        break;
      }
      if (n.isAlive()) {
        nodesFound++;
        int totalSlotsFree = n.totalSlots();
        slotsFound += totalSlotsFree;
        slotsNeeded -= totalSlotsFree;
      }
    }
    return new NodeAndSlotCounts(nodesFound, slotsFound);
  }
  
  @Override
  public Collection<Node> takeNodesBySlots(int slotsNeeded) {
    HashSet<Node> ret = new HashSet<Node>();
    LinkedList<Node> sortedNodes = new LinkedList<Node>(_nodes);
    Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR_DEC);
    for (Node n: sortedNodes) {
      if (slotsNeeded <= 0) {
        break;
      }
      if (n.isAlive()) {
        n.freeAllSlots(_cluster);
        _nodes.remove(n);
        ret.add(n);
        slotsNeeded -= n.totalSlotsFree();
      }
    }
    return ret;
  }

  @Override
  public void scheduleAsNeeded(NodePool... lesserPools) {
    for (TopologyDetails td : _tds.values()) {
      if (_cluster.needsScheduling(td)) {
        String topId = td.getId();
        LOG.debug("Scheduling topology {}",topId);
        int slotsRequested = td.getNumWorkers();
        int slotsUsed = Node.countSlotsUsed(topId, _nodes);
        int slotsFree = Node.countFreeSlotsAlive(_nodes);
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
          continue;
        }

        int slotsNeeded = slotsToUse - slotsFree;
        _nodes.addAll(NodePool.takeNodesBySlot(slotsNeeded, lesserPools));
        
        RoundRobinSlotScheduler slotSched = 
          new RoundRobinSlotScheduler(td, slotsToUse, _cluster);
        
        LinkedList<Node> nodes = new LinkedList<Node>(_nodes);
        while (true) {
          Node n = null;
          do {
            if (nodes.isEmpty()) {
              throw new IllegalStateException("This should not happen, we" +
              " messed up and did not get enough slots");
            }
            n = nodes.peekFirst();
            if (n.totalSlotsFree() == 0) {
              nodes.remove();
              n = null;
            }
          } while (n == null);
          if (!slotSched.assignSlotTo(n)) {
            break;
          }
        }
      }
    }
  }
  
  @Override
  public String toString() {
    return "DefaultPool  " + _nodes.size() + " nodes " + _tds.size() + " topologies";
  }
}