package backtype.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.TopologyDetails;

/**
 * All of the machines that currently have nothing assigned to them
 */
public class FreePool extends NodePool {
  private static final Logger LOG = LoggerFactory.getLogger(FreePool.class);
  private Set<Node> _nodes = new HashSet<Node>();
  private int _totalSlots = 0;

  @Override
  public void init(Cluster cluster, Map<String, Node> nodeIdToNode) {
    for (Node n: nodeIdToNode.values()) {
      if(n.isTotallyFree() && n.isAlive()) {
        _nodes.add(n);
        _totalSlots += n.totalSlotsFree();
      }
    }
    LOG.debug("Found {} nodes with {} slots", _nodes.size(), _totalSlots);
  }
  
  @Override
  public void addTopology(TopologyDetails td) {
    throw new IllegalArgumentException("The free pool cannot run any topologies");
  }

  @Override
  public boolean canAdd(TopologyDetails td) {
    // The free pool never has anything running
    return false;
  }
  
  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    HashSet<Node> ret = new HashSet<Node>();
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && nodesNeeded > ret.size()) {
      Node n = it.next();
      ret.add(n);
      _totalSlots -= n.totalSlotsFree();
      it.remove();
    }
    return ret;
  }
  
  @Override
  public int nodesAvailable() {
    return _nodes.size();
  }

  @Override
  public int slotsAvailable() {
    return _totalSlots;
  }

  @Override
  public Collection<Node> takeNodesBySlots(int slotsNeeded) {
    HashSet<Node> ret = new HashSet<Node>();
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && slotsNeeded > 0) {
      Node n = it.next();
      ret.add(n);
      _totalSlots -= n.totalSlotsFree();
      slotsNeeded -= n.totalSlotsFree();
      it.remove();
    }
    return ret;
  }
  
  @Override
  public NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slotsNeeded) {
    int slotsFound = 0;
    int nodesFound = 0;
    Iterator<Node> it = _nodes.iterator();
    while (it.hasNext() && slotsNeeded > 0) {
      Node n = it.next();
      nodesFound++;
      int totalSlots = n.totalSlots();
      slotsFound += totalSlots;
      slotsNeeded -= totalSlots;
    }
    return new NodeAndSlotCounts(nodesFound, slotsFound);
  }

  @Override
  public void scheduleAsNeeded(NodePool... lesserPools) {
    //No topologies running so NOOP
  }
  
  @Override
  public String toString() {
    return "FreePool of "+_nodes.size()+" nodes with "+_totalSlots+" slots";
  }
}