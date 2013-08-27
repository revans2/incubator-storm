package backtype.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.TopologyDetails;

/**
 * A pool of nodes that can be used to run topologies.
 */
public abstract class NodePool {
  public static class NodeAndSlotCounts {
    public final int _nodes;
    public final int _slots;
    
    public NodeAndSlotCounts(int nodes, int slots) {
      _nodes = nodes;
      _slots = slots;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(NodePool.class);
  /**
   * Initialize the pool.
   * @param cluster the cluster
   * @param nodeIdToNode the mapping of node id to nodes
   */
  public abstract void init(Cluster cluster, Map<String, Node> nodeIdToNode);
  
  /**
   * Add a topology to the pool
   * @param td the topology to add.
   */
  public abstract void addTopology(TopologyDetails td);
  
  /**
   * Check if this topology can be added to this pool
   * @param td the topology
   * @return true if it can else false
   */
  public abstract boolean canAdd(TopologyDetails td);
  
  /**
   * @return the number of nodes that are available to be taken
   */
  public abstract int slotsAvailable();
  
  /**
   * Take nodes from this pool that can fulfill possibly up to the
   * slotsNeeded
   * @param slotsNeeded the number of slots that are needed.
   * @return a Collection of nodes with the removed nodes in it.  
   * This may be empty, but should not be null.
   */
  public abstract Collection<Node> takeNodesBySlots(int slotsNeeded);

  /**
   * Get the number of nodes and slots this would provide to get the slots needed
   * @param slots the number of slots needed
   * @return the number of nodes and slots that would be returned.
   */
  public abstract NodeAndSlotCounts getNodeAndSlotCountIfSlotsWereTaken(int slots);
  
  /**
   * @return the number of nodes that are available to be taken
   */
  public abstract int nodesAvailable();
  
  /**
   * Take up to nodesNeeded from this pool
   * @param nodesNeeded the number of nodes that are needed.
   * @return a Collection of nodes with the removed nodes in it.  
   * This may be empty, but should not be null.
   */
  public abstract Collection<Node> takeNodes(int nodesNeeded);
  
  /**
   * Reschedule any topologies as needed.
   * @param lesserPools pools that may be used to steal nodes from.
   */
  public abstract void scheduleAsNeeded(NodePool ... lesserPools);
  
  public static int slotsAvailable(NodePool[] pools) {
    int slotsAvailable = 0;
    for (NodePool pool: pools) {
      slotsAvailable += pool.slotsAvailable();
    }
    return slotsAvailable;
  }
  
  public static int nodesAvailable(NodePool[] pools) {
    int nodesAvailable = 0;
    for (NodePool pool: pools) {
      nodesAvailable += pool.nodesAvailable();
    }
    return nodesAvailable;
  }
  
  public static Collection<Node> takeNodesBySlot(int slotsNeeded,NodePool[] pools) {
    LOG.debug("Trying to grab {} free slots from {}",slotsNeeded, pools);
    HashSet<Node> ret = new HashSet<Node>();
    for (NodePool pool: pools) {
      Collection<Node> got = pool.takeNodesBySlots(slotsNeeded);
      ret.addAll(got);
      slotsNeeded -= Node.countFreeSlotsAlive(got);
      LOG.debug("Got {} nodes so far need {} more slots",ret.size(),slotsNeeded);
      if (slotsNeeded <= 0) {
        break;
      }
    }
    return ret;
  }
  
  public static Collection<Node> takeNodes(int nodesNeeded,NodePool[] pools) {
    LOG.debug("Trying to grab {} free nodes from {}",nodesNeeded, pools);
    HashSet<Node> ret = new HashSet<Node>();
    for (NodePool pool: pools) {
      Collection<Node> got = pool.takeNodes(nodesNeeded);
      ret.addAll(got);
      nodesNeeded -= got.size();
      LOG.debug("Got {} nodes so far need {} more nodes", ret.size(), nodesNeeded);
      if (nodesNeeded <= 0) {
        break;
      }
    }
    return ret;
  }

  public static int getNodeCountIfSlotsWereTaken(int slots,NodePool[] pools) {
    LOG.debug("How many nodes to get {} slots from {}",slots, pools);
    int total = 0;
    for (NodePool pool: pools) {
      NodeAndSlotCounts ns = pool.getNodeAndSlotCountIfSlotsWereTaken(slots);
      total += ns._nodes;
      slots -= ns._slots;
      LOG.debug("Found {} nodes so far {} more slots needed", total, slots);
      if (slots <= 0) {
        break;
      }
    }    
    return total;
  }
}