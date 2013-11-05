package backtype.storm.scheduler.multitenant;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * Represents a single node in the cluster.
 */
public class Node {
  private static final Logger LOG = LoggerFactory.getLogger(Node.class);
  private Map<String, Set<WorkerSlot>> _topIdToUsedSlots = new HashMap<String,Set<WorkerSlot>>();
  private Set<WorkerSlot> _freeSlots = new HashSet<WorkerSlot>();
  private final String _nodeId;
  private boolean _isAlive;
  
  public Node(String nodeId, Set<Integer> allPorts, boolean isAlive) {
    _nodeId = nodeId;
    _isAlive = isAlive;
    if (_isAlive && allPorts != null) {
      for (int port: allPorts) {
        _freeSlots.add(new WorkerSlot(_nodeId, port));
      }
    }
  }

  public String getId() {
    return _nodeId;
  }
  
  public boolean isAlive() {
    return _isAlive;
  }
  
  /**
   * @return a collection of the topology ids currently running on this node
   */
  public Collection<String> getRunningTopologies() {
    return _topIdToUsedSlots.keySet();
  }
  
  public boolean isTotallyFree() {
    return _topIdToUsedSlots.isEmpty();
  }
  
  public int totalSlotsFree() {
    return _freeSlots.size();
  }
  
  public int totalSlotsUsed() {
    int total = 0;
    for (Set<WorkerSlot> slots: _topIdToUsedSlots.values()) {
      total += slots.size();
    }
    return total;
  }
  
  public int totalSlots() {
    return totalSlotsFree() + totalSlotsUsed();
  }
  
  public int totalSlotsUsed(String topId) {
    int total = 0;
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots != null) {
      total = slots.size();
    }
    return total;
  }

  private void validateSlot(WorkerSlot ws) {
    if (!_nodeId.equals(ws.getNodeId())) {
      throw new IllegalArgumentException(
          "Trying to add a slot to the wrong node " + ws + 
          " is not a part of " + _nodeId);
    }
  }
  
  void assignInternal(WorkerSlot ws, String topId) {
    validateSlot(ws);
    if (!_freeSlots.remove(ws) && _isAlive) {
      //If the node is dead go ahead and add in a dead slot.  This is part of initialization
      throw new IllegalStateException("Assigning a slot that was not free " + ws);
    }
    Set<WorkerSlot> usedSlots = _topIdToUsedSlots.get(topId);
    if (usedSlots == null) {
      usedSlots = new HashSet<WorkerSlot>();
      _topIdToUsedSlots.put(topId, usedSlots);
    }
    usedSlots.add(ws);
  }
  
  /**
   * Free all slots on this node.  This will update the Cluster too.
   * @param cluster the cluster to be updated
   */
  public void freeAllSlots(Cluster cluster) {
    if (!_isAlive) {
      LOG.warn("Freeing all slots on a dead node {} ",_nodeId);
    } 
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      cluster.freeSlots(entry.getValue());
      if (_isAlive) {
        _freeSlots.addAll(entry.getValue());
      }
    }
    _topIdToUsedSlots = new HashMap<String,Set<WorkerSlot>>();
  }
  
  /**
   * Frees a single slot in this node
   * @param ws the slot to free
   * @param cluster the cluster to update
   */
  public void free(WorkerSlot ws, Cluster cluster) {
    if (_freeSlots.contains(ws)) return;
    for (Entry<String, Set<WorkerSlot>> entry : _topIdToUsedSlots.entrySet()) {
      Set<WorkerSlot> slots = entry.getValue();
      if (slots.remove(ws)) {
        cluster.freeSlot(ws);
        if (_isAlive) {
          _freeSlots.add(ws);
        }
        return;
      }
    }
    throw new IllegalArgumentException("Tried to free a slot that was not" +
    		" part of this node " + _nodeId);
  }
   
  /**
   * Frees all the slots for a topology.
   * @param topId the topology to free slots for
   * @param cluster the cluster to update
   */
  public void freeTopology(String topId, Cluster cluster) {
    Set<WorkerSlot> slots = _topIdToUsedSlots.get(topId);
    if (slots == null || slots.isEmpty()) return;
    LOG.warn("Freeing "+slots.size()+" slots on "+ _nodeId+" for top "+topId);
    for (WorkerSlot ws : slots) {
      cluster.freeSlot(ws);
      if (_isAlive) {
        _freeSlots.add(ws);
      }
    }
    _topIdToUsedSlots.remove(topId);
  }
 
  /**
   * Assign a free slot on the node to the following topology and executors.
   * This will update the cluster too.
   * @param topId the topology to assign a free slot to.
   * @param executors the executors to run in that slot.
   * @param cluster the cluster to be updated
   */
  public void assign(String topId, Collection<ExecutorDetails> executors, 
      Cluster cluster) {
    if (!_isAlive) {
      throw new IllegalStateException("Trying to adding to a dead node " + _nodeId);
    }
    if (_freeSlots.isEmpty()) {
      throw new IllegalStateException("Trying to assign to a full node " + _nodeId);
    }
    if (executors.size() == 0) {
      LOG.warn("Trying to assign nothing from " + topId + " to " + _nodeId + " (Ignored)");
    } else {
      WorkerSlot slot = _freeSlots.iterator().next();
      LOG.warn("assigning "+slot+" to "+topId);
      cluster.assign(slot, topId, executors);
      assignInternal(slot, topId);
    }
  }
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof Node) {
      return _nodeId.equals(((Node)other)._nodeId);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return _nodeId.hashCode();
  }
  
  @Override
  public String toString() {
    return "Node: " + _nodeId;
  }

  public static int countSlotsUsed(String topId, Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed(topId);
    }
    return total;
  }
  
  public static int countSlotsUsed(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      total += n.totalSlotsUsed();
    }
    return total;
  }
  
  public static int countFreeSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlotsFree();
      }
    }
    return total;
  }
  
  public static int countTotalSlotsAlive(Collection<Node> nodes) {
    int total = 0;
    for (Node n: nodes) {
      if (n.isAlive()) {
        total += n.totalSlots();
      }
    }
    return total;
  }
  
  public static Map<String, Node> getAllNodesFrom(Cluster cluster) {
    Map<String, Node> nodeIdToNode = new HashMap<String, Node>();
    for (SupervisorDetails sup : cluster.getSupervisors().values()) {
      //Node ID and supervisor ID are the same.
      String id = sup.getId();
      boolean isAlive = !cluster.isBlackListed(id);
      LOG.debug("Found a {} Node {} {}", 
          new Object[] {isAlive? "living":"dead", id, sup.getAllPorts()});
      nodeIdToNode.put(id, new Node(id, sup.getAllPorts(), isAlive));
    }
    
    for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
      String topId = entry.getValue().getTopologyId();
      for (WorkerSlot ws: entry.getValue().getSlots()) {
        String id = ws.getNodeId();
        Node node = nodeIdToNode.get(id);
        if (node == null) {
          LOG.debug("Found an assigned slot on a dead supervisor {}", ws);
          node = new Node(id, null, false);
          nodeIdToNode.put(id, node);
        }
        node.assignInternal(ws, topId);
      }
    }
    
    return nodeIdToNode;
  }
  
  /**
   * Used to sort a list of nodes so the node with the most free slots comes
   * first.
   */
  public static final Comparator<Node> FREE_NODE_COMPARATOR_DEC = new Comparator<Node>() {
    @Override
    public int compare(Node o1, Node o2) {
      return o2.totalSlotsFree() - o1.totalSlotsFree();
    }
  };
}
