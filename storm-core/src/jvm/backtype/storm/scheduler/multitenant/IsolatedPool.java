package backtype.storm.scheduler.multitenant;

import java.util.ArrayList;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
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
  private Cluster _cluster;
  private Map<String, Node> _nodeIdToNode;

  public IsolatedPool(int maxNodes) {
    _maxNodes = maxNodes;
    _usedNodes = 0;
  }
  
  @Override
  public void init(Cluster cluster, Map<String, Node> nodeIdToNode) {
    LOG.info("Isolated pool initializing... max nodes: {}", _maxNodes);
    _cluster = cluster;
    _nodeIdToNode = nodeIdToNode;
  }

  @Override
  public void addTopology(TopologyDetails td) {
    String topId = td.getId();
    LOG.info("Adding in Topology {}", topId);
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
  
  public void scheduleAsNeeded(NodePool ... lesserPools) {
    for (String topId : _topologyIdToNodes.keySet()) {
      TopologyDetails td = _tds.get(topId);
      if (_cluster.needsScheduling(td)) {
        LOG.info("Scheduling topology {}",topId);
        Set<Node> allNodes = _topologyIdToNodes.get(topId);
        Number nodesRequested = (Number) td.getConf().get(Config.TOPOLOGY_ISOLATED_MACHINES);
        int slotsToUse = 0;
        if (nodesRequested == null) {
          LOG.info("Topology {} is not isolated",topId);
          int slotsRequested = td.getNumWorkers();
          int slotsUsed = Node.countSlotsUsed(topId, allNodes);
          int slotsFree = Node.countFreeSlotsAlive(allNodes);
          //Check to see if we have enough slots before trying to get them
          int slotsAvailable = 0;
          if (slotsRequested > slotsFree) {
            slotsAvailable = NodePool.slotsAvailable(lesserPools);
          }
          slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree + slotsAvailable);
          LOG.info("Slots... requested {} used {} free {} available {} to be used {}", 
              new Object[] {slotsRequested, slotsUsed, slotsFree, slotsAvailable, slotsToUse});
          if (slotsToUse <= 0) {
            LOG.warn("The cluster appears to be full no slots left to schedule {}", topId);
            //TODO it would be good to tag this topology some how
            continue;
          }
          int slotsNeeded = slotsToUse - slotsFree;
          int numNewNodes = NodePool.getNodeCountIfSlotsWereTaken(slotsNeeded, lesserPools);
          LOG.info("Nodes... new {} used {} max {}",
              new Object[]{numNewNodes, _usedNodes, _maxNodes});
          if ((numNewNodes + _usedNodes) > _maxNodes) {
            LOG.warn("Unable to schedule topology {} because it needs {} " +
                "more nodes which would exced the limit of {}",
                new Object[] {topId, numNewNodes, _maxNodes});
            //TODO set an error of some sort on the topology
            continue;
          }
          
          Collection<Node> found = NodePool.takeNodesBySlot(slotsNeeded, lesserPools);
          _usedNodes += found.size();
          allNodes.addAll(found);
        } else {
          LOG.info("Topology {} is isolated", topId);
          int nodesFromUsAvailable = nodesAvailable();
          int nodesFromOthersAvailable = NodePool.nodesAvailable(lesserPools);
          
          int nodesUsed = _topologyIdToNodes.get(topId).size();
          int nodesNeeded = nodesRequested.intValue() - nodesUsed;
          LOG.info("Nodes... requested {} used {} available from us {} " +
          		"avail from other {} needed {}", new Object[] {nodesRequested, 
              nodesUsed, nodesFromUsAvailable, nodesFromOthersAvailable,
              nodesNeeded});
          if ((nodesNeeded - nodesFromUsAvailable) > (_maxNodes - _usedNodes)) {
            LOG.warn("Unable to schedule topology {} because it needs {} " +
            		"more nodes which would exced the limit of {}",
            		new Object[] {topId, nodesNeeded - nodesFromUsAvailable, _maxNodes});
            //TODO set an error of some sort on the topology
            continue;
          }
          
          //In order to avoid going over _maxNodes we may need to steal from
          // ourself even though other pools have free nodes. so figure out how
          // much each group should provide
          int nodesNeededFromOthers = Math.min(Math.min(_maxNodes - _usedNodes, 
              nodesFromOthersAvailable), nodesNeeded);
          int nodesNeededFromUs = nodesNeeded - nodesNeededFromOthers; 
          LOG.info("Nodes... needed from us {} needed from others {}", 
              nodesNeededFromUs, nodesNeededFromOthers);
          
          if (nodesNeededFromUs > nodesFromUsAvailable) {
            LOG.warn("There are not engough free nodes to schedule {}", topId);
            //TODO set an error of some sort on the topology
            continue;
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
          slotsToUse = Math.min(slotsRequested - slotsUsed, slotsFree);
          if (slotsToUse <= 0) {
            //TODO we really need some more state about how long a supervisor 
            // has been gone for so we can better determine what to do.  Grab a
            // new node, or wait a little longer. 
            LOG.warn("An Isolated topology {} has had both a supervisor and" +
            		" a worker crash, but not all workers. lets hope it comes " +
            		"back up soon.", topId);
            continue;
          }
        }

        Map<ExecutorDetails, String> execToComp = td.getExecutorToComponent();
        SchedulerAssignment assignment = _cluster.getAssignmentById(topId);
        Map<String,Set<String>> nodeToComps = new HashMap<String, Set<String>>();

        if (assignment != null) {
          Map<ExecutorDetails, WorkerSlot> execToSlot = assignment.getExecutorToSlot();
          
          for (Entry<ExecutorDetails, WorkerSlot> entry: execToSlot.entrySet()) {
            String nodeId = entry.getValue().getNodeId();
            Set<String> comps = nodeToComps.get(nodeId);
            if (comps == null) {
              comps = new HashSet<String>();
              nodeToComps.put(nodeId, comps);
            }
            comps.add(execToComp.get(entry.getKey()));
          }
        } 
        
        HashMap<String, List<ExecutorDetails>> spreadToSchedule = new HashMap<String, List<ExecutorDetails>>();
        List<String> spreadComps = (List<String>)td.getConf().get(Config.TOPOLOGY_SPREAD_COMPONENTS);
        if (spreadComps != null) {
          for (String comp: spreadComps) {
            spreadToSchedule.put(comp, new ArrayList<ExecutorDetails>());
          }
        }
        
        ArrayList<Set<ExecutorDetails>> slots = new ArrayList<Set<ExecutorDetails>>(slotsToUse);
        for (int i = 0; i < slotsToUse; i++) {
          slots.add(new HashSet<ExecutorDetails>());
        }

        int at = 0;
        for (Entry<String, List<ExecutorDetails>> entry: _cluster.getNeedsSchedulingComponentToExecutors(td).entrySet()) {
          LOG.info("Scheduling for {}", entry.getKey());
          if (spreadToSchedule.containsKey(entry.getKey())) {
            LOG.info("Saving {} for spread...",entry.getKey());
            spreadToSchedule.get(entry.getKey()).addAll(entry.getValue());
          } else {
            for (ExecutorDetails ed: entry.getValue()) {
              LOG.info("Assigning {} {} to slot {}", new Object[]{entry.getKey(), ed, at});
              slots.get(at).add(ed);
              at++;
              if (at >= slots.size()) {
                at = 0;
              }
            }
          }
        }

        Set<ExecutorDetails> lastSlot = slots.get(slots.size() - 1);
        LinkedList<Node> sortedNodes = new LinkedList<Node>(allNodes);
        Collections.sort(sortedNodes, Node.FREE_NODE_COMPARATOR);
        Collections.reverse(sortedNodes);

        LOG.info("Nodes sorted by free space {}", sortedNodes);
        
        for (Set<ExecutorDetails> slot: slots) {
          Node n = sortedNodes.remove();
          LOG.info("Adding slot to node {} with {} free slots",n,n.totalSlotsFree());
          if (slot == lastSlot) {
            //The last slot fill it up
            for (Entry<String, List<ExecutorDetails>> entry: spreadToSchedule.entrySet()) {
              if (entry.getValue().size() > 0) {
                slot.addAll(entry.getValue());
              }
            }
          } else {
            String nodeId = n.getId();
            Set<String> nodeComps = nodeToComps.get(nodeId);
            if (nodeComps == null) {
              nodeComps = new HashSet<String>();
              nodeToComps.put(nodeId, nodeComps);
            }
            for (Entry<String, List<ExecutorDetails>> entry: spreadToSchedule.entrySet()) {
              if (entry.getValue().size() > 0) {
                String comp = entry.getKey();
                if (!nodeComps.contains(comp)) {
                  nodeComps.add(comp);
                  slot.add(entry.getValue().remove(0));
                }
              }
            }
          }
          n.assign(topId, slot, _cluster);
          int freeSlots = n.totalSlotsFree();
          LOG.info("Node {} now has {} free slots",n,n.totalSlotsFree());
          //TODO really should do a binary search but it should be small
          for (int i = 0; i < sortedNodes.size(); i++) {
            LOG.info("Should I put it in at {} with {} free slots",i,sortedNodes.get(i).totalSlotsFree());
            if (freeSlots >= sortedNodes.get(i).totalSlotsFree()) {
              LOG.info("Yes...");
              sortedNodes.add(i, n);
              n = null;
              break;
            }
          }
          if (n != null) {
            LOG.info("Adding {} in at the end of the list", n);
            sortedNodes.add(n);
          }
        }
      }
    }
  }

  @Override
  public Collection<Node> takeNodes(int nodesNeeded) {
    LOG.info("Taking {} from {}", nodesNeeded, this);
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