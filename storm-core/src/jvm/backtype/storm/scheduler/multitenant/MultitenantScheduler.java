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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.Utils;

public class MultitenantScheduler implements IScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MultitenantScheduler.class);
  Set<Node> nodesRasCanUse = new HashSet<Node>();
  @SuppressWarnings("rawtypes")
  private Map _conf;
  
  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map conf) {
    _conf = conf;
  }
 
  private Map<String, Number> getUserConf() {
    Map<String, Number> ret = (Map<String, Number>)_conf.get(Config.MULTITENANT_SCHEDULER_USER_POOLS);
    if (ret == null) {
      ret = new HashMap<String, Number>();
    } else {
      ret = new HashMap<String, Number>(ret); 
    }

    Map fromFile = Utils.findAndReadConfigFile("multitenant-scheduler.yaml", false);
    Map<String, Number> tmp = (Map<String, Number>)fromFile.get(Config.MULTITENANT_SCHEDULER_USER_POOLS);
    if (tmp != null) {
      ret.putAll(tmp);
    }
    return ret;
  }

 
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    LOG.debug("Rerunning scheduling...");
    LOG.debug("Cluster Scheduling:\n{}", printScheduling(cluster, topologies));

    Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster);
    
    Map<String, Number> userConf = getUserConf();
    
    Map<String, IsolatedPool> userPools = new HashMap<String, IsolatedPool>();
    for (Map.Entry<String, Number> entry : userConf.entrySet()) {
      userPools.put(entry.getKey(), new IsolatedPool(entry.getValue().intValue()));
    }
    DefaultPool defaultPool = new DefaultPool();
    FreePool freePool = new FreePool();
    
    freePool.init(cluster, nodeIdToNode);
    for (IsolatedPool pool : userPools.values()) {
      pool.init(cluster, nodeIdToNode);
    }
    defaultPool.init(cluster, nodeIdToNode);
    
    for (TopologyDetails td: topologies.getTopologies()) {
      String user = (String)td.getConf().get(Config.TOPOLOGY_SUBMITTER_USER);
      LOG.debug("Found top {} run by user {}",td.getId(), user);
      NodePool pool = userPools.get(user);
      if (pool == null || !pool.canAdd(td)) {
        pool = defaultPool;
      }
      pool.addTopology(td);
    }
    
    //Now schedule all of the topologies that need to be scheduled
    for (IsolatedPool pool : userPools.values()) {
      pool.scheduleAsNeeded(freePool, defaultPool);
      pool.printInfo();
    }
    defaultPool.scheduleAsNeeded(freePool);
    
    this.nodesRasCanUse.addAll(defaultPool.getNodesInPool());
    this.nodesRasCanUse.addAll(freePool.getNodesInPool());
    LOG.debug("Scheduling done...");
  }

    @Override
    public Map<String, Object> config() {
        return (Map)getUserConf();
    }

    /**
     * returns a list of nodes RAS can use
     * basically a list of nodes from both free and default pool
     * since nodes from the isolated pool are the only nodes RAS cannot touch
     * @return
     */
    public Map<String, Node> getNodesRASCanUse() {
        Map<String, Node> nodeMap = new HashMap<String, Node>();
        for(Node node : this.nodesRasCanUse) {
            nodeMap.put(node.getId(), node);
        }
        return nodeMap;
    }

  /**
   * print scheduling for debug purposes
   * @param cluster
   * @param topologies
   */
  public String printScheduling(Cluster cluster, Topologies topologies) {
    StringBuilder str = new StringBuilder();
    Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap = new HashMap<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>>();
    for (TopologyDetails topo : topologies.getTopologies()) {
      if (cluster.getAssignmentById(topo.getId()) != null) {
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
          WorkerSlot slot = entry.getValue();
          String nodeId = slot.getNodeId();
          ExecutorDetails exec = entry.getKey();
          if (schedulingMap.containsKey(nodeId) == false) {
            schedulingMap.put(nodeId, new HashMap<String, Map<WorkerSlot, Collection<ExecutorDetails>>>());
          }
          if (schedulingMap.get(nodeId).containsKey(topo.getId()) == false) {
            schedulingMap.get(nodeId).put(topo.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());
          }
          if (schedulingMap.get(nodeId).get(topo.getId()).containsKey(slot) == false) {
            schedulingMap.get(nodeId).get(topo.getId()).put(slot, new LinkedList<ExecutorDetails>());
          }
          schedulingMap.get(nodeId).get(topo.getId()).get(slot).add(exec);
        }
      }
    }

    for (Map.Entry<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> entry : schedulingMap.entrySet()) {
      if (cluster.getSupervisorById(entry.getKey()) != null) {
        str.append("/** Node: " + cluster.getSupervisorById(entry.getKey()).getHost() + "-" + entry.getKey() + " **/\n");
      } else {
        str.append("/** Node: Unknown may be dead -" + entry.getKey() + " **/\n");
      }
      for (Map.Entry<String, Map<WorkerSlot, Collection<ExecutorDetails>>> topo_sched : schedulingMap.get(entry.getKey()).entrySet()) {
        str.append("\t-->Topology: " + topo_sched.getKey() + "\n");
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> ws : topo_sched.getValue().entrySet()) {
          str.append("\t\t->Slot [" + ws.getKey().getPort() + "] -> " + ws.getValue() + "\n");
        }
      }
    }
    return str.toString();
  }
}
