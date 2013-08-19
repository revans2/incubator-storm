package backtype.storm.scheduler.multitenant;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class MultitenantScheduler implements IScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MultitenantScheduler.class);
  @SuppressWarnings("rawtypes")
  private Map _conf;
  
  @Override
  public void prepare(@SuppressWarnings("rawtypes") Map conf) {
    _conf = conf;
  }
  
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    LOG.info("Rerunning scheduling...");
    Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster);
    
    //TODO get this from someplace else so it is loaded dynamically.
    Map<String, Number> userConf = (Map<String, Number>)_conf.get(Config.MULTITENANT_SCHEDULER_USER_POOLS);
    if (userConf == null) {
      userConf = new HashMap<String, Number>();
    }
    
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
      LOG.info("Found top {} run by user {}",td.getId(), user);
      NodePool pool = userPools.get(user);
      if (pool == null || !pool.canAdd(td)) {
        pool = defaultPool;
      }
      pool.addTopology(td);
    }
    
    //Now schedule all of the topologies that need to be scheduled
    for (IsolatedPool pool : userPools.values()) {
      pool.scheduleAsNeeded(freePool, defaultPool);
    }
    defaultPool.scheduleAsNeeded(freePool);
    LOG.info("Scheduling done...");
  }
}
