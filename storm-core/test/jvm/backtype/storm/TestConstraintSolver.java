package backtype.storm;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.multitenant.ConstraintSolverForMultitenant;
import backtype.storm.scheduler.multitenant.MultitenantScheduler;
import backtype.storm.scheduler.multitenant.Node;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class TestConstraintSolver {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestConstraintSolver.class);
  private static final int NUM_SUPS = 20;
  private static final int NUM_WORKERS_PER_SUP = 4;
  private static final int MAX_TRAVERSAL_DEPTH = 1000000;
  private static final int NUM_WORKERS = NUM_SUPS * NUM_WORKERS_PER_SUP;
  private final String TOPOLOGY_SUBMITTER = "jerry";

  @Test
  public void testIsolatedSchedulerWithConstraints() throws IOException {

    MultitenantScheduler ms = new MultitenantScheduler();
    Map<String, SupervisorDetails> supMap = genSupervisors(20, 4);
    List<Node> nodes = getNodes(supMap);

    INimbus iNimbus = new INimbus() {
      @Override
      public void prepare(Map stormConf, String schedulerLocalDir) {

      }

      @Override
      public Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
        return null;
      }

      @Override
      public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {

      }

      @Override
      public String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId) {
        if(existingSupervisors.containsKey(nodeId)) {
          return existingSupervisors.get(nodeId).getHost();
        }
        return null;
      }

      @Override
      public IScheduler getForcedScheduler() {
        return null;
      }
    };

    List<List<String>> constraints = new LinkedList<>();
    addContraints("spout-0", "bolt-0", constraints);
    addContraints("spout-0", "bolt-1", constraints);
    addContraints("bolt-1", "bolt-1", constraints);
    addContraints("bolt-1", "bolt-2", constraints);

    List<String> spread = new LinkedList<String>();
    spread.add("spout-0");
    spread.add("spout-1");

    LOG.info("constraints: {}", constraints);

    backtype.storm.Config config = new backtype.storm.Config();

    config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
    config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
    config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
    config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
    config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
    config.put(Config.TOPOLOGY_PRIORITY, 1);

    TopologyDetails topo = getTopology(config, 5, 15, 15, 30);
    Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
    topoMap.put(topo.getId(), topo);

    Topologies topologies = new Topologies(topoMap);

    Map<String, Number> userPools = new HashMap<String, Number>();
    userPools.put(TOPOLOGY_SUBMITTER, NUM_SUPS);
    Config conf = new Config();
    conf.put(Config.MULTITENANT_SCHEDULER_USER_POOLS, userPools);

    ms.prepare(conf);

    Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, new Config());

    for(Node n : Node.getAllNodesFrom(cluster).values()) {
      LOG.info("Node: {} slots: {}", n.getId(), n.getFreeSlots());
    }
    
    ms.schedule(topologies, cluster);

    LOG.info("status: {}", cluster.getStatusMap());
  }

  @Test
  public void testConstraintSolver() {
    List<List<String>> constraints = new LinkedList<>();
    addContraints("spout-0", "bolt-0", constraints);
    addContraints("bolt-1", "bolt-1", constraints);
    addContraints("bolt-1", "bolt-2", constraints);

      List<String> spread = new LinkedList<String>();
    spread.add("spout-0");
    spread.add("spout-1");


    LOG.info("constraints: {}", constraints);

    backtype.storm.Config config = new backtype.storm.Config();

    config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
    config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
    config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
    config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
    config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
    config.put(Config.TOPOLOGY_PRIORITY, 1);

    TopologyDetails topo = getTopology(config, 5, 15, 15, 30);

    List<Node> nodes = getNodes(genSupervisors(NUM_SUPS, NUM_WORKERS_PER_SUP));

    ConstraintSolverForMultitenant cs = new ConstraintSolverForMultitenant(topo, NUM_WORKERS, new HashSet<Node>(nodes));
      Map<ExecutorDetails, WorkerSlot> results = cs.findScheduling();

    LOG.info("Results: {}", results);


    Map<WorkerSlot, HashSet<ExecutorDetails>> workerExecMap = new HashMap<WorkerSlot, HashSet<ExecutorDetails>>();
    Map<WorkerSlot, ArrayList<String>> workerCompMap = new HashMap<WorkerSlot, ArrayList<String>>();
    for(Map.Entry<ExecutorDetails, WorkerSlot> entry : results.entrySet()) {
      if(workerExecMap.containsKey(entry.getValue())==false) {
        workerExecMap.put(entry.getValue(), new HashSet<ExecutorDetails>());
        workerCompMap.put(entry.getValue(), new ArrayList<String>());
      }
      workerExecMap.get(entry.getValue()).add(entry.getKey());
      workerCompMap.get(entry.getValue()).add(topo.getExecutorToComponent().get(entry.getKey()));
    }
    LOG.info("Size: " + workerCompMap.size() + " workerCompMap:\n" + workerCompMap);
    LOG.info("# backtrack: " + cs.getNumBacktrack() + " depth: " + cs.getTraversalDepth());
    Assert.assertTrue("Valid Scheduling?", cs.validateSolution(results));
  }

  public static void addContraints(String comp1, String comp2, List<List<String>> constraints) {
    LinkedList<String> constraintPair = new LinkedList<String>();
    constraintPair.add(comp1);
    constraintPair.add(comp2);
    constraints.add(constraintPair);
  }

  List<Node> getNodes(Map<String, SupervisorDetails> supMap) {
    List<Node> retList = new LinkedList<Node>();
    for(SupervisorDetails sup : supMap.values()) {
      retList.add(new Node(sup.getId(), sup.getAllPorts(), true));
    }
    return retList;
  }


  private static Map<String, SupervisorDetails> genSupervisors(int numSup, int numPorts) {
    Map<String, SupervisorDetails> retList = new HashMap<String, SupervisorDetails>();
    for(int i=0; i<numSup; i++) {
      List<Number> ports = new LinkedList<Number>();
      for(int j = 0; j<numPorts; j++) {
        ports.add(j);
      }
      SupervisorDetails sup = new SupervisorDetails("sup-"+i, "host-"+i, null, ports);
      retList.put(sup.getId(), sup);
    }
    return retList;
  }

  private static Map<ExecutorDetails, String> genExecsAndComps(StormTopology topology, int spoutParallelism, int boltParallelism) {
    Map<ExecutorDetails, String> retMap = new HashMap<ExecutorDetails, String> ();
    int startTask=0;
    int endTask=1;
    for(Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
      for(int i=0; i<spoutParallelism; i++) {
        retMap.put(new ExecutorDetails(startTask, endTask), entry.getKey());
        startTask++;
        endTask++;
      }
    }

    for(Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
      for(int i=0; i<boltParallelism; i++) {
        retMap.put(new ExecutorDetails(startTask, endTask), entry.getKey());
        startTask++;
        endTask++;
      }
    }
    return retMap;
  }

  private static TopologyDetails getTopology(Map config, int numSpout, int numBolt,
                                      int spoutParallelism, int boltParallelism) {

    StormTopology topology = buildTopology(numSpout,numBolt, spoutParallelism, boltParallelism);
    TopologyDetails topo = new TopologyDetails("topo-1", config, topology, (Integer) config.get(Config.TOPOLOGY_WORKERS),
        genExecsAndComps(topology, spoutParallelism, boltParallelism), "jerry");
    return topo;
  }

  public static StormTopology buildTopology(int numSpout, int numBolt,
                                            int spoutParallelism, int boltParallelism) {
    LOG.info("buildTopology with -> numSpout: " + numSpout + " spoutParallelism: "
            + spoutParallelism + " numBolt: "
            + numBolt + " boltParallelism: " + boltParallelism);
    TopologyBuilder builder = new TopologyBuilder();

    for (int i = 0; i < numSpout; i++) {
      SpoutDeclarer s1 = builder.setSpout("spout-" + i, new TestSpout(),
              spoutParallelism);
    }
    int j = 0;
    for (int i = 0; i < numBolt; i++) {
      if (j >= numSpout) {
        j = 0;
      }
      BoltDeclarer b1 = builder.setBolt("bolt-" + i, new TestBolt(),
              boltParallelism).shuffleGrouping("spout-" + j);
    }

    return builder.createTopology();
  }

  public static class TestSpout extends BaseRichSpout {
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TestSpout() {
      this(true);
    }

    public TestSpout(boolean isDistributed) {
      _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
      Utils.sleep(100);
      final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      final Random rand = new Random();
      final String word = words[rand.nextInt(words.length)];
      _collector.emit(new Values(word));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      if (!_isDistributed) {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        return ret;
      } else {
        return null;
      }
    }
  }

  public static class TestBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }
}
