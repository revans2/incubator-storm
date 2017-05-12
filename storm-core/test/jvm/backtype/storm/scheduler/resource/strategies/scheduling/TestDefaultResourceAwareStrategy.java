package backtype.storm.scheduler.resource.strategies.scheduling;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.WorkerResources;
import backtype.storm.networktopography.DNSToSwitchMapping;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Cluster.SupervisorResources;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.RAS_Node;
import backtype.storm.scheduler.resource.ResourceAwareScheduler;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy;
import backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.ObjectResources;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import backtype.storm.scheduler.resource.User;
import backtype.storm.topology.SharedOffHeapWithinNode;
import backtype.storm.topology.SharedOffHeapWithinWorker;
import backtype.storm.topology.SharedOnHeap;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

public class TestDefaultResourceAwareStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(TestDefaultResourceAwareStrategy.class);

    private static int currentTime = 1450418597;

    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     */
    @Test
    public void testDefaultResourceAwareStrategySharedMemory() {
        int spoutParallelism = 2;
        int boltParallelism = 2;
        int numBolts = 3;
        double cpuPercent = 10;
        double memoryOnHeap = 10;
        double memoryOffHeap = 10;
        double sharedOnHeap = 500;
        double sharedOffHeapNode = 700;
        double sharedOffHeapWorker = 500;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinWorker(sharedOffHeapWorker, "bolt-1 shared off heap worker")).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOffHeapWithinNode(sharedOffHeapNode, "bolt-2 shared node")).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).addSharedMemory(new SharedOnHeap(sharedOnHeap, "bolt-3 shared worker")).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        Config conf = new Config();
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 500.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        conf.putAll(Utils.readDefaultConfig());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, DefaultEvictionStrategy.class.getName());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, DefaultResourceAwareStrategy.class.getName());
        conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, cpuPercent);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, memoryOffHeap);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, memoryOnHeap);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 2000);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                TestUtilsForResourceAwareScheduler.genExecsAndComps(stormToplogy)
                , currentTime);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);
        
        for (Entry<String, SupervisorResources> entry: cluster.getSupervisorsResourcesMap().entrySet()) {
            String supervisorId = entry.getKey();
            SupervisorResources resources = entry.getValue();
            assertTrue(supervisorId, resources.getTotalCpu() >= resources.getUsedCpu());
            assertTrue(supervisorId, resources.getTotalMem() >= resources.getUsedMem());
        }

        // Everything should fit in a single slot
        int totalNumberOfTasks = (spoutParallelism + (boltParallelism * numBolts));
        double totalExpectedCPU = totalNumberOfTasks * cpuPercent;
        double totalExpectedOnHeap = (totalNumberOfTasks * memoryOnHeap) + sharedOnHeap;
        double totalExpectedWorkerOffHeap = (totalNumberOfTasks * memoryOffHeap) + sharedOffHeapWorker;
        
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());
        assertEquals(1, assignment.getSlots().size());
        WorkerSlot ws = assignment.getSlots().iterator().next();
        String nodeId = ws.getNodeId();
        assertEquals(1, assignment.getTotalSharedOffHeapMemory().size());
        assertEquals(sharedOffHeapNode, assignment.getTotalSharedOffHeapMemory().get(nodeId), 0.01);
        assertEquals(1, assignment.getScheduledResources().size());
        WorkerResources resources = assignment.getScheduledResources().get(ws);
        assertEquals(totalExpectedCPU, resources.get_cpu(), 0.01);
        assertEquals(totalExpectedOnHeap, resources.get_mem_on_heap(), 0.01);
        assertEquals(totalExpectedWorkerOffHeap, resources.get_mem_off_heap(), 0.01);
        assertEquals(sharedOnHeap, resources.get_shared_mem_on_heap(), 0.01);
        assertEquals(sharedOffHeapWorker, resources.get_shared_mem_off_heap(), 0.01);
    }
    
    
    /**
     * test if the scheduling logic for the DefaultResourceAwareStrategy is correct
     */
    @Test
    public void testDefaultResourceAwareStrategy() {
        int spoutParallelism = 1;
        int boltParallelism = 2;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TestUtilsForResourceAwareScheduler.TestSpout(),
                spoutParallelism);
        builder.setBolt("bolt-1", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-1");
        builder.setBolt("bolt-3", new TestUtilsForResourceAwareScheduler.TestBolt(),
                boltParallelism).shuffleGrouping("bolt-2");

        StormTopology stormToplogy = builder.createTopology();

        Config conf = new Config();
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 150.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1500.0);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(4, 4, resourceMap);
        conf.putAll(Utils.readDefaultConfig());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        conf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 50.0);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 250);
        conf.put(Config.TOPOLOGY_PRIORITY, 0);
        conf.put(Config.TOPOLOGY_NAME, "testTopology");
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);

        TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
                TestUtilsForResourceAwareScheduler.genExecsAndComps(stormToplogy)
                , this.currentTime);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, conf);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(conf);
        rs.schedule(topologies, cluster);

        Map<String, List<String>> nodeToComps = new HashMap<String, List<String>>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignments().get("testTopology-id").getExecutorToSlot().entrySet()) {
            WorkerSlot ws = entry.getValue();
            ExecutorDetails exec = entry.getKey();
            if (!nodeToComps.containsKey(ws.getNodeId())) {
                nodeToComps.put(ws.getNodeId(), new LinkedList<String>());
            }
            nodeToComps.get(ws.getNodeId()).add(topo.getExecutorToComponent().get(exec));
        }

        /**
         * check for correct scheduling
         * Since all the resource availabilites on nodes are the same in the beginining
         * DefaultResourceAwareStrategy can arbitrarily pick one thus we must find if a particular scheduling
         * exists on a node the the cluster.
         */

        //one node should have the below scheduling
        List<String> node1 = new LinkedList<>();
        node1.add("spout");
        node1.add("bolt-1");
        node1.add("bolt-2");
        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node1));

        //one node should have the below scheduling
        List<String> node2 = new LinkedList<>();
        node2.add("bolt-3");
        node2.add("bolt-1");
        node2.add("bolt-2");

        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node2));

        //one node should have the below scheduling
        List<String> node3 = new LinkedList<>();
        node3.add("bolt-3");

        Assert.assertTrue("Check DefaultResourceAwareStrategy scheduling", checkDefaultStrategyScheduling(nodeToComps, node3));

        //three used and one node should be empty
        Assert.assertEquals("only three nodes should be used", 3, nodeToComps.size());
    }

    private boolean checkDefaultStrategyScheduling(Map<String, List<String>> nodeToComps, List<String> schedulingToFind) {
        for (List<String> entry : nodeToComps.values()) {
            if (schedulingToFind.containsAll(entry) && entry.containsAll(schedulingToFind)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Test whether strategy will choose correct rack
     */
    @Test
    public void testMultipleRacks() {

        final Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        //generate a rack of supervisors
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0);
        final Map<String, SupervisorDetails> supMapRack1 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 0, resourceMap);

        //generate another rack of supervisors with less resources
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4000.0);
        final Map<String, SupervisorDetails> supMapRack2 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 10, resourceMap);

        //generate some supervisors that are depleted of one resource
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 0.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0);
        final Map<String, SupervisorDetails> supMapRack3 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 20, resourceMap);

        //generate some that has alot of memory but little of cpu
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 10.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 8000.0 *2 + 4000.0);
        final Map<String, SupervisorDetails> supMapRack4 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 30, resourceMap);

        //generate some that has alot of cpu but little of memory
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0 + 200.0 + 10.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        final Map<String, SupervisorDetails> supMapRack5 = TestUtilsForResourceAwareScheduler.genSupervisors(10, 4, 40, resourceMap);


        supMap.putAll(supMapRack1);
        supMap.putAll(supMapRack2);
        supMap.putAll(supMapRack3);
        supMap.putAll(supMapRack4);
        supMap.putAll(supMapRack5);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);
        INimbus iNimbus = new TestUtilsForResourceAwareScheduler.INimbusTest();

        //create test DNSToSwitchMapping plugin
        DNSToSwitchMapping TestNetworkTopographyPlugin = new DNSToSwitchMapping() {
            @Override
            public Map<String, String> resolve(List<String> names) {
                Map<String, String> ret = new HashMap<String, String>();
                for (SupervisorDetails sup : supMapRack1.values()) {
                    ret.put(sup.getHost(), "rack-0");
                }
                for (SupervisorDetails sup : supMapRack2.values()) {
                    ret.put(sup.getHost(), "rack-1");
                }
                for (SupervisorDetails sup : supMapRack3.values()) {
                    ret.put(sup.getHost(), "rack-2");
                }
                for (SupervisorDetails sup : supMapRack4.values()) {
                    ret.put(sup.getHost(), "rack-3");
                }
                for (SupervisorDetails sup : supMapRack5.values()) {
                    ret.put(sup.getHost(), "rack-4");
                }
                return ret;
            }
        };

        //generate topologies
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        TopologyDetails topo1 = TestUtilsForResourceAwareScheduler.getTopology("topo-1", config, 8, 0, 2, 0, currentTime - 2, 10);
        TopologyDetails topo2 = TestUtilsForResourceAwareScheduler.getTopology("topo-2", config, 8, 0, 2, 0, currentTime - 2, 10);

        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        
        List<String> supHostnames = new LinkedList<>();
        for (SupervisorDetails sup : supMap.values()) {
            supHostnames.add(sup.getHost());
        }
        Map<String, List<String>> rackToNodes = new HashMap<>();
        Map<String, String> resolvedSuperVisors =  TestNetworkTopographyPlugin.resolve(supHostnames);
        for (Map.Entry<String, String> entry : resolvedSuperVisors.entrySet()) {
            String hostName = entry.getKey();
            String rack = entry.getValue();
            List<String> nodesForRack = rackToNodes.get(rack);
            if (nodesForRack == null) {
                nodesForRack = new ArrayList<String>();
                rackToNodes.put(rack, nodesForRack);
            }
            nodesForRack.add(hostName);
        }
        cluster.setNetworkTopography(rackToNodes);

        DefaultResourceAwareStrategy rs = new DefaultResourceAwareStrategy();

        rs.prepare(new SchedulingState(new HashMap<String, User>(), cluster, topologies, config));
        TreeSet<ObjectResources> sortedRacks= rs.sortRacks(topo1.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());

        Assert.assertEquals("# of racks sorted", 5, sortedRacks.size());
        Iterator<ObjectResources> it = sortedRacks.iterator();
        // Ranked first since rack-0 has the most balanced set of resources
        Assert.assertEquals("rack-0 should be ordered first", "rack-0", it.next().id);
        // Ranked second since rack-1 has a balanced set of resources but less than rack-0
        Assert.assertEquals("rack-1 should be ordered second", "rack-1", it.next().id);
        // Ranked third since rack-4 has a lot of cpu but not a lot of memory
        Assert.assertEquals("rack-4 should be ordered third", "rack-4", it.next().id);
        // Ranked fourth since rack-3 has alot of memory but not cpu
        Assert.assertEquals("rack-3 should be ordered fourth", "rack-3", it.next().id);
        //Ranked last since rack-2 has not cpu resources
        Assert.assertEquals("rack-2 should be ordered fifth", "rack-2", it.next().id);

        SchedulingResult schedulingResult = rs.schedule(topo1);
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : schedulingResult.getSchedulingResultMap().entrySet()) {
            WorkerSlot ws = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            //make sure all workers on scheduled in rack-0
            Assert.assertEquals("assert worker scheduled on rack-0", "rack-0", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
            // make actual assignments
            cluster.assign(ws, topo1.getId(), execs);
        }
        Assert.assertEquals("All executors in topo-1 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());

        //Test if topology is already partially scheduled on one rack

        Iterator<ExecutorDetails> executorIterator = topo2.getExecutors().iterator();
        List<String> nodeHostnames = rackToNodes.get("rack-1");
        for (int i=0 ; i< topo2.getExecutors().size()/2; i++) {
            String nodeHostname = nodeHostnames.get(i % nodeHostnames.size());
            RAS_Node node = rs.idToNode(rs.NodeHostnameToId(nodeHostname));
            WorkerSlot targetSlot = node.getFreeSlots().iterator().next();
            ExecutorDetails targetExec = executorIterator.next();
            // to keep track of free slots
            node.assign(targetSlot, topo2, Arrays.asList(targetExec));
            // to actually assign
            cluster.assign(targetSlot, topo2.getId(), Arrays.asList(targetExec));
        }

        rs = new DefaultResourceAwareStrategy();
        rs.prepare(new SchedulingState(new HashMap<String, User>(), cluster, topologies, config));
        // schedule topo2
        schedulingResult = rs.schedule(topo2);

        // checking assignments
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> entry : schedulingResult.getSchedulingResultMap().entrySet()) {
            WorkerSlot ws = entry.getKey();
            Collection<ExecutorDetails> execs = entry.getValue();
            //make sure all workers on scheduled in rack-1
            Assert.assertEquals("assert worker scheduled on rack-1", "rack-1", resolvedSuperVisors.get(rs.idToNode(ws.getNodeId()).getHostname()));
            // make actual assignments
            cluster.assign(ws, topo2.getId(), execs);
        }
        Assert.assertEquals("All executors in topo-2 scheduled", 0, cluster.getUnassignedExecutors(topo1).size());
    }
}
