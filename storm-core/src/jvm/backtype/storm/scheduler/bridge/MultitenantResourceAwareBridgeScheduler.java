package backtype.storm.scheduler.bridge;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.ResourceAwareScheduler;
import backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy;
import backtype.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import backtype.storm.scheduler.multitenant.MultitenantScheduler;
import backtype.storm.scheduler.multitenant.Node;

public class MultitenantResourceAwareBridgeScheduler implements IScheduler{
    private static final Logger LOG = LoggerFactory.getLogger(MultitenantResourceAwareBridgeScheduler.class);
    @SuppressWarnings("rawtypes")
    private Map _conf;
    private static final Class<MultitenantStrategy> MULTITENANT_STRATEGY = MultitenantStrategy.class;
    private static final Class<DefaultResourceAwareStrategy> RESOURCE_AWARE_STRATEGY = DefaultResourceAwareStrategy.class;
    private MultitenantScheduler multitenantScheduler = new MultitenantScheduler();
    ResourceAwareScheduler ras = new ResourceAwareScheduler();
    private static final Double PER_WORKER_CPU_SWAG = 100.0;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf) {
        _conf = conf;
        multitenantScheduler.prepare(_conf);
        ras.prepare(_conf);
    }
 
    @Override
    public Map<String, Object> config() {

        Map<String, Object> bridgeSchedulerConfigs = new HashMap<String, Object>();
        Map<String, Object> multitenantSchedulerConfigs = multitenantScheduler.config();
        Map<String, Object> resourceAwareSchedulerConfigs = ras.config();

        //set isolated nodes for multitenant scheduler
        for (String user : multitenantSchedulerConfigs.keySet()) {
            bridgeSchedulerConfigs.put(user, new HashMap<String, Object>());
            ((Map<String, Object>) bridgeSchedulerConfigs.get(user)).put("MultitenantScheduler", multitenantSchedulerConfigs.get(user));
        }

        //set resource guarantee for RAS
        for (String user : resourceAwareSchedulerConfigs.keySet()) {
            if (!bridgeSchedulerConfigs.containsKey(user)) {
                bridgeSchedulerConfigs.put(user,  new HashMap<String, Object>());
            }
            LOG.debug("bridgeSchedulerConfigs.get(user) {}",bridgeSchedulerConfigs.get(user));
            ((Map<String, Object>) bridgeSchedulerConfigs.get(user)).put("ResourceAwareScheduler", resourceAwareSchedulerConfigs.get(user));
        }
        return bridgeSchedulerConfigs;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("\n\n\n/** Rerunning CombinedScheduler... **/");
        this.printScheduling(cluster, topologies);
        this.printClusterInfo(cluster);

        LOG.debug("/* dividing topologies */");
        Map<String, Topologies> dividedTopologies = this.divideTopologies(cluster, topologies);
        
        if(LOG.isDebugEnabled()) {
            for(Entry<String, Topologies> entry : dividedTopologies.entrySet()) {
                LOG.debug("scheduler: {}", entry.getKey());
                for(TopologyDetails topo : entry.getValue().getTopologies()) {
                    LOG.debug("-> {}-{}", topo.getName(), topo.getId());
                }
            }
        }
        
        Topologies rasTopologies = dividedTopologies.get(RESOURCE_AWARE_STRATEGY.getName());
        Topologies mtTopologies = dividedTopologies.get(MULTITENANT_STRATEGY.getName());

        LOG.debug("/* running Multitenant scheduler */");

        //Even though all the topologies are passed into the multitenant scheduler
        //Topologies marked as RAS will be skipped by the multitenant scheduler
        multitenantScheduler.schedule(topologies, cluster);
        //Update resource assignment information for each multitenant topology
        cluster.addMTResourceEstimates(mtTopologies);

        LOG.debug("/* Translating to RAS cluster */");
        Cluster rasCluster = translateToRASCluster(cluster, rasTopologies, topologies,
                multitenantScheduler.getNodesRASCanUse());

        LOG.debug("RAS cluster info: ");
        this.printClusterInfo(rasCluster);

        LOG.debug("/* running RAS scheduler */");
        if(rasTopologies.getTopologies().size() > 0) {
            ras.schedule(rasTopologies, rasCluster);
        }

        LOG.debug("/* Merge RAS Cluster with actual cluster */");

        this.mergeCluster(cluster, rasCluster);
    }

    /**
     * divides the topologies into two groups. 
     * One group are topologies labeled to be scheduled by the Multitenant Scheduler
     * the other group to be scheduled by the Resource Aware scheduler
     * @param cluster
     * @param topologies
     * @return A map containing topologies that are divided by scheduler
     */
    Map<String, Topologies> divideTopologies(Cluster cluster, Topologies topologies) {
        Map<String, Topologies> dividedTopos = new HashMap<String, Topologies>();
        Map<String, TopologyDetails> multitenantTopologies = new HashMap<String, TopologyDetails>();
        Map<String, TopologyDetails> rasTopologies = new HashMap<String, TopologyDetails>();
        for(TopologyDetails topo : topologies.getTopologies()) {
            if(MULTITENANT_STRATEGY.getName().equals(topo.getTopologyStrategy())) {
                multitenantTopologies.put(topo.getId(), topo);
            } else {
                rasTopologies.put(topo.getId(), topo);
            }
        }
        dividedTopos.put(MULTITENANT_STRATEGY.getName(), new Topologies(multitenantTopologies));
        dividedTopos.put(RESOURCE_AWARE_STRATEGY.getName(), new Topologies(rasTopologies));
        return dividedTopos;
      }

    /**
     * creates a mock cluster object to feed into RAS
     * So that RAS will not need to know what multitenant has already done
     * @param cluster
     * @param rasTopologies
     * @param allTopologies
     * @param nodesRASCanUse
     * @return creates a mock cluster object to feed into RAS
     */
    public Cluster translateToRASCluster(Cluster cluster, Topologies rasTopologies, Topologies allTopologies, Map<String, Node> nodesRASCanUse) {
        Map<String, SupervisorDetails> rasClusterSups = this.getRASClusterSups(cluster, rasTopologies, allTopologies, nodesRASCanUse);
        Map<String, SchedulerAssignmentImpl> rasClusterAssignments = this.getRASClusterAssignments(cluster, rasTopologies);

        Cluster rasCluster = new Cluster(cluster.getINimbus(), rasClusterSups, rasClusterAssignments, rasTopologies, cluster.getConf());
        //set existing statuses
        for (Entry<String, String> entry : cluster.getStatusMap().entrySet()) {
            String topoId = entry.getKey();
            String status = entry.getValue();
            if (rasTopologies.getById(topoId) != null) {
                rasCluster.setStatus(topoId, status);
            }
        }
        return rasCluster;
    }

    /**
     * Generates a list of SupervisorDetails objects that RAS can use.
     * @param cluster
     * @param rasTopologies
     * @param allTopologies
     * @param nodesRASCanUse
     * @return a map of supervisors that RAS can use
     */
    Map<String, SupervisorDetails> getRASClusterSups(Cluster cluster, Topologies rasTopologies, Topologies allTopologies, Map<String, Node> nodesRASCanUse) {
        Map<String, SupervisorDetails> rasClusterSups = new HashMap<>();
        for(SupervisorDetails sup : cluster.getSupervisors().values()) {
            Node n = nodesRASCanUse.get(sup.getId());
            if(n != null) {
                LOG.debug("RAS Supervisor: {}-{}", sup.getHost(), sup.getId());
                Set<Number> availPorts = new HashSet<>(cluster.getAssignablePorts(sup));
                //Find the slots currently occupied by MT topologies
                for (Map.Entry<WorkerSlot, String> entry: n.getWorkerToTopo().entrySet()) {
                    String topoId = entry.getValue();
                    if(rasTopologies.getById(topoId) == null) {
                        //Non-RAS topo so remove the slot
                        int port = entry.getKey().getPort();
                        if (!availPorts.remove(port)) {
                            LOG.warn("MT assigned a slot to non-existent port {} {}", topoId, entry.getKey());
                        }
                    }
                }

                LOG.debug("->free ports: {}", availPorts);
                //calculate resources available
                Map<String, Double> supResources = swagMultitenantResourceUsageForRAS(cluster, allTopologies,  nodesRASCanUse.get(sup.getId()), sup);
                LOG.debug("->sup_resource: {}", supResources);
                SupervisorDetails newRasSup = new SupervisorDetails(sup.getId(), sup.getHost(),
                        sup.getMeta(), sup.getSchedulerMeta(), availPorts, supResources);
                rasClusterSups.put(newRasSup.getId(), newRasSup);
            }
        }
        return rasClusterSups;
    }

    Map<String, SchedulerAssignmentImpl> getRASClusterAssignments(Cluster cluster, Topologies rasTopologies) {
        Map<String, SchedulerAssignmentImpl> rasClusterAssignments =  new HashMap<String, SchedulerAssignmentImpl>();
        for(String topoId : cluster.getAssignments().keySet()) {
            if(rasTopologies.getById(topoId) != null) {
                rasClusterAssignments.put(topoId, new SchedulerAssignmentImpl(cluster.getAssignments().get(topoId)));
            }
        }
        printAssignment(rasClusterAssignments);
        return rasClusterAssignments;
    }

    /**
     * estimate the resource usage on a node where multitenant has already scheduled something
     * @param cluster
     * @param allTopologies
     * @param node
     * @param sup
     * @return a map of estimated resources available for a supervisor.
     */
    Map<String, Double> swagMultitenantResourceUsageForRAS(Cluster cluster, Topologies allTopologies, Node node, SupervisorDetails sup) {
        Double memoryUsedOnNode = 0.0;
        Double cpuUsedOnNode = 0.0;
        Map<String, Double> resourceList = new HashMap<String, Double>();

        LOG.debug("->Topologies running on Node: {}", node.getRunningTopologies());
        for(String topoId : node.getRunningTopologies()) {
            if (MULTITENANT_STRATEGY.getName().equals(allTopologies.getById(topoId).getTopologyStrategy())) {
                SchedulerAssignment assignment = cluster.getAssignmentById(topoId);
                Set<WorkerSlot> usedSlots = assignment.getSlots();
                LOG.debug("->usedSlots: {})", usedSlots);
                Map topConf = allTopologies.getById(topoId).getConf();
                double topologyWorkerMemory = cluster.getAssignedMemoryForSlot(topConf);
                for (WorkerSlot ws : usedSlots) {
                    if (sup.getId().equals(ws.getNodeId())) {
                        memoryUsedOnNode += topologyWorkerMemory;
                        cpuUsedOnNode += PER_WORKER_CPU_SWAG;
                    }
                }
            }
        }

        LOG.debug("->memoryUsedOnNode: {}", memoryUsedOnNode);
        LOG.debug("->supervisor total memory: {}", sup.getTotalMemory() );
        resourceList.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, sup.getTotalMemory() - memoryUsedOnNode);
        resourceList.put(Config.SUPERVISOR_CPU_CAPACITY, sup.getTotalCPU() - cpuUsedOnNode);
        return resourceList;
    }

    /**
     * Merge mock RAS cluster object into the actual cluster object
     * so that scheduling done by RAS with actually materialize.
     * @param target
     * @param ephemeral
     */
    public void mergeCluster(Cluster target, Cluster ephemeral) {
        //Not the same object
        if (target != ephemeral) {
            //Unassign everything first in case things have moved and there is overlap
            for (String id : ephemeral.getAssignments().keySet()) {
                target.unassign(id);
            }
            for (SchedulerAssignment assignment : ephemeral.getAssignments().values()) {
                try {
                    target.assign(assignment, false);
                } catch (RuntimeException e) {
                    LOG.error("Looks like RAS scheduled something on a slot it was not supposed to use.  Unassigning offending topology {}",
                        assignment.getTopologyId(), e);
                    target.unassign(assignment.getTopologyId());
                    target.setStatus(assignment.getTopologyId(), "Internal Scheduling Error...");
                }
            }
            //add scheduler identifier and merge scheduler set status
            for (Entry<String, String> statusEntry : target.getStatusMap().entrySet()) {
                String topoId = statusEntry.getKey();
                String status = statusEntry.getValue();
                if (!status.startsWith("(MT)")) {
                    target.setStatus(topoId, "(MT) " + status);
                } else {
                    target.setStatus(topoId, status);
                }
            }
            for (Entry<String, String> statusEntry : ephemeral.getStatusMap().entrySet()) {
                String topoId = statusEntry.getKey();
                String status =  statusEntry.getValue();
                if (!status.startsWith("(RAS)")) {
                    target.setStatus(topoId, "(RAS) " + status);
                } else {
                    target.setStatus(topoId, status);
                }
            }
        }
    }

    WorkerSlot findWorker(String nodeId, Integer port, Collection<WorkerSlot> slots) {
        for(WorkerSlot slot : slots) {
            if(slot.getNodeId().equals(nodeId) && slot.getPort() == port) {
                return slot;
            }
        }
        return null;
    }

    /**
     * print scheduling for debug purposes
     * @param cluster
     * @param topologies
     */
    public void printScheduling(Cluster cluster, Topologies topologies) {
        StringBuilder str = new StringBuilder();
        Map<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> schedulingMap = new HashMap<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>>();
        for(TopologyDetails topo : topologies.getTopologies()) {
                if(cluster.getAssignmentById(topo.getId()) !=  null) {
                for(Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
                    WorkerSlot slot = entry.getValue();
                    String nodeId = slot.getNodeId();
                    ExecutorDetails exec = entry.getKey(); 
                    if(schedulingMap.containsKey(nodeId) == false) {
                        schedulingMap.put(nodeId, new HashMap<String, Map<WorkerSlot, Collection<ExecutorDetails>>>());
                    }
                    if(schedulingMap.get(nodeId).containsKey(topo.getId()) == false) {
                        schedulingMap.get(nodeId).put(topo.getId(), new HashMap<WorkerSlot, Collection<ExecutorDetails>>());
                    }
                    if(schedulingMap.get(nodeId).get(topo.getId()).containsKey(slot) == false) {
                        schedulingMap.get(nodeId).get(topo.getId()).put(slot, new LinkedList<ExecutorDetails>());
                    }
                    schedulingMap.get(nodeId).get(topo.getId()).get(slot).add(exec);
                }
            }
        }
        
        for(Entry<String, Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>>> entry : schedulingMap.entrySet()) {
            if(cluster.getSupervisorById(entry.getKey()) != null) {
             str.append("/** Node: " + cluster.getSupervisorById(entry.getKey()).getHost() + "-"+entry.getKey() + " **/\n");
             } else {
                 str.append("/** Node: Unknown may be dead -" + entry.getKey() + " **/\n");
             }
             for(Entry<String, Map<WorkerSlot, Collection<ExecutorDetails>>> topo_sched : schedulingMap.get(entry.getKey()).entrySet()) {
                 str.append("\t-->Topology: " + topo_sched.getKey()+"\n");
                 for(Map.Entry<WorkerSlot, Collection<ExecutorDetails>> ws : topo_sched.getValue().entrySet()) {
                     str.append("\t\t->Slot [" + ws.getKey().getPort() + "] -> " + ws.getValue() + "\n");
                 }
             }
         }
        LOG.debug("Scheduling of Cluster\n{}", str.toString());
    }

    /**
     * print nodes ands and assignable workers for a cluster object. For debugging purposes
     * @param cluster
     */
    public void printClusterInfo(Cluster cluster) {
        Map<String, Collection<WorkerSlot>> nodeToWsMap = new HashMap<String, Collection<WorkerSlot>> ();
        for (WorkerSlot ws : cluster.getAssignableSlots()) {
            if(nodeToWsMap.containsKey(ws.getNodeId()) == false) {
                nodeToWsMap.put(ws.getNodeId(), new LinkedList<WorkerSlot>());
            }
            nodeToWsMap.get(ws.getNodeId()).add(ws);
        }
        LOG.debug("Cluster setup\n{}", nodeToWsMap);
    }

    public void printAssignment(Map<String, SchedulerAssignmentImpl> assignments) {
        for(Entry<String, SchedulerAssignmentImpl> entry : assignments.entrySet()) {
            LOG.debug("Topology: {} Assignments: {}", entry.getKey(),  entry.getValue().getExecutorToSlot());
        }
    }
}
