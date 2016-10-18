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
package org.apache.storm.daemon.nimbus;

import static org.apache.storm.metric.StormMetricsRegistry.registerMeter;
import static org.apache.storm.utils.Utils.OR;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.KeySequenceNumber;
import org.apache.storm.blobstore.LocalFsBlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.ClusterMetricsConsumerExecutor;
import org.apache.storm.nimbus.DefaultTopologyValidator;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;

public class Nimbus {
    private final static Logger LOG = LoggerFactory.getLogger(Nimbus.class);
    
    public static final Meter submitTopologyWithOptsCalls = registerMeter("nimbus:num-submitTopologyWithOpts-calls");
    public static final Meter submitTopologyCalls = registerMeter("nimbus:num-submitTopology-calls");
    public static final Meter killTopologyWithOptsCalls = registerMeter("nimbus:num-killTopologyWithOpts-calls");
    public static final Meter killTopologyCalls = registerMeter("nimbus:num-killTopology-calls");
    public static final Meter rebalanceCalls = registerMeter("nimbus:num-rebalance-calls");
    public static final Meter activateCalls = registerMeter("nimbus:num-activate-calls");
    public static final Meter deactivateCalls = registerMeter("nimbus:num-deactivate-calls");
    public static final Meter debugCalls = registerMeter("nimbus:num-debug-calls");
    public static final Meter setWorkerProfilerCalls = registerMeter("nimbus:num-setWorkerProfiler-calls");
    public static final Meter getComponentPendingProfileActionsCalls = registerMeter("nimbus:num-getComponentPendingProfileActions-calls");
    public static final Meter setLogConfigCalls = registerMeter("nimbus:num-setLogConfig-calls");
    public static final Meter uploadNewCredentialsCalls = registerMeter("nimbus:num-uploadNewCredentials-calls");
    public static final Meter beginFileUploadCalls = registerMeter("nimbus:num-beginFileUpload-calls");
    public static final Meter uploadChunkCalls = registerMeter("nimbus:num-uploadChunk-calls");
    public static final Meter finishFileUploadCalls = registerMeter("nimbus:num-finishFileUpload-calls");
    public static final Meter beginFileDownloadCalls = registerMeter("nimbus:num-beginFileDownload-calls");
    public static final Meter downloadChunkCalls = registerMeter("nimbus:num-downloadChunk-calls");
    public static final Meter getNimbusConfCalls = registerMeter("nimbus:num-getNimbusConf-calls");
    public static final Meter getLogConfigCalls = registerMeter("nimbus:num-getLogConfig-calls");
    public static final Meter getTopologyConfCalls = registerMeter("nimbus:num-getTopologyConf-calls");
    public static final Meter getTopologyCalls = registerMeter("nimbus:num-getTopology-calls");
    public static final Meter getUserTopologyCalls = registerMeter("nimbus:num-getUserTopology-calls");
    public static final Meter getClusterInfoCalls = registerMeter("nimbus:num-getClusterInfo-calls");
    public static final Meter getTopologyInfoWithOptsCalls = registerMeter("nimbus:num-getTopologyInfoWithOpts-calls");
    public static final Meter getTopologyInfoCalls = registerMeter("nimbus:num-getTopologyInfo-calls");
    public static final Meter getTopologyPageInfoCalls = registerMeter("nimbus:num-getTopologyPageInfo-calls");
    public static final Meter getSupervisorPageInfoCalls = registerMeter("nimbus:num-getSupervisorPageInfo-calls");
    public static final Meter getComponentPageInfoCalls = registerMeter("nimbus:num-getComponentPageInfo-calls");
    public static final Meter shutdownCalls = registerMeter("nimbus:num-shutdown-calls");
    
    public static final String STORM_VERSION = VersionInfo.getVersion();
    public static final List<ACL> ZK_ACLS = Arrays.asList(ZooDefs.Ids.CREATOR_ALL_ACL.get(0),
            new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    public static final Subject NIMBUS_SUBJECT = new Subject();
    static {
        NIMBUS_SUBJECT.getPrincipals().add(new NimbusPrincipal());
        NIMBUS_SUBJECT.setReadOnly();
    }
    
    public static final BinaryOperator<Map<String, Map<WorkerSlot, WorkerResources>>> MERGE_ID_TO_WORKER_RESOURCES = (orig, update) -> {
        return merge(orig, update);
    };
    
    public static final BinaryOperator<Map<String, TopologyResources>> MERGE_ID_TO_RESOURCES = (orig, update) -> {
        return merge(orig, update);
    };
    
    //TODO perhaps this should all go to a few switch statements?
    public static final Map<TopologyStatus, Map<TopologyActions, TopologyStateTransition>> TOPO_STATE_TRANSITIONS = 
            new ImmutableMap.Builder<TopologyStatus, Map<TopologyActions, TopologyStateTransition>>()
            .put(TopologyStatus.ACTIVE, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                    .put(TopologyActions.INACTIVATE, TopologyStateTransition.INACTIVE)
                    .put(TopologyActions.ACTIVATE, TopologyStateTransition.NOOP)
                    .put(TopologyActions.REBALANCE, TopologyStateTransition.REBALANCE)
                    .put(TopologyActions.KILL, TopologyStateTransition.KILL)
                    .build())
            .put(TopologyStatus.INACTIVE, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                    .put(TopologyActions.ACTIVATE, TopologyStateTransition.ACTIVE)
                    .put(TopologyActions.INACTIVATE, TopologyStateTransition.NOOP)
                    .put(TopologyActions.REBALANCE, TopologyStateTransition.REBALANCE)
                    .put(TopologyActions.KILL, TopologyStateTransition.KILL)
                    .build())
            .put(TopologyStatus.KILLED, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                    .put(TopologyActions.STARTUP, TopologyStateTransition.STARTUP_WHEN_KILLED)
                    .put(TopologyActions.KILL, TopologyStateTransition.KILL)
                    .put(TopologyActions.REMOVE, TopologyStateTransition.REMOVE)
                    .build())
            .put(TopologyStatus.REBALANCING, new ImmutableMap.Builder<TopologyActions, TopologyStateTransition>()
                    .put(TopologyActions.STARTUP, TopologyStateTransition.STARTUP_WHEN_REBALANCING)
                    .put(TopologyActions.KILL, TopologyStateTransition.KILL)
                    .put(TopologyActions.DO_REBALANCE, TopologyStateTransition.DO_REBALANCE)
                    .build())
            .build();
    
    //TODO is it possible to move these to a ConcurrentMap?
    public static final class Assoc<K,V> implements UnaryOperator<Map<K, V>> {
        private final K key;
        private final V value;
        
        public Assoc(K key, V value) {
            this.key = key;
            this.value = value;
        }
        
        @Override
        public Map<K, V> apply(Map<K, V> t) {
            Map<K, V> ret = new HashMap<>(t);
            ret.put(key, value);
            return ret;
        }
    }
    
    public static final class Dissoc<K,V> implements UnaryOperator<Map<K, V>> {
        private final K key;
        
        public Dissoc(K key) {
            this.key = key;
        }
        
        @Override
        public Map<K, V> apply(Map<K, V> t) {
            Map<K, V> ret = new HashMap<>(t);
            ret.remove(key);
            return ret;
        }
    }
    
    @SuppressWarnings("deprecation")
    public static TimeCacheMap<String, AutoCloseable> fileCacheMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 600),
                (id, stream) -> {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static <K, V> Map<K, V> merge(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> other) {
        Map<K, V> ret = new HashMap<>(first);
        if (other != null) {
            ret.putAll(other);
        }
        return ret;
    }
    
    //TODO private
    public static <K, V> Map<K, V> mapDiff(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> second) {
        Map<K, V> ret = new HashMap<>();
        for (Entry<? extends K, ? extends V> entry: second.entrySet()) {
            if (!entry.getValue().equals(first.get(entry.getKey()))) {
                ret.put(entry.getKey(), entry.getValue());
            }
        }
        return ret;
    }

    public static IScheduler makeScheduler(Map<String, Object> conf, INimbus inimbus) {
        String schedClass = (String) conf.get(Config.STORM_SCHEDULER);
        IScheduler scheduler = inimbus == null ? null : inimbus.getForcedScheduler();
        if (scheduler != null) {
            LOG.info("Using forced scheduler from INimbus {} {}", scheduler.getClass(), scheduler);
        } else if (schedClass != null){
            LOG.info("Using custom scheduler: {}", schedClass);
            scheduler = Utils.newInstance(schedClass);
        } else {
            LOG.info("Using default scheduler");
            scheduler = new DefaultScheduler();
        }
        scheduler.prepare(conf);
        return scheduler;
    }

    /**
     * Constructs a TimeCacheMap instance with a blob store timeout whose
     * expiration callback invokes cancel on the value held by an expired entry when
     * that value is an AtomicOutputStream and calls close otherwise.
     * @param conf the config to use
     * @return the newly created map
     */
    @SuppressWarnings("deprecation")
    public static TimeCacheMap<String, OutputStream> makeBlobCachMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_BLOBSTORE_EXPIRATION_SECS), 600),
                (id, stream) -> {
                    try {
                        if (stream instanceof AtomicOutputStream) {
                            ((AtomicOutputStream) stream).cancel();
                        } else {
                            stream.close();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
    
    /**
     * Constructs a TimeCacheMap instance with a blobstore timeout and no callback function.
     * @param conf
     * @return
     */
    @SuppressWarnings("deprecation")
    public static TimeCacheMap<String, Iterator<String>> makeBlobListCachMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_BLOBSTORE_EXPIRATION_SECS), 600));
    }
    
    public static ITopologyActionNotifierPlugin createTopologyActionNotifier(Map<String, Object> conf) {
        String clazz = (String) conf.get(Config.NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN);
        ITopologyActionNotifierPlugin ret = null;
        if (clazz != null && !clazz.isEmpty()) {
            ret = Utils.newInstance(clazz);
            try {
                ret.prepare(conf);
            } catch (Exception e) {
                LOG.warn("Ignoring exception, Could not initialize {}", clazz, e);
                ret = null;
            }
        }
        return ret;
    }
    
    @SuppressWarnings("unchecked")
    public static List<ClusterMetricsConsumerExecutor> makeClusterMetricsConsumerExecutors(Map<String, Object> conf) {
        Collection<Map<String, Object>> consumers = (Collection<Map<String, Object>>) conf.get(Config.STORM_CLUSTER_METRICS_CONSUMER_REGISTER);
        List<ClusterMetricsConsumerExecutor> ret = new ArrayList<>();
        if (consumers != null) {
            for (Map<String, Object> consumer : consumers) {
                ret.add(new ClusterMetricsConsumerExecutor((String) consumer.get("class"), consumer.get("argument")));
            }
        }
        return ret;
    }
    
    //TODO private
    public static Subject getSubject() {
        return ReqContext.context().subject();
    }
    
    //TODO private
    public static Map<String, Object> readTopoConf(String topoId, BlobStore blobStore) throws KeyNotFoundException, AuthorizationException, IOException {
        return blobStore.readTopologyConf(topoId, getSubject());
    }
    
    //TODO private
    public static List<String> getKeyListFromId(Map<String, Object> conf, String id) {
        List<String> ret = new ArrayList<>(3);
        ret.add(ConfigUtils.masterStormCodeKey(id));
        ret.add(ConfigUtils.masterStormConfKey(id));
        if (!ConfigUtils.isLocalMode(conf)) {
            ret.add(ConfigUtils.masterStormJarKey(id));
        }
        return ret;
    }
    
    //TODO private
    public static int getVerionForKey(String key, NimbusInfo nimbusInfo, Map<String, Object> conf) {
        KeySequenceNumber kseq = new KeySequenceNumber(key, nimbusInfo);
        return kseq.getKeySequenceNumber(conf);
    }
    
    //TODO private
    public static StormTopology readStormTopology(String topoId, BlobStore store) throws KeyNotFoundException, AuthorizationException, IOException {
        return store.readTopology(topoId, getSubject());
    }
    
    //TODO private
    public static Map<String, Object> readTopoConfAsNimbus(String topoId, BlobStore store) throws KeyNotFoundException, AuthorizationException, IOException {
        return store.readTopologyConf(topoId, NIMBUS_SUBJECT);
    }
    
    //TODO private
    public static StormTopology readStormTopologyAsNimbus(String topoId, BlobStore store) throws KeyNotFoundException, AuthorizationException, IOException {
        return store.readTopology(topoId, NIMBUS_SUBJECT);
    }
    
    //TODO private
    //TODO lets not use lists for all of this but real objects
    /**
     * convert {topology-id -> SchedulerAssignment} to
     *         {topology-id -> {executor [node port]}}
     * @return
     */
    public static Map<String, Map<List<Long>, List<Object>>> computeTopoToExecToNodePort(Map<String, SchedulerAssignment> schedAssignments) {
        Map<String, Map<List<Long>, List<Object>>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignment> schedEntry: schedAssignments.entrySet()) {
            Map<List<Long>, List<Object>> execToNodePort = new HashMap<>();
            for (Entry<ExecutorDetails, WorkerSlot> execAndNodePort: schedEntry.getValue().getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execAndNodePort.getKey();
                WorkerSlot slot = execAndNodePort.getValue();
                
                List<Long> listExec = new ArrayList<>(2);
                listExec.add((long) exec.getStartTask());
                listExec.add((long) exec.getEndTask());
                
                List<Object> nodePort = new ArrayList<>(2);
                nodePort.add(slot.getNodeId());
                nodePort.add((long)slot.getPort());
                
                execToNodePort.put(listExec, nodePort);
            }
            ret.put(schedEntry.getKey(), execToNodePort);
        }
        return ret;
    }
    
    //TODO private
    public static int numUsedWorkers(SchedulerAssignment assignment) {
        if (assignment == null) {
            return 0;
        }
        return assignment.getSlots().size();
    }
    
    //TODO private
    //TODO lets use real objects again
    /**
     * convert {topology-id -> SchedulerAssignment} to
     *         {topology-id -> {[node port] [mem-on-heap mem-off-heap cpu]}}
     * Make sure this can deal with other non-RAS schedulers
     * later we may further support map-for-any-resources
     * @param schedAssignments the assignments
     * @return  {topology-id {[node port] [mem-on-heap mem-off-heap cpu]}}
     */
    public static Map<String, Map<List<Object>, List<Double>>> computeTopoToNodePortToResources(Map<String, SchedulerAssignment> schedAssignments) {
        Map<String, Map<List<Object>, List<Double>>> ret = new HashMap<>();
        for (Entry<String, SchedulerAssignment> schedEntry: schedAssignments.entrySet()) {
            Map<List<Object>, List<Double>> nodePortToResources = new HashMap<>();
            for (WorkerSlot slot: schedEntry.getValue().getExecutorToSlot().values()) {
                List<Object> nodePort = new ArrayList<>(2);
                nodePort.add(slot.getNodeId());
                nodePort.add((long)slot.getPort());
                
                List<Double> resources = new ArrayList<>(3);
                resources.add(slot.getAllocatedMemOnHeap());
                resources.add(slot.getAllocatedMemOffHeap());
                resources.add(slot.getAllocatedCpu());
                
                nodePortToResources.put(nodePort, resources);
            }
            ret.put(schedEntry.getKey(), nodePortToResources);
        }
        return ret;
    }

    //TODO private
    public static Map<String, Map<List<Long>, List<Object>>> computeNewTopoToExecToNodePort(Map<String, SchedulerAssignment> schedAssignments,
            Map<String, Assignment> existingAssignments) {
        Map<String, Map<List<Long>, List<Object>>> ret = computeTopoToExecToNodePort(schedAssignments);
        //Print some useful information
        if (existingAssignments != null && !existingAssignments.isEmpty()) {
            for (Entry<String, Map<List<Long>, List<Object>>> entry: ret.entrySet()) {
                String topoId = entry.getKey();
                Map<List<Long>, List<Object>> execToNodePort = entry.getValue();
                Assignment assignment = existingAssignments.get(topoId);
                if (assignment == null) {
                    continue;
                }
                Map<List<Long>, NodeInfo> old = assignment.get_executor_node_port();
                Map<List<Long>, List<Object>> reassigned = new HashMap<>();
                for (Entry<List<Long>, List<Object>> execAndNodePort: execToNodePort.entrySet()) {
                    NodeInfo oldAssigned = old.get(execAndNodePort.getKey());
                    String node = (String) execAndNodePort.getValue().get(0);
                    Long port = (Long) execAndNodePort.getValue().get(1);
                    if (oldAssigned == null || !oldAssigned.get_node().equals(node) 
                            || !port.equals(oldAssigned.get_port_iterator().next())) {
                        reassigned.put(execAndNodePort.getKey(), execAndNodePort.getValue());
                    }
                }

                if (!reassigned.isEmpty()) {
                    int count = (new HashSet<>(execToNodePort.values())).size();
                    Set<List<Long>> reExecs = reassigned.keySet();
                    LOG.info("Reassigning {} to {} slots", topoId, count);
                    LOG.info("Reassign executors: {}", reExecs);
                }
            }
        }
        return ret;
    }
    
    //TODO private
    public static List<List<Long>> changedExecutors(Map<List<Long>, NodeInfo> map,
            Map<List<Long>, List<Object>> newExecToNodePort) {
        HashMap<NodeInfo, List<List<Long>>> tmpSlotAssigned = map == null ? new HashMap<>() : Utils.reverseMap(map);
        HashMap<List<Object>, List<List<Long>>> slotAssigned = new HashMap<>();
        for (Entry<NodeInfo, List<List<Long>>> entry: tmpSlotAssigned.entrySet()) {
            NodeInfo ni = entry.getKey();
            List<Object> key = new ArrayList<>(2);
            key.add(ni.get_node());
            key.add(ni.get_port_iterator().next());
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort((a, b) -> a.get(0).compareTo(b.get(0)));
            slotAssigned.put(key, value);
        }
        HashMap<List<Object>, List<List<Long>>> tmpNewSlotAssigned = newExecToNodePort == null ? new HashMap<>() : Utils.reverseMap(newExecToNodePort);
        HashMap<List<Object>, List<List<Long>>> newSlotAssigned = new HashMap<>();
        for (Entry<List<Object>, List<List<Long>>> entry: tmpNewSlotAssigned.entrySet()) {
            List<List<Long>> value = new ArrayList<>(entry.getValue());
            value.sort((a, b) -> a.get(0).compareTo(b.get(0)));
            newSlotAssigned.put(entry.getKey(), value);
        }
        Map<List<Object>, List<List<Long>>> diff = mapDiff(slotAssigned, newSlotAssigned);
        List<List<Long>> ret = new ArrayList<>();
        for (List<List<Long>> val: diff.values()) {
            ret.addAll(val);
        }
        return ret;
    }

    //TODO private
    public static Set<WorkerSlot> newlyAddedSlots(Assignment old, Assignment current) {
        Set<NodeInfo> oldSlots = new HashSet<>(old.get_executor_node_port().values());
        Set<NodeInfo> niRet = new HashSet<>(current.get_executor_node_port().values());
        niRet.removeAll(oldSlots);
        Set<WorkerSlot> ret = new HashSet<>();
        for (NodeInfo ni: niRet) {
            ret.add(new WorkerSlot(ni.get_node(), ni.get_port_iterator().next()));
        }
        return ret;
    }
    
    //TODO private
    public static Map<String, SupervisorDetails> basicSupervisorDetailsMap(IStormClusterState state) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        for (Entry<String, SupervisorInfo> entry: state.allSupervisorInfo().entrySet()) {
            String id = entry.getKey();
            SupervisorInfo info = entry.getValue();
            ret.put(id, new SupervisorDetails(id, info.get_hostname(), info.get_scheduler_meta(), null,
                    info.get_resources_map()));
        }
        return ret;
    }
    
    //TODO private
    public static boolean isTopologyActive(IStormClusterState state, String topoName) {
        return StormCommon.getStormId(state, topoName) != null;
    }
    
    //TODO private
    public static Map<String, Object> tryReadTopoConf(String topoId, BlobStore store) throws NotAliveException, AuthorizationException, IOException {
        try {
            return readTopoConfAsNimbus(topoId, store);
            //Was a try-cause but I looked at the code around this and key not found is not wrapped in runtime,
            // so it is not needed
        } catch (KeyNotFoundException e) {
            if (topoId == null) {
                throw new NullPointerException();
            }
            throw new NotAliveException(topoId);
        }
    }
    
    private static final List<String> EMPTY_STRING_LIST = Collections.unmodifiableList(Collections.emptyList());
    private static final Set<String> EMPTY_STRING_SET = Collections.unmodifiableSet(Collections.emptySet());
    
    //TODO private??
    public static Set<String> topoIdsToClean(IStormClusterState state, BlobStore store) {
        //TODO go back to a regular hash set once we move nimbus_test into Mockito
        // the mocked verification in clojure needs the order to be guaranteed.
        Set<String> ret = new java.util.TreeSet<>();
        //TODO handle nulls
        ret.addAll(OR(state.heartbeatStorms(), EMPTY_STRING_LIST));
        ret.addAll(OR(state.errorTopologies(), EMPTY_STRING_LIST));
        ret.addAll(OR(store.storedTopoIds(), EMPTY_STRING_SET));
        ret.addAll(OR(state.backpressureTopologies(), EMPTY_STRING_LIST));
        ret.removeAll(OR(state.activeStorms(), EMPTY_STRING_LIST));
        return ret;
    }
    
    //TODO private???
    public static String extractStatusStr(StormBase base) {
        String ret = null;
        TopologyStatus status = base.get_status();
        if (status != null) {
            ret = status.name().toUpperCase();
        }
        return ret;
    }
    
    private static int componentParallelism(Map<String, Object> topoConf, Object component) throws InvalidTopologyException {
        Map<String, Object> combinedConf = merge(topoConf, StormCommon.componentConf(component));
        int numTasks = Utils.getInt(combinedConf.get(Config.TOPOLOGY_TASKS), StormCommon.numStartExecutors(component));
        Integer maxParallel = Utils.getInt(combinedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM), null);
        int ret = numTasks;
        if (maxParallel != null) {
            ret = Math.min(maxParallel, numTasks);
        }
        return ret;
    }
    
    //TODO private
    public static StormTopology normalizeTopology(Map<String, Object> topoConf, StormTopology topology) throws InvalidTopologyException {
        StormTopology ret = topology.deepCopy();
        for (Object comp: StormCommon.allComponents(ret).values()) {
            Map<String, Object> mergedConf = StormCommon.componentConf(comp);
            mergedConf.put(Config.TOPOLOGY_TASKS, componentParallelism(topoConf, comp));
            String jsonConf = JSONValue.toJSONString(mergedConf);
            StormCommon.getComponentCommon(comp).set_json_conf(jsonConf);
        }
        return ret;
    }
    
    private static void addToDecorators(Set<String> decorators, List<String> conf) {
        if (conf != null) {
            decorators.addAll(conf);
        }
    }
    
    @SuppressWarnings("unchecked")
    private static void addToSerializers(Map<String, String> ser, List<Object> conf) {
        if (conf != null) {
            for (Object o: conf) {
                if (o instanceof Map) {
                    ser.putAll((Map<String,String>)o);
                } else {
                    ser.put((String)o, null);
                }
            }
        }
    }
    
    //TODO private
    @SuppressWarnings("unchecked")
    public static Map<String, Object> normalizeConf(Map<String,Object> conf, Map<String, Object> topoConf, StormTopology topology) {
        //ensure that serializations are same for all tasks no matter what's on
        // the supervisors. this also allows you to declare the serializations as a sequence
        List<Map<String, Object>> allConfs = new ArrayList<>();
        for (Object comp: StormCommon.allComponents(topology).values()) {
            allConfs.add(StormCommon.componentConf(comp));
        }

        Set<String> decorators = new HashSet<>();
        //TODO we are putting in a config that is not the same type we pulled out.
        // Why don't we just use a single map instead from the beginning?
        Map<String, String> serializers = new HashMap<>();
        for (Map<String, Object> c: allConfs) {
            addToDecorators(decorators, (List<String>) c.get(Config.TOPOLOGY_KRYO_DECORATORS));
            addToSerializers(serializers, (List<Object>) c.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        //TODO this seems like of dumb that we take all of nothing on the topoConf vs daemon conf
        // Why not just merge them like we do for the other configs.
        addToDecorators(decorators, (List<String>)topoConf.getOrDefault(Config.TOPOLOGY_KRYO_DECORATORS, 
                conf.get(Config.TOPOLOGY_KRYO_DECORATORS)));
        addToSerializers(serializers, (List<Object>)topoConf.getOrDefault(Config.TOPOLOGY_KRYO_REGISTER, 
                conf.get(Config.TOPOLOGY_KRYO_REGISTER)));
        
        Map<String, Object> mergedConf = merge(conf, topoConf);
        Map<String, Object> ret = new HashMap<>(topoConf);
        ret.put(Config.TOPOLOGY_KRYO_REGISTER, serializers);
        ret.put(Config.TOPOLOGY_KRYO_DECORATORS, new ArrayList<>(decorators));
        ret.put(Config.TOPOLOGY_ACKER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
        ret.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, mergedConf.get(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS));
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, mergedConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
        return ret;
    }
    
    //TODO private
    //TODO can we find a way to move this into BlobStore itself???
    public static void rmBlobKey(BlobStore store, String key, IStormClusterState state) {
        try {
            store.deleteBlob(key, NIMBUS_SUBJECT);
            if (store instanceof LocalFsBlobStore) {
                state.removeBlobstoreKey(key);
            }
        } catch (Exception e) {
            //Yes eat the exception
            LOG.info("Exception {}", e);
        }
    }
        
    private final Map<String, Object> conf;
    private final NimbusInfo nimbusHostPortInfo;
    private final INimbus inimbus;
    private final IAuthorizer authorizationHandler;
    private final IAuthorizer impersonationAuthorizationHandler;
    private final AtomicLong submittedCount;
    private final IStormClusterState stormClusterState;
    private final Object submitLock;
    private final Object credUpdateLock;
    private final Object logUpdateLock;
    private final AtomicReference<Map<String, Map<List<Integer>, Map<String, Object>>>> heartbeatsCache;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, AutoCloseable> downloaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, AutoCloseable> uploaders;
    private final BlobStore blobStore;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, OutputStream> blobDownloaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, OutputStream> blobUploaders;
    @SuppressWarnings("deprecation")
    private final TimeCacheMap<String, Iterator<String>> blobListers;
    private final UptimeComputer uptime;
    private final ITopologyValidator validator;
    private final StormTimer timer;
    private final IScheduler scheduler;
    private final ILeaderElector leaderElector;
    private final AtomicReference<Map<String, String>> idToSchedStatus;
    private final AtomicReference<Map<String, Double[]>> nodeIdToResources;
    private final AtomicReference<Map<String, TopologyResources>> idToResources;
    private final AtomicReference<Map<String, Map<WorkerSlot, WorkerResources>>> idToWorkerResources;
    private final Collection<ICredentialsRenewer> credRenewers;
    private final Object topologyHistoryLock;
    private final LocalState topologyHistoryState;
    private final Collection<INimbusCredentialPlugin> nimbusAutocredPlugins;
    private final ITopologyActionNotifierPlugin nimbusTopologyActionNotifier;
    private final List<ClusterMetricsConsumerExecutor> clusterConsumerExceutors;
    
    private static IStormClusterState makeStormClusterState(Map<String, Object> conf) throws Exception {
        //TODO need to change CLusterUtils to have a Map option
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = ZK_ACLS;
        }
        return ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.NIMBUS));
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus) throws Exception {
        this(conf, inimbus, null, null, null, null);
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo, BlobStore blobStore, ILeaderElector leaderElector) throws Exception {
        this.conf = conf;
        if (hostPortInfo == null) {
            hostPortInfo = NimbusInfo.fromConf(conf);
        }
        this.nimbusHostPortInfo = hostPortInfo;
        this.inimbus = inimbus;
        this.authorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(Config.NIMBUS_AUTHORIZER), conf);
        this.impersonationAuthorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
        this.submittedCount = new AtomicLong(0);
        if (stormClusterState == null) {
            stormClusterState =  makeStormClusterState(conf);
        }
        this.stormClusterState = stormClusterState;
        //TODO we need a better lock for this...
        this.submitLock = new Object();
        this.credUpdateLock = new Object();
        this.logUpdateLock = new Object();
        this.heartbeatsCache = new AtomicReference<>(new HashMap<>());
        this.downloaders = fileCacheMap(conf);
        this.uploaders = fileCacheMap(conf);
        if (blobStore == null) {
            blobStore = Utils.getNimbusBlobStore(conf, this.nimbusHostPortInfo);
        }
        this.blobStore = blobStore;
        this.blobDownloaders = makeBlobCachMap(conf);
        this.blobUploaders = makeBlobCachMap(conf);
        this.blobListers = makeBlobListCachMap(conf);
        this.uptime = Utils.makeUptimeComputer();
        this.validator = Utils.newInstance((String) conf.getOrDefault(Config.NIMBUS_TOPOLOGY_VALIDATOR, DefaultTopologyValidator.class.getName()));
        this.timer = new StormTimer(null, (t, e) -> {
            LOG.error("Error while processing event", e);
            Utils.exitProcess(20, "Error while processing event");
        });
        this.scheduler = makeScheduler(conf, inimbus);
        if (leaderElector == null) {
            leaderElector = Zookeeper.zkLeaderElector(conf, getBlobStore());;
        }
        this.leaderElector = leaderElector;
        this.idToSchedStatus = new AtomicReference<>(new HashMap<>());
        this.nodeIdToResources = new AtomicReference<>(new HashMap<>());
        this.idToResources = new AtomicReference<>(new HashMap<>());
        this.idToWorkerResources = new AtomicReference<>(new HashMap<>());
        this.credRenewers = AuthUtils.GetCredentialRenewers(conf);
        this.topologyHistoryLock = new Object();
        this.topologyHistoryState = ConfigUtils.nimbusTopoHistoryState(conf);
        this.nimbusAutocredPlugins = AuthUtils.getNimbusAutoCredPlugins(conf);
        this.nimbusTopologyActionNotifier = createTopologyActionNotifier(conf);
        this.clusterConsumerExceutors = makeClusterMetricsConsumerExecutors(conf);
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public NimbusInfo getNimbusHostPortInfo() {
        return nimbusHostPortInfo;
    }

    public INimbus getINimbus() {
        return inimbus;
    }

    public IAuthorizer getAuthorizationHandler() {
        return authorizationHandler;
    }

    public IAuthorizer getImpersonationAuthorizationHandler() {
        return impersonationAuthorizationHandler;
    }

    public AtomicLong getSubmittedCount() {
        return submittedCount;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public Object getSubmitLock() {
        return submitLock;
    }

    public Object getCredUpdateLock() {
        return credUpdateLock;
    }

    public Object getLogUpdateLock() {
        return logUpdateLock;
    }

    public AtomicReference<Map<String,Map<List<Integer>,Map<String,Object>>>> getHeartbeatsCache() {
        return heartbeatsCache;
    }

    @SuppressWarnings("deprecation")
    public TimeCacheMap<String, AutoCloseable> getDownloaders() {
        return downloaders;
    }

    @SuppressWarnings("deprecation")
    public TimeCacheMap<String, AutoCloseable> getUploaders() {
        return uploaders;
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    @SuppressWarnings("deprecation")
    public TimeCacheMap<String, OutputStream> getBlobDownloaders() {
        return blobDownloaders;
    }

    @SuppressWarnings("deprecation")
    public TimeCacheMap<String, OutputStream> getBlobUploaders() {
        return blobUploaders;
    }

    @SuppressWarnings("deprecation")
    public TimeCacheMap<String, Iterator<String>> getBlobListers() {
        return blobListers;
    }

    public UptimeComputer getUptime() {
        return uptime;
    }

    public ITopologyValidator getValidator() {
        return validator;
    }

    public StormTimer getTimer() {
        return timer;
    }

    public IScheduler getScheduler() {
        return scheduler;
    }

    public ILeaderElector getLeaderElector() {
        return leaderElector;
    }

    public AtomicReference<Map<String, String>> getIdToSchedStatus() {
        return idToSchedStatus;
    }

    public AtomicReference<Map<String, Double[]>> getNodeIdToResources() {
        return nodeIdToResources;
    }

    public AtomicReference<Map<String, TopologyResources>> getIdToResources() {
        return idToResources;
    }

    public AtomicReference<Map<String, Map<WorkerSlot, WorkerResources>>> getIdToWorkerResources() {
        return idToWorkerResources;
    }

    public Collection<ICredentialsRenewer> getCredRenewers() {
        return credRenewers;
    }

    public Object getTopologyHistoryLock() {
        return topologyHistoryLock;
    }

    public LocalState getTopologyHistoryState() {
        return topologyHistoryState;
    }

    public Collection<INimbusCredentialPlugin> getNimbusAutocredPlugins() {
        return nimbusAutocredPlugins;
    }

    public ITopologyActionNotifierPlugin getNimbusTopologyActionNotifier() {
        return nimbusTopologyActionNotifier;
    }

    public List<ClusterMetricsConsumerExecutor> getClusterConsumerExecutors() {
        return clusterConsumerExceutors;
    }
    
    public boolean isLeader() throws Exception {
        return getLeaderElector().isLeader();
    }
    
    public void assertIsLeader() throws Exception {
        if (!isLeader()) {
            NimbusInfo leaderAddress = getLeaderElector().getLeader();
            throw new RuntimeException("not a leader, current leader is " + leaderAddress);
        }
    }
    
    public String getInbox() throws IOException {
        return ConfigUtils.masterInbox(getConf());
    }
    
    //TODO private
    public void delayEvent(String topoId, int delaySecs, TopologyActions event, Object args) {
        LOG.info("Delaying event {} for {} secs for {}", event, delaySecs, topoId);
        getTimer().schedule(delaySecs, () -> {
            try {
                transition(topoId, event, args, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void doRebalance(String topoId, StormBase stormBase) throws Exception {
        RebalanceOptions rbo = stormBase.get_topology_action_options().get_rebalance_options();
        StormBase updated = new StormBase();
        updated.set_topology_action_options(null);
        updated.set_component_debug(Collections.emptyMap());
        
        if (rbo.is_set_num_executors()) {
            updated.set_component_executors(rbo.get_num_executors());
        }
        
        if (rbo.is_set_num_workers()) {
            updated.set_num_workers(rbo.get_num_workers());
        }
        getStormClusterState().updateStorm(topoId, updated);
        mkAssignments(topoId);
    }
    
    private String toTopoId(String topoName) throws NotAliveException {
        String topoId = StormCommon.getStormId(getStormClusterState(), topoName);
        if (topoId == null) {
            throw new NotAliveException(topoName+" is not alive");
        }
        return topoId;
    }
    
    public void transitionName(String topoName, TopologyActions event, Object eventArg) throws Exception {
        transition(toTopoId(topoName), event, eventArg);
    }
    
    public void transitionName(String topoName, TopologyActions event, Object eventArg, boolean errorOnNoTransition) throws Exception {
        transition(toTopoId(topoName), event, eventArg, errorOnNoTransition);
    }

    public void transition(String topoId, TopologyActions event, Object eventArg) throws Exception {
        transition(topoId, event, eventArg, false);
    }
    
    public void transition(String topoId, TopologyActions event, Object eventArg, boolean errorOnNoTransition) throws Exception {
        LOG.info("TRANSITION: {} {} {} {}", topoId, event, eventArg, errorOnNoTransition);
        assertIsLeader();
        synchronized(getSubmitLock()) {
            IStormClusterState clusterState = getStormClusterState();
            StormBase base = clusterState.stormBase(topoId, null);
            TopologyStatus status = base.get_status();
            if (status == null) {
                LOG.info("Cannot apply event {} to {} because topology no longer exists", event, topoId);
            } else {
                TopologyStateTransition transition = TOPO_STATE_TRANSITIONS.get(status).get(event);
                if (transition == null) {
                    String message = "No transition for event: " + event + ", status: " + status + " storm-id: " + topoId;
                    if (errorOnNoTransition) {
                        throw new RuntimeException(message);
                    }
                    
                    if (TopologyActions.STARTUP != event) {
                        //STARTUP is a system event so don't log an issue
                        LOG.info(message);
                    }
                    transition = TopologyStateTransition.NOOP;
                }
                StormBase updates = transition.transition(eventArg, this, topoId, base);
                if (updates != null) {
                    clusterState.updateStorm(topoId, updates);
                }
            }
        }
    }
    
    //TODO private
    public void setupStormCode(Map<String, Object> conf, String topoId, String tmpJarLocation, 
            Map<String, Object> topoConf, StormTopology topology) throws Exception {
        Subject subject = getSubject();
        IStormClusterState clusterState = getStormClusterState();
        BlobStore store = getBlobStore();
        String jarKey = ConfigUtils.masterStormJarKey(topoId);
        String codeKey = ConfigUtils.masterStormCodeKey(topoId);
        String confKey = ConfigUtils.masterStormConfKey(topoId);
        NimbusInfo hostPortInfo = getNimbusHostPortInfo();
        if (tmpJarLocation != null) {
            //in local mode there is no jar
            try (FileInputStream fin = new FileInputStream(tmpJarLocation)) {
                store.createBlob(jarKey, fin, new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
            }
            if (store instanceof LocalFsBlobStore) {
                clusterState.setupBlobstore(jarKey, hostPortInfo, getVerionForKey(jarKey, hostPortInfo, conf));
            }
        }
        
        //TODO looks like some code reuse potential here
        store.createBlob(confKey, Utils.toCompressedJsonConf(topoConf), new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
        if (store instanceof LocalFsBlobStore) {
            clusterState.setupBlobstore(confKey, hostPortInfo, getVerionForKey(confKey, hostPortInfo, conf));
        }
        
        store.createBlob(codeKey, Utils.serialize(topology), new SettableBlobMeta(BlobStoreAclHandler.DEFAULT), subject);
        if (store instanceof LocalFsBlobStore) {
            clusterState.setupBlobstore(codeKey, hostPortInfo, getVerionForKey(codeKey, hostPortInfo, conf));
        }
    }
    
    //TODO private
    //TODO can this go to int
    public Integer getBlobReplicationCount(String key) throws Exception {
        BlobStore store = getBlobStore();
        if (store != null) { //TODO why is this ever null
            return store.getBlobReplication(key, NIMBUS_SUBJECT);
        }
        return null;
    }
    
    //TODO private
    public void waitForDesiredCodeReplication(Map<String, Object> topoConf, String topoId) throws Exception {
        //TODO is this the topo conf?  Why not get this from nimbus itself?
        int minReplicationCount = Utils.getInt(topoConf.get(Config.TOPOLOGY_MIN_REPLICATION_COUNT));
        int maxWaitTime = Utils.getInt(topoConf.get(Config.TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC));
        int jarCount = minReplicationCount;
        if (!ConfigUtils.isLocalMode(topoConf)) {
            jarCount = getBlobReplicationCount(ConfigUtils.masterStormJarKey(topoId));
        }
        int codeCount = getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId));
        int confCount = getBlobReplicationCount(ConfigUtils.masterStormConfKey(topoId));
        long totalWaitTime = 0;
        //When is this ever null?
        if (getBlobStore() != null) {
            while (jarCount < minReplicationCount &&
                    codeCount < minReplicationCount &&
                    confCount < minReplicationCount) {
                if (maxWaitTime > 0 && totalWaitTime > maxWaitTime) {
                    LOG.info("desired replication count of {} not achieved but we have hit the max wait time {}"
                            + " so moving on with replication count for conf key = {} for code key = {} for jar key = ",
                            minReplicationCount, maxWaitTime, confCount, codeCount, jarCount);
                    return;
                }
                LOG.info("WAITING... {} <? {} {} {}", minReplicationCount, jarCount, codeCount, confCount);
                LOG.info("WAITING... {} <? {}", totalWaitTime, maxWaitTime);
                Time.sleepSecs(1);
                totalWaitTime++;
                if (!ConfigUtils.isLocalMode(topoConf)) {
                    jarCount = getBlobReplicationCount(ConfigUtils.masterStormJarKey(topoId));
                }
                codeCount = getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId));
                confCount = getBlobReplicationCount(ConfigUtils.masterStormConfKey(topoId));
            }
        }
        LOG.info("desired replication count {} achieved, current-replication-count for conf key = {},"
                + " current-replication-count for code key = {}, current-replication-count for jar key = {}", 
                minReplicationCount, confCount, codeCount, jarCount);
    }
    
    public TopologyDetails readTopologyDetails(String topoId) throws NotAliveException, KeyNotFoundException, AuthorizationException, IOException, InvalidTopologyException {
        StormBase base = getStormClusterState().stormBase(topoId, null);
        if (base == null) {
            if (topoId == null) {
                throw new NullPointerException();
            }
            throw new NotAliveException(topoId);
        }
        BlobStore store = getBlobStore();
        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, store);
        StormTopology topo = readStormTopologyAsNimbus(topoId, store);
        Map<List<Integer>, String> rawExecToComponent = computeExecutorToComponent(topoId);
        Map<ExecutorDetails, String> executorsToComponent = new HashMap<>();
        for (Entry<List<Integer>, String> entry: rawExecToComponent.entrySet()) {
            List<Integer> execs = entry.getKey();
            ExecutorDetails execDetails = new ExecutorDetails(execs.get(0), execs.get(1));
            executorsToComponent.put(execDetails, entry.getValue());
        }
        
        return new TopologyDetails(topoId, topoConf, topo, base.get_num_workers(), executorsToComponent, base.get_launch_time_secs());
    }
    
    private void updateHeartbeats(String topoId, Set<List<Integer>> allExecutors, Assignment existingAssignment) {
        LOG.debug("Updating heartbeats for {} {}", topoId, allExecutors);
        IStormClusterState state = getStormClusterState();
        Map<List<Integer>, Map<String, Object>> executorBeats = StatsUtil.convertExecutorBeats(state.executorBeats(topoId, existingAssignment.get_executor_node_port()));
        Map<List<Integer>, Map<String, Object>> cache = StatsUtil.updateHeartbeatCache(getHeartbeatsCache().get().get(topoId), executorBeats, allExecutors, Utils.getInt(getConf().get(Config.NIMBUS_TASK_TIMEOUT_SECS)));
        getHeartbeatsCache().getAndUpdate(new Assoc<String, Map<List<Integer>, Map<String, Object>>>(topoId, cache));
    }
    
    //TODO private
    /**
     * update all the heartbeats for all the topologies' executors
     * @param existingAssignments current assignments (thrift)
     * @param topologyToExecutors topology ID to executors.
     */
    public void updateAllHeartbeats(Map<String, Assignment> existingAssignments, Map<String, Set<List<Integer>>> topologyToExecutors) {
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            updateHeartbeats(topoId, topologyToExecutors.get(topoId), entry.getValue());
        }
    }
    
    //TODO private
    public Set<List<Integer>> aliveExecutors(TopologyDetails td, Set<List<Integer>> allExecutors, Assignment assignment) {
        String topoId = td.getId();
        Map<List<Integer>, Map<String, Object>> hbCache = getHeartbeatsCache().get().get(topoId);
        LOG.debug("NEW  Computing alive executors for {}\nExecutors: {}\nAssignment: {}\nHeartbeat cache: {}",
                topoId, allExecutors, assignment, hbCache);
        //TODO need to consider all executors associated with a dead executor (in same slot) dead as well,
        // don't just rely on heartbeat being the same
        
        Map<String, Object> conf = getConf();
        int taskLaunchSecs = Utils.getInt(conf.get(Config.NIMBUS_TASK_LAUNCH_SECS));
        Set<List<Integer>> ret = new HashSet<>();
        Map<List<Long>, Long> execToStartTimes = assignment.get_executor_start_time_secs();

        for (List<Integer> exec: allExecutors) {
            //TODO it really would be best to not need to do this translation.
            // Ideally we would not use a List<Integer> but instead use Something else for the executor data.
            List<Long> longExec = new ArrayList<Long>(exec.size());
            for (Integer num : exec) {
                longExec.add(num.longValue());
            }

            Long startTime = execToStartTimes.get(longExec);
            //TODO it would really be great to not use a Map with strings to pass around this kind of cached data
            // Lets use a real object instead.
            Boolean isTimedOut = (Boolean)hbCache.get(StatsUtil.convertExecutor(longExec)).get("is-timed-out");
            Integer delta = startTime == null ? null : Time.deltaSecs(startTime.intValue());
            if (startTime != null && ((delta < taskLaunchSecs) || !isTimedOut)) {
                ret.add(exec);
            } else {
                LOG.info("Executor {}:{} not alive", topoId, exec);
            }
        }
        return ret;
    }
    
    //TODO private
    public List<List<Integer>> computeExecutors(String topoId) throws KeyNotFoundException, AuthorizationException, IOException, InvalidTopologyException {
        Map<String, Object> conf = getConf();
        BlobStore store = getBlobStore();
        StormBase base = getStormClusterState().stormBase(topoId, null);
        Map<String, Integer> compToExecutors = base.get_component_executors();
        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, store);
        StormTopology topology = readStormTopologyAsNimbus(topoId, store);
        List<List<Integer>> ret = new ArrayList<>();
        if (compToExecutors != null) {
            Map<Integer, String> taskInfo = StormCommon.stormTaskInfo(topology, topoConf);
            Map<String, List<Integer>> compToTaskList = Utils.reverseMap(taskInfo);
            for (Entry<String, List<Integer>> entry: compToTaskList.entrySet()) {
                List<Integer> comps = entry.getValue();
                comps.sort(null);
                Integer numExecutors = compToExecutors.get(entry.getKey());
                if (numExecutors != null) {
                    List<List<Integer>> partitioned = Utils.partitionFixed(numExecutors, comps);
                    for (List<Integer> partition: partitioned) {
                        ret.add(Arrays.asList(partition.get(0), partition.get(partition.size() - 1)));
                    }
                }
            }
        }
        return ret;
    }
    
    //TODO private
    public Map<List<Integer>, String> computeExecutorToComponent(String topoId) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        BlobStore store = getBlobStore();
        //TODO computing executors and this both read topoConf and topology.  Lets see if we can just compute all of this in one pass.
        List<List<Integer>> executors = computeExecutors(topoId);
        StormTopology topology = readStormTopologyAsNimbus(topoId, store);
        Map<String, Object> topoConf = readTopoConfAsNimbus(topoId, store);
        Map<Integer, String> taskToComponent = StormCommon.stormTaskInfo(topology, topoConf);
        Map<List<Integer>, String> ret = new HashMap<>();
        for (List<Integer> executor: executors) {
            ret.put(executor, taskToComponent.get(executor.get(0)));
        }
        return ret;
    }
    
    //TODO private
    public Map<String, Set<List<Integer>>> computeTopologyToExecutors(Collection<String> topoIds) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        if (topoIds != null) {
            for (String topoId: topoIds) {
                ret.put(topoId, new HashSet<>(computeExecutors(topoId)));
            }
        }
        return ret;
    }
    
    //TODO private
    /**
     * compute a topology-id -> alive executors map
     * @param existingAssignment the current assignments
     * @param topologies the current topologies
     * @param topologyToExecutors the executors for the current topologies
     * @param scratchTopologyId the topology being rebalanced and should be excluded
     * @return the map of topology id to alive executors
     */
    public Map<String, Set<List<Integer>>> computeTopologyToAliveExecutors(Map<String, Assignment> existingAssignment, Topologies topologies, 
            Map<String, Set<List<Integer>>> topologyToExecutors, String scratchTopologyId) {
        Map<String, Set<List<Integer>>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignment.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            TopologyDetails td = topologies.getById(topoId);
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors;
            if (topoId.equals(scratchTopologyId)) {
                aliveExecutors = allExecutors;
            } else {
                aliveExecutors = new HashSet<>(aliveExecutors(td, allExecutors, assignment));
            }
            ret.put(topoId, aliveExecutors);
        }
        return ret;
    }
    
    private static List<Integer> asIntExec(List<Long> exec) {
        //TODO this should just not exist
        List<Integer> ret = new ArrayList<>(2);
        ret.add(exec.get(0).intValue());
        ret.add(exec.get(1).intValue());
        return ret;
    }
    
    //TODO private
    public Map<String, Set<Long>> computeSupervisorToDeadPorts(Map<String, Assignment> existingAssignments, Map<String, Set<List<Integer>>> topologyToExecutors,
            Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, Set<Long>> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> allExecutors = topologyToExecutors.get(topoId);
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Set<List<Integer>> deadExecutors = new HashSet<>(allExecutors);
            deadExecutors.removeAll(aliveExecutors);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            for (Entry<List<Long>, NodeInfo> assigned: execToNodePort.entrySet()) {
                if (deadExecutors.contains(asIntExec(assigned.getKey()))) {
                    NodeInfo info = assigned.getValue();
                    String superId = info.get_node();
                    Set<Long> ports = ret.get(superId);
                    if (ports == null) {
                        ports = new HashSet<>();
                        ret.put(superId, ports);
                    }
                    ports.addAll(info.get_port());
                }
            }
        }
        return ret;
    }
    
    //TODO private
    /**
     * Convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api.
     * @param existingAssignments current assignments
     * @param topologyToAliveExecutors executors that are alive
     * @return topo ID to schedulerAssignment
     */
    //TODO this should really return a SchedulerAssignment or we need to merge the two things together.
    public Map<String, SchedulerAssignmentImpl> computeTopologyToSchedulerAssignment(Map<String, Assignment> existingAssignments,
            Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, SchedulerAssignmentImpl> ret = new HashMap<>();
        for (Entry<String, Assignment> entry: existingAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Set<List<Integer>> aliveExecutors = topologyToAliveExecutors.get(topoId);
            Map<List<Long>, NodeInfo> execToNodePort = assignment.get_executor_node_port();
            Map<NodeInfo, WorkerResources> workerToResources = assignment.get_worker_resources();
            Map<NodeInfo, WorkerSlot> nodePortToSlot = new HashMap<>();
            for (Entry<NodeInfo, WorkerResources> nodeAndResources: workerToResources.entrySet()) {
                NodeInfo info = nodeAndResources.getKey();
                WorkerResources resources = nodeAndResources.getValue();
                WorkerSlot slot = new WorkerSlot(info.get_node(), info.get_port_iterator().next(),
                        resources.get_mem_on_heap(), resources.get_mem_off_heap(),
                        resources.get_cpu());
                nodePortToSlot.put(info, slot);
            }
            Map<ExecutorDetails, WorkerSlot> execToSlot = new HashMap<>();
            for (Entry<List<Long>, NodeInfo> execAndNodePort: execToNodePort.entrySet()) {
                List<Integer> exec = asIntExec(execAndNodePort.getKey());
                NodeInfo info = execAndNodePort.getValue();
                if (aliveExecutors.contains(exec)) {
                    execToSlot.put(new ExecutorDetails(exec.get(0), exec.get(1)), nodePortToSlot.get(info));
                }
            }
            ret.put(topoId, new SchedulerAssignmentImpl(topoId, execToSlot));
        }
        return ret;
    }
    
    //TODO private
    /**
     * @param superToDeadPorts dead ports on the supervisor
     * @param topologies all of the topologies
     * @param missingAssignmentTopologies topologies that need assignments
     * @return a map: {supervisor-id SupervisorDetails}
     */
    public Map<String, SupervisorDetails> readAllSupervisorDetails(Map<String, Set<Long>> superToDeadPorts,
            Topologies topologies, Collection<String> missingAssignmentTopologies) {
        Map<String, SupervisorDetails> ret = new HashMap<>();
        IStormClusterState state = getStormClusterState();
        Map<String, SupervisorInfo> superInfos = state.allSupervisorInfo();
        List<SupervisorDetails> superDetails = new ArrayList<>();
        for (Entry<String, SupervisorInfo> entry: superInfos.entrySet()) {
            SupervisorInfo info = entry.getValue();
            superDetails.add(new SupervisorDetails(entry.getKey(), info.get_meta(), info.get_resources_map()));
        }
        // Note that allSlotsAvailableForScheduling
        // only uses the supervisor-details. The rest of the arguments
        // are there to satisfy the INimbus interface.
        Map<String, Set<Long>> superToPorts = new HashMap<>();
        for (WorkerSlot slot : getINimbus().allSlotsAvailableForScheduling(superDetails, topologies, 
                new HashSet<>(missingAssignmentTopologies))) {
            String superId = slot.getNodeId();
            Set<Long> ports = superToPorts.get(superId);
            if (ports == null) {
                ports = new HashSet<>();
                superToPorts.put(superId, ports);
            }
            ports.add((long) slot.getPort());
        }
        for (Entry<String, SupervisorInfo> entry: superInfos.entrySet()) {
            String superId = entry.getKey();
            SupervisorInfo info = entry.getValue();
            String hostname = info.get_hostname();
            Set<Long> deadPorts = superToDeadPorts.get(superId);
            Set<Long> allPorts = superToPorts.get(superId);
            if (allPorts == null) {
                allPorts = new HashSet<>();
            } else {
                allPorts = new HashSet<>(allPorts);
            }
            if (deadPorts != null) {
                allPorts.removeAll(deadPorts);
            }
            //hide the dead-ports from the all-ports
            // these dead-ports can be reused in next round of assignments}
            ret.put(superId, new SupervisorDetails(superId, hostname, info.get_scheduler_meta(), 
                    allPorts, info.get_resources_map()));
        }
        return ret;
    }
    
    //TODO private
    //TODO misspelled!!!
    public Map<String, SchedulerAssignment> computeNewSchedulerAssignmnets(Map<String, Assignment> existingAssignments,
            Topologies topologies, String scratchTopologyId) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
        Map<String, Object> conf = getConf();
        Map<String, Set<List<Integer>>> topoToExec = computeTopologyToExecutors(existingAssignments.keySet());
        
        updateAllHeartbeats(existingAssignments, topoToExec);

        Map<String, Set<List<Integer>>> topoToAliveExecutors = computeTopologyToAliveExecutors(existingAssignments, topologies,
                topoToExec, scratchTopologyId);
        Map<String, Set<Long>> supervisorToDeadPorts = computeSupervisorToDeadPorts(existingAssignments, topoToExec,
                topoToAliveExecutors);
        Map<String, SchedulerAssignmentImpl> topoToSchedAssignment = computeTopologyToSchedulerAssignment(existingAssignments,
                topoToAliveExecutors);
        Set<String> missingAssignmentTopologies = new HashSet<>();
        for (TopologyDetails topo: topologies.getTopologies()) {
            String id = topo.getId();
            Set<List<Integer>> allExecs = topoToExec.get(id);
            Set<List<Integer>> aliveExecs = topoToAliveExecutors.get(id);
            int numDesiredWorkers = topo.getNumWorkers();
            int numAssignedWorkers = numUsedWorkers(topoToSchedAssignment.get(id));
            if (allExecs == null || allExecs.isEmpty() || !allExecs.equals(aliveExecs) || numDesiredWorkers < numAssignedWorkers) {
                //We have something to schedule...
                missingAssignmentTopologies.add(id);
            }
        }
        Map<String, SupervisorDetails> supervisors = readAllSupervisorDetails(supervisorToDeadPorts, topologies, missingAssignmentTopologies);
        Cluster cluster = new Cluster(getINimbus(), supervisors, topoToSchedAssignment, conf);
        cluster.setStatusMap(getIdToSchedStatus().get());
        getScheduler().schedule(topologies, cluster);

        //merge with existing statuses
        getIdToSchedStatus().set(merge(getIdToSchedStatus().get(), cluster.getStatusMap()));
        getNodeIdToResources().set(cluster.getSupervisorsResourcesMap());
        
        if (!Utils.getBoolean(conf.get(Config.SCHEDULER_DISPLAY_RESOURCE), false)) {
            cluster.updateAssignedMemoryForTopologyAndSupervisor(topologies);
        }
        
        //TODO remove both of swaps below at first opportunity.
        // This is a hack for non-ras scheduler topology and worker resources
        Map<String, TopologyResources> resources = new HashMap<>();
        for (Entry<String, Double[]> uglyResources : cluster.getTopologyResourcesMap().entrySet()) {
            Double[] r = uglyResources.getValue();
            resources.put(uglyResources.getKey(), new TopologyResources(r[0], r[1], r[2], r[3], r[4], r[5]));
        }
        getIdToResources().getAndAccumulate(resources, MERGE_ID_TO_RESOURCES);
        
        //TODO remove this also at first chance
        Map<String, Map<WorkerSlot, WorkerResources>> workerResources = new HashMap<>();
        for (Entry<String, Map<WorkerSlot, Double[]>> uglyWorkerResources: cluster.getWorkerResourcesMap().entrySet()) {
            Map<WorkerSlot, WorkerResources> slotToResources = new HashMap<>();
            for (Entry<WorkerSlot, Double[]> uglySlotToResources : uglyWorkerResources.getValue().entrySet()) {
                Double[] r = uglySlotToResources.getValue();
                WorkerResources wr = new WorkerResources();
                wr.set_mem_on_heap(r[0]);
                wr.set_mem_off_heap(r[1]);
                wr.set_cpu(r[2]);
                slotToResources.put(uglySlotToResources.getKey(), wr);
            }
            workerResources.put(uglyWorkerResources.getKey(), slotToResources);
        }
        getIdToWorkerResources().getAndAccumulate(workerResources, MERGE_ID_TO_WORKER_RESOURCES);
        
        return cluster.getAssignments();
    }
    
    //TODO private
    public TopologyResources getResourcesForTopology(String topoId) throws NotAliveException, AuthorizationException, InvalidTopologyException, IOException {
        TopologyResources ret = getIdToResources().get().get(topoId);
        if (ret == null) {
            try {
                IStormClusterState state = getStormClusterState();
                TopologyDetails details = readTopologyDetails(topoId);
                double sumOnHeap = 0.0;
                double sumOffHeap = 0.0;
                double sumCPU = 0.0;
                
                Assignment assignment = state.assignmentInfo(topoId, null);
                if (assignment != null) {
                    if (assignment.is_set_worker_resources()) {
                        for (WorkerResources wr: assignment.get_worker_resources().values()) {
                            if (wr.is_set_cpu()) {
                                sumCPU += wr.get_cpu();
                            }
                            
                            if (wr.is_set_mem_off_heap()) {
                                sumOffHeap += wr.get_mem_off_heap();
                            }
                            
                            if (wr.is_set_mem_on_heap()) {
                                sumOnHeap += wr.get_mem_on_heap();
                            }
                        }
                    }
                }
                ret = new TopologyResources(details.getTotalRequestedMemOnHeap(),
                        details.getTotalRequestedMemOffHeap(),
                        details.getTotalRequestedCpu(),
                        sumOnHeap,
                        sumOffHeap,
                        sumCPU);
            } catch(KeyNotFoundException e) {
                //This can happen when a topology is first coming up
                // It's thrown by the blobstore code
                LOG.error("Failed to get topology details", e);
                ret = new TopologyResources(0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
            }
        }
        return ret;
    }
    
    // TODO private
    public Map<WorkerSlot, WorkerResources> getWorkerResourcesForTopology(String topoId) {
        Map<WorkerSlot, WorkerResources> ret = getIdToWorkerResources().get().get(topoId);
        if (ret == null) {
            IStormClusterState state = getStormClusterState();
            ret = new HashMap<>();
            Assignment assignment = state.assignmentInfo(topoId, null);
            if (assignment != null && assignment.is_set_worker_resources()) {
                for (Entry<NodeInfo, WorkerResources> entry: assignment.get_worker_resources().entrySet()) {
                    NodeInfo ni = entry.getKey();
                    WorkerSlot slot = new WorkerSlot(ni.get_node(), ni.get_port_iterator().next());
                    ret.put(slot, entry.getValue());
                }
                getIdToWorkerResources().getAndUpdate(new Assoc<>(topoId, ret));
            }
        }
        return ret;
    }

    //TODO private
    public void mkAssignments() throws Exception {
        mkAssignments(null);
    }
    
    public void mkAssignments(String scratchTopoId) throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping assignments");
            return;
        }
        // get existing assignment (just the executor->node+port map) -> default to {}
        // filter out ones which have a executor timeout
        // figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
        // only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
        // edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around

        Map<String, Object> conf = getConf();
        IStormClusterState state = getStormClusterState();
        INimbus inumbus = getINimbus();
        //read all the topologies
        List<String> topologyIds = state.activeStorms();
        Map<String, TopologyDetails> tds = new HashMap<>();
        for (String id: topologyIds) {
            tds.put(id, readTopologyDetails(id));
        }
        Topologies topologies = new Topologies(tds);
        List<String> assignedTopologyIds = state.assignments(null);
        Map<String, Assignment> existingAssignments = new HashMap<>();
        for (String id: assignedTopologyIds) {
            //for the topology which wants rebalance (specified by the scratch-topology-id)
            // we exclude its assignment, meaning that all the slots occupied by its assignment
            // will be treated as free slot in the scheduler code.
            if (!id.equals(scratchTopoId)) {
                existingAssignments.put(id, state.assignmentInfo(id, null));
            }
        }
        // make the new assignments for topologies
        Map<String, SchedulerAssignment> newSchedulerAssignments = computeNewSchedulerAssignmnets(existingAssignments, topologies, scratchTopoId);
        Map<String, Map<List<Long>, List<Object>>> topologyToExecutorToNodePort = computeNewTopoToExecToNodePort(newSchedulerAssignments, existingAssignments);
        for (String id: assignedTopologyIds) {
            if (!topologyToExecutorToNodePort.containsKey(id)) {
                topologyToExecutorToNodePort.put(id, null);
            }
        }
        Map<String, Map<List<Object>, List<Double>>> newAssignedWorkerToResources = computeTopoToNodePortToResources(newSchedulerAssignments);
        int nowSecs = Time.currentTimeSecs();
        Map<String, SupervisorDetails> basicSupervisorDetailsMap = basicSupervisorDetailsMap(state);
        //construct the final Assignments by adding start-times etc into it
        Map<String, Assignment> newAssignments  = new HashMap<>();
        for (Entry<String, Map<List<Long>, List<Object>>> entry: topologyToExecutorToNodePort.entrySet()) {
            String topoId = entry.getKey();
            Map<List<Long>, List<Object>> execToNodePort = entry.getValue();
            Assignment existingAssignment = existingAssignments.get(topoId);
            Set<String> allNodes = new HashSet<>();
            for (List<Object> nodePort: execToNodePort.values()) {
                allNodes.add((String) nodePort.get(0));
            }
            Map<String, String> allNodeHost = new HashMap<>();
            if (existingAssignment != null) {
                allNodeHost.putAll(existingAssignment.get_node_host());
            }
            for (String node: allNodes) {
                String host = inimbus.getHostName(basicSupervisorDetailsMap, node);
                if (host != null) {
                    allNodeHost.put(node, host);
                }
            }
            Map<List<Long>, NodeInfo> execNodeInfo = null;
            if (existingAssignment != null) {
                execNodeInfo = existingAssignment.get_executor_node_port();
            }
            List<List<Long>> reassignExecutors = changedExecutors(execNodeInfo, execToNodePort);
            Map<List<Long>, Long> startTimes = new HashMap<>();
            if (existingAssignment != null) {
                startTimes.putAll(existingAssignment.get_executor_start_time_secs());
            }
            for (List<Long> id: reassignExecutors) {
                startTimes.put(id, (long)nowSecs);
            }
            Map<List<Object>, List<Double>> workerToResources = newAssignedWorkerToResources.get(topoId);
            Assignment newAssignment = new Assignment((String)conf.get(Config.STORM_LOCAL_DIR));
            Map<String, String> justAssignedKeys = new HashMap<>(allNodeHost);
            //Modifies justAssignedKeys
            justAssignedKeys.keySet().retainAll(allNodes);
            newAssignment.set_node_host(justAssignedKeys);
            //convert NodePort to NodeInfo (again!!!).
            Map<List<Long>, NodeInfo> execToNodeInfo = new HashMap<>();
            for (Entry<List<Long>, List<Object>> execAndNodePort: execToNodePort.entrySet()) {
                List<Object> nodePort = execAndNodePort.getValue();
                NodeInfo ni = new NodeInfo();
                ni.set_node((String) nodePort.get(0));
                ni.add_to_port((Long)nodePort.get(1));
                execToNodeInfo.put(execAndNodePort.getKey(), ni);
            }
            newAssignment.set_executor_node_port(execToNodeInfo);
            newAssignment.set_executor_start_time_secs(startTimes);
            //do another conversion (lets just make this all common)
            Map<NodeInfo, WorkerResources> workerResources = new HashMap<>();
            for (Entry<List<Object>, List<Double>> wr: workerToResources.entrySet()) {
                List<Object> nodePort = wr.getKey();
                NodeInfo ni = new NodeInfo();
                ni.set_node((String) nodePort.get(0));
                ni.add_to_port((Long) nodePort.get(1));
                List<Double> r = wr.getValue();
                WorkerResources resources = new WorkerResources();
                resources.set_mem_on_heap(r.get(0));
                resources.set_mem_off_heap(r.get(1));
                resources.set_cpu(r.get(2));
                workerResources.put(ni, resources);
            }
            newAssignment.set_worker_resources(workerResources);
            newAssignments.put(topoId, newAssignment);
        }
        
        if (!newAssignments.equals(existingAssignments)) {
            LOG.debug("RESETTING id->resources and id->worker-resources cache!");
            getIdToResources().set(new HashMap<>());
            getIdToWorkerResources().set(new HashMap<>());
        }
        //tasks figure out what tasks to talk to by looking at topology at runtime
        // only log/set when there's been a change to the assignment
        for (Entry<String, Assignment> entry: newAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Assignment existingAssignment = existingAssignments.get(topoId);
            //NOT Used TopologyDetails topologyDetails = topologies.getById(topoId);
            if (assignment.equals(existingAssignment)) {
                LOG.debug("Assignment for {} hasn't changed", topoId);
            } else {
                LOG.info("Setting new assignment for topology id {}: {}", topoId, assignment);
                state.setAssignment(topoId, assignment);
            }
        }
        //TODO yes we loop through again (Do we want to combine the various loops???)
        Map<String, Collection<WorkerSlot>> addedSlots = new HashMap<>();
        for (Entry<String, Assignment> entry: newAssignments.entrySet()) {
            String topoId = entry.getKey();
            Assignment assignment = entry.getValue();
            Assignment existingAssignment = existingAssignments.get(topoId);
            if (existingAssignment == null) {
                existingAssignment = new Assignment();
                existingAssignment.set_executor_node_port(new HashMap<>());
                existingAssignment.set_executor_start_time_secs(new HashMap<>());
            }
            Set<WorkerSlot> newSlots = newlyAddedSlots(existingAssignment, assignment);
            addedSlots.put(topoId, newSlots);
        }
        inumbus.assignSlots(topologies, addedSlots);
    }
    
    //TODO private
    public void notifyTopologyActionListener(String topoId, String action) {
        ITopologyActionNotifierPlugin notifier = getNimbusTopologyActionNotifier();
        if (notifier != null) {
            try {
                notifier.notify(topoId, action);
            } catch (Exception e) {
                LOG.warn("Ignoring exception from Topology action notifier for storm-Id {}", topoId, e);
            }
        }
    }

    //TODO private
    //TODO rename
    public void startStorm(String topoName, String topoId, TopologyStatus initStatus) throws KeyNotFoundException, AuthorizationException, IOException, InvalidTopologyException {
        assert(TopologyStatus.ACTIVE == initStatus || TopologyStatus.INACTIVE == initStatus);
        IStormClusterState state = getStormClusterState();
        Map<String, Object> conf = getConf();
        BlobStore store = getBlobStore();
        Map<String, Object> topoConf = readTopoConf(topoId, store);
        StormTopology topology = StormCommon.systemTopology(topoConf, readStormTopology(topoId, store));
        Map<String, Integer> numExecutors = new HashMap<>();
        for (Entry<String, Object> entry: StormCommon.allComponents(topology).entrySet()) {
            numExecutors.put(entry.getKey(), StormCommon.numStartExecutors(entry.getValue()));
        }
        LOG.info("Activating {}: {}", topoName, topoId);
        StormBase base = new StormBase();
        base.set_name(topoName);
        base.set_launch_time_secs(Time.currentTimeSecs());
        base.set_status(initStatus);
        base.set_num_workers(Utils.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 0));
        base.set_component_executors(numExecutors);
        base.set_owner((String) topoConf.get(Config.TOPOLOGY_SUBMITTER_USER));
        base.set_component_debug(new HashMap<>());
        state.activateStorm(topoId, base);
        notifyTopologyActionListener(topoName, "activate");
    }
    
    //TODO private
    public void assertTopoActive(String topoName, boolean expectActive) throws NotAliveException, AlreadyAliveException {
        if (isTopologyActive(getStormClusterState(), topoName) != expectActive) {
            if (expectActive) {
                throw new NotAliveException(topoName + " is not alive");
            }
            throw new AlreadyAliveException(topoName + " is already alive");
        }
    }
    
    public Map<String, Object> tryReadTopoConfFromName(String topoName) throws NotAliveException, AuthorizationException, IOException {
        IStormClusterState state = getStormClusterState();
        String topoId = state.getTopoId(topoName);
        if (topoId == null) {
            throw new NotAliveException(topoName + " is not alive");
        }
        return tryReadTopoConf(topoId, getBlobStore());
    }

    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation) throws AuthorizationException {
        checkAuthorization(topoName, topoConf, operation, null);
    }
    
    public void checkAuthorization(String topoName, Map<String, Object> topoConf, String operation, ReqContext context) throws AuthorizationException {
        //if (true) {throw new RuntimeException("HERE");}
        IAuthorizer aclHandler = getAuthorizationHandler();
        IAuthorizer impersonationAuthorizer = getImpersonationAuthorizationHandler();
        if (context == null) {
            context = ReqContext.context();
        }
        Map<String, Object> checkConf = new HashMap<>();
        if (topoConf != null) {
            checkConf.putAll(topoConf);
        } else if (topoName != null) {
            checkConf.put(Config.TOPOLOGY_NAME, topoName);
        }
       
        if (context.isImpersonating()) {
            LOG.warn("principal: {} is trying to impersonate principal: {}", context.realPrincipal(), context.principal());
            if (impersonationAuthorizer == null) {
                //TODO now that we are 2.x lets fail closed here
                LOG.warn("impersonation attempt but {} has no authorizer configured. potential security risk, "
                        + "please see SECURITY.MD to learn how to configure impersonation authorizer.", Config.NIMBUS_IMPERSONATION_AUTHORIZER);
            } else {
                if (!impersonationAuthorizer.permit(context, operation, checkConf)) {
                    ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(),
                            context.principal(), operation, topoName, "access-denied");
                    throw new AuthorizationException("principal " + context.realPrincipal() + 
                            " is not authorized to impersonate principal " + context.principal() +
                            " from host " + context.remoteAddress() +
                            " Please see SECURITY.MD to learn how to configure impersonation acls.");
                }
            }
        }
        
        if (aclHandler != null) {
            if (!aclHandler.permit(context, operation, checkConf)) {
              ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(), operation,
                      topoName, "access-denied");
              throw new AuthorizationException( operation + (topoName != null ? " on topology " + topoName : "") + 
                      " is not authorized");
            } else {
              ThriftAccessLogger.logAccess(context.requestID(), context.remoteAddress(), context.principal(),
                      operation, topoName, "access-granted");
            }
        }
    }
    
    public boolean isAuthorized(String operation, String topoId) throws NotAliveException, AuthorizationException, IOException {
        Map<String, Object> topoConf = tryReadTopoConf(topoId, getBlobStore());
        String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
        try {
            checkAuthorization(topoName, topoConf, operation);
            return true;
        } catch (AuthorizationException e) {
            return false;
        }
    }
    
    //TODO priavte???
    public void rmDependencyJarsInTopology(String topoId) {
        try {
            BlobStore store = getBlobStore();
            IStormClusterState state = getStormClusterState();
            StormTopology topo = readStormTopologyAsNimbus(topoId, store);
            List<String> dependencyJars = topo.get_dependency_jars();
            LOG.info("Removing dependency jars from blobs - {}", dependencyJars);
            if (dependencyJars != null && !dependencyJars.isEmpty()) {
                for (String key: dependencyJars) {
                    rmBlobKey(store, key, state);
                }
            }
        } catch (Exception e) {
            //Yes eat the exception
            LOG.info("Exception {}", e);
        }
    }
    
    //TODO private???
    public void rmTopologyKeys(String topoId) {
        BlobStore store = getBlobStore();
        IStormClusterState state = getStormClusterState();
        rmBlobKey(store, ConfigUtils.masterStormJarKey(topoId), state);
        rmBlobKey(store, ConfigUtils.masterStormConfKey(topoId), state);
        rmBlobKey(store, ConfigUtils.masterStormCodeKey(topoId), state);
    }
    
    //TODO private???
    public void forceDeleteTopoDistDir(String topoId) throws IOException {
        Utils.forceDelete(ConfigUtils.masterStormDistRoot(getConf(), topoId));
    }
}
