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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.Principal;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.BlobSynchronizer;
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
import org.apache.storm.generated.BeginDownloadResult;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ComponentPageInfo;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.LSTopoHistory;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.SupervisorPageInfo;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.generated.TopologyPageInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.metric.ClusterMetricsConsumerExecutor;
import org.apache.storm.metric.api.DataPoint;
import org.apache.storm.metric.api.IClusterMetricsConsumer;
import org.apache.storm.metric.api.IClusterMetricsConsumer.ClusterInfo;
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
import org.apache.storm.security.auth.IGroupMappingServiceProvider;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.NimbusPrincipal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;

public class Nimbus implements Iface {
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
    
    private static class CommonTopoInfo {
        public Map<String, Object> topoConf;
        public String topoName;
        public StormTopology topology;
        public Map<Integer, String> taskToComponent;
        public StormBase base;
        public int launchTimeSecs;
        public Assignment assignment;
        public Map<List<Integer>, Map<String, Object>> beats;
        public HashSet allComponents;

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
    //TODO fix spelling
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
        Set<String> ret = new HashSet<>();
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
    
    //TODO private???
    /**
     * Deletes jar files in dirLoc older than seconds.
     * @param dirLoc the location to look in for file
     * @param seconds how old is too old and should be deleted
     */
    public static void cleanInbox (String dirLoc, int seconds) {
        final long now = Time.currentTimeMillis();
        final long ms = Time.secsToMillis(seconds);
        File dir = new File(dirLoc);
        for (File f : dir.listFiles((f) -> f.isFile() && ((f.lastModified() + ms) <= now))) {
            if (f.delete()) {
                LOG.info("Cleaning inbox ... deleted: {}", f.getName());
            } else {
                LOG.error("Cleaning inbox ... error deleting: {}", f.getName());
            }
        }
    }
    
    //TODO private
    public static ExecutorInfo toExecInfo(List<Long> exec) {
        return new ExecutorInfo(exec.get(0).intValue(), exec.get(1).intValue());
    }
    
    private static final Pattern TOPOLOGY_NAME_REGEX = Pattern.compile("^[^/.:\\\\]+$");
    //TODO private
    public static void validateTopologyName(String name) throws InvalidTopologyException {
        Matcher m = TOPOLOGY_NAME_REGEX.matcher(name);
        if (!m.matches()) {
            throw new InvalidTopologyException("Topology name must match " + TOPOLOGY_NAME_REGEX);
        }
    }
    
    //TODO private???
    public static StormTopology tryReadTopology(String topoId, BlobStore store) throws NotAliveException, AuthorizationException, IOException {
        try {
            return readStormTopologyAsNimbus(topoId, store);
        } catch (KeyNotFoundException e) {
            throw new NotAliveException(topoId);
        }
    }
    
    //TODO private
    public static void validateTopologySize(Map<String, Object> topoConf, Map<String, Object> nimbusConf, StormTopology topology) throws InvalidTopologyException {
        int workerCount = Utils.getInt(topoConf.get(Config.TOPOLOGY_WORKERS), 1);
        Integer allowedWorkers = Utils.getInt(nimbusConf.get(Config.NIMBUS_SLOTS_PER_TOPOLOGY), null);
        int executorsCount = 0;
        for (Object comp : StormCommon.allComponents(topology).values()) {
            executorsCount += StormCommon.numStartExecutors(comp);
        }
        Integer allowedExecutors = Utils.getInt(nimbusConf.get(Config.NIMBUS_EXECUTORS_PER_TOPOLOGY), null);
        if (allowedExecutors != null && executorsCount > allowedExecutors) {
            throw new InvalidTopologyException("Failed to submit topology. Topology requests more than " +
                    allowedExecutors + " executors.");
        }
        
        if (allowedWorkers != null && workerCount > allowedWorkers) {
            throw new InvalidTopologyException("Failed to submit topology. Topology requests more than " +
                    allowedWorkers + " workers.");
        }
    }
    
    //TODO private
    public static void setLoggerTimeouts(LogLevel level) {
        int timeoutSecs = level.get_reset_log_level_timeout_secs();
        if (timeoutSecs > 0) {
            level.set_reset_log_level_timeout_epoch(Time.currentTimeSecs() + timeoutSecs);
        } else {
            level.unset_reset_log_level_timeout_epoch();
        }
    }
    
    //TODO private
    public static List<String> topologiesOnSupervisor(Map<String, Assignment> assignments, String supervisorId) {
        Set<String> ret = new HashSet<>();
        for (Entry<String, Assignment> entry: assignments.entrySet()) {
            Assignment assignment = entry.getValue();
            //TODO it might be a lot faster to use
            //assignment.get_node_host().containsKey(supervisorId);
            for (NodeInfo nodeInfo: assignment.get_executor_node_port().values()) {
                if (supervisorId.equals(nodeInfo.get_node())) {
                    ret.add(entry.getKey());
                    break;
                }
            }
        }
        
        return new ArrayList<>(ret);
    }
    
    //TODO private
    public static IClusterMetricsConsumer.ClusterInfo mkClusterInfo() {
        return new IClusterMetricsConsumer.ClusterInfo(Time.currentTimeSecs());
    }
    
    //TODO private???
    public static List<DataPoint> extractClusterMetrics(ClusterSummary summ) {
        List<DataPoint> ret = new ArrayList<>();
        ret.add(new DataPoint("supervisors", summ.get_supervisors_size()));
        ret.add(new DataPoint("topologies", summ.get_topologies_size()));
        
        int totalSlots = 0;
        int usedSlots = 0;
        for (SupervisorSummary sup: summ.get_supervisors()) {
            usedSlots += sup.get_num_used_workers();
            totalSlots += sup.get_num_workers();
        }
        ret.add(new DataPoint("slotsTotal", totalSlots));
        ret.add(new DataPoint("slotsUsed", usedSlots));
        ret.add(new DataPoint("slotsFree", totalSlots - usedSlots));
        
        int totalExecutors = 0;
        int totalTasks = 0;
        for (TopologySummary topo: summ.get_topologies()) {
            totalExecutors += topo.get_num_executors();
            totalTasks += topo.get_num_tasks();
        }
        ret.add(new DataPoint("executorsTotal", totalExecutors));
        ret.add(new DataPoint("tasksTotal", totalTasks));
        return ret;
    }

    //TODO private
    public static Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> extractSupervisorMetrics(ClusterSummary summ) {
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> ret = new HashMap<>();
        for (SupervisorSummary sup: summ.get_supervisors()) {
            IClusterMetricsConsumer.SupervisorInfo info = new IClusterMetricsConsumer.SupervisorInfo(sup.get_host(), sup.get_supervisor_id(), Time.currentTimeSecs());
            List<DataPoint> metrics = new ArrayList<>();
            metrics.add(new DataPoint("slotsTotal", sup.get_num_workers()));
            metrics.add(new DataPoint("slotsUsed", sup.get_num_used_workers()));
            metrics.add(new DataPoint("totalMem", sup.get_total_resources().get(Config.SUPERVISOR_MEMORY_CAPACITY_MB)));
            metrics.add(new DataPoint("totalCpu", sup.get_total_resources().get(Config.SUPERVISOR_CPU_CAPACITY)));
            metrics.add(new DataPoint("usedMem", sup.get_used_mem()));
            metrics.add(new DataPoint("usedCpu", sup.get_used_cpu()));
            ret.put(info, metrics);
        }
        return ret;
    }
    
    private final Map<String, Object> conf;
    private final NimbusInfo nimbusHostPortInfo;
    private final INimbus inimbus;
    private IAuthorizer authorizationHandler;
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
    private final IGroupMappingServiceProvider groupMapper;
    private final IPrincipalToLocal principalToLocal;
    
    private static IStormClusterState makeStormClusterState(Map<String, Object> conf) throws Exception {
        //TODO need to change CLusterUtils to have a Map option
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = ZK_ACLS;
        }
        return ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.NIMBUS));
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus) throws Exception {
        this(conf, inimbus, null, null, null, null, null);
    }
    
    public Nimbus(Map<String, Object> conf, INimbus inimbus, IStormClusterState stormClusterState, NimbusInfo hostPortInfo,
            BlobStore blobStore, ILeaderElector leaderElector, IGroupMappingServiceProvider groupMapper) throws Exception {
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
        if (groupMapper == null) {
            groupMapper = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
        }
        this.groupMapper = groupMapper;
        this.principalToLocal = AuthUtils.GetPrincipalToLocalPlugin(conf);
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
    
    public void setAuthorizationHandler(IAuthorizer authorizationHandler) {
        this.authorizationHandler = authorizationHandler;
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
    
    public IGroupMappingServiceProvider getGroupMapper() {
        return groupMapper;
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
    
    //TODO private
    public Set<String> filterAuthorized(String operation, Collection<String> topoIds) throws NotAliveException, AuthorizationException, IOException {
        Set<String> ret = new HashSet<>();
        for (String topoId : topoIds) {
            if (isAuthorized(operation, topoId)) {
                ret.add(topoId);
            }
        }
        return ret;
    }
    
    //TODO private???
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
    
    //TODO private???
    public void doCleanup() throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping cleanup");
            return;
        }
        IStormClusterState state = getStormClusterState();
        Set<String> toClean;
        synchronized(getSubmitLock()) {
            toClean = topoIdsToClean(state, getBlobStore());
        }
        if (toClean != null) {
            for (String topoId: toClean) {
                LOG.info("Cleaning up {}", topoId);
                state.teardownHeartbeats(topoId);
                state.teardownTopologyErrors(topoId);
                state.removeBackpressure(topoId);
                rmDependencyJarsInTopology(topoId);
                forceDeleteTopoDistDir(topoId);
                rmTopologyKeys(topoId);
                getHeartbeatsCache().getAndUpdate(new Dissoc<>(topoId));
            }
        }
    }
    
    //TODO private???
    /**
     * Deletes topologies from history older than mins minutes.
     * @param mins the number of mins for old topologies
     */
    public void cleanTopologyHistory(int mins) {
        int cutoffAgeSecs = Time.currentTimeSecs() - (mins * 60);
        synchronized(getTopologyHistoryLock()) {
            LocalState state = getTopologyHistoryState();
            state.filterOldTopologies(cutoffAgeSecs);
        }
    }
    
    //TODO private
    /**
     * Sets up blobstore state for all current keys.
     * @throws KeyNotFoundException 
     * @throws AuthorizationException 
     */
    public void setupBlobstore() throws AuthorizationException, KeyNotFoundException {
        //TODO it this should really be part of the blob store
        //TODO it would be great to not need to read all blobs into memory to make this happen.
        IStormClusterState state = getStormClusterState();
        BlobStore store = getBlobStore();
        Set<String> localKeys = new HashSet<>();
        for (Iterator<String> it = store.listKeys(); it.hasNext();) {
            localKeys.add(it.next());
        }
        Set<String> activeKeys = new HashSet<>(state.activeKeys());
        Set<String> activeLocalKeys = new HashSet<>(localKeys);
        activeLocalKeys.retainAll(activeKeys);
        Set<String> keysToDelete = new HashSet<>(localKeys);
        keysToDelete.removeAll(activeKeys);
        Map<String, Object> conf = getConf();
        NimbusInfo nimbusInfo = getNimbusHostPortInfo();
        LOG.debug("Deleting keys not on the zookeeper {}", keysToDelete);
        for (String toDelete: keysToDelete) {
            store.deleteBlob(toDelete, NIMBUS_SUBJECT);
        }
        LOG.debug("Creating list of key entries for blobstore inside zookeeper {} local {}", activeKeys, activeLocalKeys);
        for (String key: activeLocalKeys) {
            state.setupBlobstore(key, nimbusInfo, getVerionForKey(key, nimbusInfo, conf));
        }
    }
    
    //TODO private
    public void addTopoToHistoryLog(String topoId, Map<String, Object> topoConf) {
        LOG.info("Adding topo to history log: {}", topoId);
        LocalState state = getTopologyHistoryState();
        List<String> users = ConfigUtils.getTopoLogsUsers(topoConf);
        List<String> groups = ConfigUtils.getTopoLogsGroups(topoConf);
        synchronized(getTopologyHistoryLock()) {
            state.addTopologyHistory(new LSTopoHistory(topoId, Time.currentTimeSecs(), users, groups));
        }
    }
    
    //TODO private
    public Set<String> userGroups(String user) throws IOException {
        if (user == null || user.isEmpty()) {
            return Collections.emptySet();
        }
        return getGroupMapper().getGroups(user);
    }
    
    //TODO private?
    /**
     * Check to see if any of the users groups intersect with the list of groups passed in
     * @param user the user to check
     * @param groupsToCheck the groups to see if user is a part of
     * @return true if user is a part of groups, else false
     * @throws IOException on any error
     */
    public boolean isUserPartOf(String user, Collection<String> groupsToCheck) throws IOException {
        Set<String> userGroups = new HashSet<>(userGroups(user));
        userGroups.retainAll(groupsToCheck);
        return !userGroups.isEmpty();
    }

    //TODO private
    public List<String> readTopologyHistory(String user, Collection<String> adminUsers) throws IOException {
        LocalState state = getTopologyHistoryState();
        List<String> ret = new ArrayList<>();
        for (LSTopoHistory history: state.getTopoHistoryList()) {
            
            if (user == null || //Security off
                    adminUsers.contains(user) || //is admin
                    isUserPartOf(user, history.get_groups()) || //is in allowed group
                    history.get_users().contains(user)) { //is an allowed user
                ret.add(history.get_topology_id());
            }
        }
        return ret;
    }
    
    //TODO private ???
    public void renewCredentials() throws Exception {
        if (!isLeader()) {
            LOG.info("not a leader, skipping credential renewal.");
            return;
        }
        IStormClusterState state = getStormClusterState();
        BlobStore store = getBlobStore();
        Collection<ICredentialsRenewer> renewers = getCredRenewers();
        Object lock = getCredUpdateLock();
        List<String> assignedIds = state.activeStorms();
        if (assignedIds != null) {
            for (String id: assignedIds) {
                Map<String, Object> topoConf = Collections.unmodifiableMap(tryReadTopoConf(id, store));
                synchronized(lock) {
                    Credentials origCreds = state.credentials(id, null);
                    if (origCreds != null) {
                        Map<String, String> orig = origCreds.get_creds();
                        Map<String, String> newCreds = new HashMap<>(orig);
                        for (ICredentialsRenewer renewer: renewers) {
                            LOG.info("Renewing Creds For {} with {}", id, renewer);
                            renewer.renew(newCreds, topoConf);
                        }
                        if (!newCreds.equals(origCreds)) {
                            state.setCredentials(id, new Credentials(newCreds), topoConf);
                        }
                    }
                }
            }
        }
    }
    
    //TODO private?
    public void blobSync() throws Exception {
        Map<String, Object> conf = getConf();
        if ("distributed".equals(conf.get(Config.STORM_CLUSTER_MODE))) {
            if (!isLeader()) {
                IStormClusterState state = getStormClusterState();
                NimbusInfo nimbusInfo = getNimbusHostPortInfo();
                BlobStore store = getBlobStore();
                //TODO would really be great if we didn't cache all of these in memory
                Set<String> allKeys = new HashSet<>();
                for (Iterator<String> it = store.listKeys(); it.hasNext();) {
                    allKeys.add(it.next());
                }
                Set<String> zkKeys = new HashSet<>(state.blobstore(() -> {
                    try {
                        this.blobSync();
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
                LOG.debug("blob-sync blob-store-keys {} zookeeper-keys {}", allKeys, zkKeys);
                BlobSynchronizer sync = new BlobSynchronizer(store, conf);
                sync.setNimbusInfo(nimbusInfo);
                sync.setBlobStoreKeySet(allKeys);
                sync.setZookeeperKeySet(zkKeys);
                sync.syncBlobs();
            } //else not leader (NOOP)
        } //else local (NOOP)
    }
    
    //TODO private
    public SupervisorSummary makeSupervisorSummary(String supervisorId, SupervisorInfo info) {
        LOG.debug("INFO: {} ID: {}", info, supervisorId);
        int numPorts = 0;
        if (info.is_set_meta()) {
            numPorts = info.get_meta_size();
        }
        int numUsedPorts = 0;
        if (info.is_set_used_ports()) {
            numUsedPorts = info.get_used_ports_size();
        }
        LOG.debug("NUM PORTS: {}", numPorts);
        SupervisorSummary ret = new SupervisorSummary(info.get_hostname(),
                (int) info.get_uptime_secs(), numPorts, numUsedPorts, supervisorId);
        ret.set_total_resources(info.get_resources_map());
        Double[] resources = getNodeIdToResources().get().get(supervisorId);
        if (resources != null) {
            ret.set_used_mem(Utils.nullToZero(resources[2]));
            ret.set_used_cpu(Utils.nullToZero(resources[3]));
        }
        if (info.is_set_version()) {
            ret.set_version(info.get_version());
        }
        return ret;
    }

    //TODO private
    public ClusterSummary getClusterInfoImpl() throws Exception {
        IStormClusterState state = getStormClusterState();
        Map<String, SupervisorInfo> infos = state.allSupervisorInfo();
        List<SupervisorSummary> summaries = new ArrayList<>(infos.size());
        for (Entry<String, SupervisorInfo> entry: infos.entrySet()) {
            summaries.add(makeSupervisorSummary(entry.getKey(), entry.getValue()));
        }
        int uptime = getUptime().upTime();
        Map<String, StormBase> bases = state.topologyBases();

        List<NimbusSummary> nimbuses = state.nimbuses();
        //update the isLeader field for each nimbus summary
        NimbusInfo leader = getLeaderElector().getLeader();
        for (NimbusSummary nimbusSummary: nimbuses) {
            nimbusSummary.set_uptime_secs(Time.deltaSecs(nimbusSummary.get_uptime_secs()));
            nimbusSummary.set_isLeader(leader.getHost().equals(nimbusSummary.get_host()) &&
                    leader.getPort() == nimbusSummary.get_port());
        }
        
        List<TopologySummary> topologySummaries = new ArrayList<>();
        for (Entry<String, StormBase> entry: bases.entrySet()) {
            StormBase base = entry.getValue();
            if (base == null) {
                continue;
            }
            String topoId = entry.getKey();
            Assignment assignment = state.assignmentInfo(topoId, null);
            
            int numTasks = 0;
            int numExecutors = 0;
            int numWorkers = 0;
            if (assignment != null && assignment.is_set_executor_node_port()) {
                for (List<Long> ids: assignment.get_executor_node_port().keySet()) {
                    numTasks += StormCommon.executorIdToTasks(ids).size();
                }
            
                numExecutors = assignment.get_executor_node_port_size();
                numWorkers = new HashSet<>(assignment.get_executor_node_port().values()).size();
            }
            
            TopologySummary summary = new TopologySummary(topoId, base.get_name(), numTasks, numExecutors, numWorkers,
                    Time.deltaSecs(base.get_launch_time_secs()), extractStatusStr(base));
            
            if (base.is_set_owner()) {
                summary.set_owner(base.get_owner());
            }
            String status = getIdToSchedStatus().get().get(topoId);
            if (status != null) {
                summary.set_sched_status(status);
            }
            TopologyResources resources = getResourcesForTopology(topoId);
            if (resources != null) {
                summary.set_requested_memonheap(resources.getRequestedMemOnHeap());
                summary.set_requested_memoffheap(resources.getRequestedMemOffHeap());
                summary.set_requested_cpu(resources.getRequestedCpu());
                summary.set_assigned_memonheap(resources.getAssignedMemOnHeap());
                summary.set_assigned_memoffheap(resources.getAssignedMemOffHeap());
                summary.set_assigned_cpu(resources.getAssignedCpu());
            }
            summary.set_replication_count(getBlobReplicationCount(ConfigUtils.masterStormCodeKey(topoId)));
            topologySummaries.add(summary);
        }
        
        ClusterSummary ret = new ClusterSummary(summaries, topologySummaries, nimbuses);
        ret.set_nimbus_uptime_secs(uptime);
        return ret;
    }
    
    //TODO private
    //TODO Executors????
    public void sendClusterMetricsToExecutors() throws Exception {
        ClusterInfo clusterInfo = mkClusterInfo();
        ClusterSummary clusterSummary = getClusterInfoImpl();
        List<DataPoint> clusterMetrics = extractClusterMetrics(clusterSummary);
        Map<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> supervisorMetrics = extractSupervisorMetrics(clusterSummary);
        for (ClusterMetricsConsumerExecutor consumerExecutor: getClusterConsumerExecutors()) {
            consumerExecutor.handleDataPoints(clusterInfo, clusterMetrics);
            for (Entry<IClusterMetricsConsumer.SupervisorInfo, List<DataPoint>> entry: supervisorMetrics.entrySet()) {
                consumerExecutor.handleDataPoints(entry.getKey(), entry.getValue());
            }
        }
    }

    //TODO private
    public CommonTopoInfo getCommonTopoInfo(String topoId, String operation) throws NotAliveException, AuthorizationException, IOException, InvalidTopologyException {
        BlobStore store = getBlobStore();
        IStormClusterState state = getStormClusterState();
        CommonTopoInfo ret = new CommonTopoInfo();
        ret.topoConf = tryReadTopoConf(topoId, store);
        ret.topoName = (String)ret.topoConf.get(Config.TOPOLOGY_NAME);
        checkAuthorization(ret.topoName, ret.topoConf, operation);
        ret.topology = tryReadTopology(topoId, store);
        ret.taskToComponent = StormCommon.stormTaskInfo(ret.topology, ret.topoConf);
        ret.base = state.stormBase(topoId, null);
        ret.launchTimeSecs = ret.base.get_launch_time_secs();
        ret.assignment = state.assignmentInfo(topoId, null);
        ret.beats = OR(getHeartbeatsCache().get().get(topoId), Collections.emptyMap());
        ret.allComponents = new HashSet<>(ret.taskToComponent.values());
        return ret;
    }
    
    //THRIFT SERVER METHODS...
    
    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        submitTopologyCalls.mark();
        submitTopologyWithOpts(name, uploadedJarLocation, jsonConf, topology, new SubmitOptions(TopologyInitialStatus.ACTIVE));
    }

    @Override
    public void submitTopologyWithOpts(String topoName, String uploadedJarLocation, String jsonConf, StormTopology topology,
            SubmitOptions options)
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {
        try {
            submitTopologyWithOptsCalls.mark();
            assertIsLeader();
            assert(options != null);
            validateTopologyName(topoName);
            checkAuthorization(topoName, null, "submitTopology");
            assertTopoActive(topoName, false);
            @SuppressWarnings("unchecked")
            Map<String, Object> topoConf = (Map<String, Object>) JSONValue.parse(jsonConf);
            try {
                ConfigValidation.validateFields(topoConf);
            } catch (IllegalArgumentException ex) {
                throw new InvalidTopologyException(ex.getMessage());
            }
            getValidator().validate(topoName, topoConf, topology);
            long uniqueNum = getSubmittedCount().incrementAndGet();
            String topoId = topoName + "-" + uniqueNum + "-" + Time.currentTimeSecs();
            Map<String, String> creds = null;
            if (options.is_set_creds()) {
                creds = options.get_creds().get_creds();
            }
            topoConf.put(Config.STORM_ID, topoId);
            topoConf.put(Config.TOPOLOGY_NAME, topoName);
            topoConf = normalizeConf(getConf(), topoConf, topology);
            
            ReqContext req = ReqContext.context();
            Principal principal = req.principal();
            String submitterPrincipal = principal == null ? null : principal.toString();
            String submitterUser = principalToLocal.toLocal(principal);
            String systemUser = System.getProperty("user.name");
            @SuppressWarnings("unchecked")
            Set<String> topoAcl = new HashSet<>((List<String>)topoConf.getOrDefault(Config.TOPOLOGY_USERS, Collections.emptyList()));
            topoAcl.add(submitterPrincipal);
            topoAcl.add(submitterUser);
            
            topoConf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, OR(submitterPrincipal, ""));
            topoConf.put(Config.TOPOLOGY_SUBMITTER_USER, OR(submitterUser, systemUser)); //Don't let the user set who we launch as
            topoConf.put(Config.TOPOLOGY_USERS, new ArrayList<>(topoAcl));
            topoConf.put(Config.STORM_ZOOKEEPER_SUPERACL, conf.get(Config.STORM_ZOOKEEPER_SUPERACL));
            if (!Utils.isZkAuthenticationConfiguredStormServer(conf)) {
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
                topoConf.remove(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
            }
            if (!(Boolean)conf.getOrDefault(Config.STORM_TOPOLOGY_CLASSPATH_BEGINNING_ENABLED, false)) {
                topoConf.remove(Config.TOPOLOGY_CLASSPATH_BEGINNING);
            }
            Map<String, Object> totalConf = merge(conf, topoConf);
            topology = normalizeTopology(totalConf, topology);
            IStormClusterState state = getStormClusterState();
            
            if (creds != null) {
                Map<String, Object> finalConf = Collections.unmodifiableMap(topoConf);
                for (INimbusCredentialPlugin autocred: getNimbusAutocredPlugins()) {
                    autocred.populateCredentials(creds, finalConf);
                }
            }
            
            if (Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false) &&
                    (submitterUser == null || submitterUser.isEmpty())) {
                throw new AuthorizationException("Could not determine the user to run this topology as.");
            }
            StormCommon.systemTopology(totalConf, topology); //this validates the structure of the topology
            validateTopologySize(topoConf, conf, topology);
            if (Utils.isZkAuthenticationConfiguredStormServer(conf) &&
                    !Utils.isZkAuthenticationConfiguredTopology(topoConf)) {
                throw new IllegalArgumentException("The cluster is configured for zookeeper authentication, but no payload was provided.");
            }
            LOG.info("Received topology submission for {} with conf {}", topoName, Utils.redactValue(topoConf, Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD));
            
            // lock protects against multiple topologies being submitted at once and
            // cleanup thread killing topology in b/w assignment and starting the topology
            synchronized(getSubmitLock()) {
                assertTopoActive(topoName, false);
                //cred-update-lock is not needed here because creds are being added for the first time.
                if (creds != null) {
                    state.setCredentials(topoId, new Credentials(creds), topoConf);
                }
                LOG.info("uploadedJar {}", uploadedJarLocation);
                setupStormCode(conf, topoId, uploadedJarLocation, totalConf, topology);
                waitForDesiredCodeReplication(totalConf, topoId);
                state.setupHeatbeats(topoId);
                if (Utils.getBoolean(totalConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false)) {
                    state.setupBackpressure(topoId);
                }
                notifyTopologyActionListener(topoName, "submitTopology");
                TopologyStatus status = null;
                switch (options.get_initial_status()) {
                    case INACTIVE:
                        status = TopologyStatus.INACTIVE;
                        break;
                    case ACTIVE:
                        status = TopologyStatus.ACTIVE;
                        break;
                    default:
                        throw new IllegalArgumentException("Inital Status of " + options.get_initial_status() + " is not allowed.");
                            
                }
                startStorm(topoName, topoId, status);
            }
        } catch (Exception e) {
            LOG.warn("Topology submission exception. (topology name='{}')", topoName, e);
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void killTopology(String name) throws NotAliveException, AuthorizationException, TException {
        killTopologyCalls.mark();
        killTopologyWithOpts(name, new KillOptions());
    }
    
    @Override
    public void killTopologyWithOpts(String topoName, KillOptions options)
            throws NotAliveException, AuthorizationException, TException {
        killTopologyWithOptsCalls.mark();
        assertTopoActive(topoName, true);
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            final String operation = "killTopology";
            checkAuthorization(topoName, topoConf, operation);
            Integer waitAmount = null;
            if (options.is_set_wait_secs()) {
                waitAmount = options.get_wait_secs();
            }
            transitionName(topoName, TopologyActions.KILL, waitAmount, true);
            notifyTopologyActionListener(topoName, operation);
            addTopoToHistoryLog(StormCommon.getStormId(getStormClusterState(), topoName), topoConf);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void activate(String topoName) throws NotAliveException, AuthorizationException, TException {
        activateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            final String operation = "activate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.ACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deactivate(String topoName) throws NotAliveException, AuthorizationException, TException {
        deactivateCalls.mark();
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            final String operation = "deactivate";
            checkAuthorization(topoName, topoConf, operation);
            transitionName(topoName, TopologyActions.INACTIVATE, null, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rebalance(String topoName, RebalanceOptions options)
            throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {
        rebalanceCalls.mark();
        assertTopoActive(topoName, true);
        try {
            Map<String, Object> topoConf = tryReadTopoConfFromName(topoName);
            final String operation = "rebalance";
            checkAuthorization(topoName, topoConf, operation);
            Map<String, Integer> execOverrides = options.is_set_num_executors() ? options.get_num_executors() : Collections.emptyMap();
            for (Integer value : execOverrides.values()) {
                if (value == null || value <= 0) {
                    throw new InvalidTopologyException("Number of executors must be greater than 0");
                }
            }
            transitionName(topoName, TopologyActions.REBALANCE, options, true);
            notifyTopologyActionListener(topoName, operation);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLogConfig(String topoId, LogConfig config) throws TException {
        try {
            setLogConfigCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, getBlobStore());
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setLogConfig");
            IStormClusterState state = getStormClusterState();
            LogConfig mergedLogConfig = state.topologyLogConfig(topoId, null);
            if (mergedLogConfig == null) {
                mergedLogConfig = new LogConfig();
            }
            Map<String, LogLevel> namedLoggers = mergedLogConfig.get_named_logger_level();
            for (LogLevel level: namedLoggers.values()) {
                level.set_action(LogLevelAction.UNCHANGED);
            }
            
            if (config.is_set_named_logger_level()) {
                for (Entry<String, LogLevel> entry: config.get_named_logger_level().entrySet()) {
                    LogLevel logConfig = entry.getValue();
                    String loggerName = entry.getKey();
                    LogLevelAction action = logConfig.get_action();
                    if (loggerName.isEmpty()) {
                        throw new RuntimeException("Named loggers need a valid name. Use ROOT for the root logger");
                    }
                    switch (action) {
                        case UPDATE:
                            setLoggerTimeouts(logConfig);
                            mergedLogConfig.put_to_named_logger_level(loggerName, logConfig);
                            break;
                        case REMOVE:
                            Map<String, LogLevel> nl = mergedLogConfig.get_named_logger_level();
                            if (nl != null) {
                                nl.remove(loggerName);
                            }
                            break;
                        default:
                            //NOOP
                            break;
                    }
                }
            }
            LOG.info("Setting log config for {}:{}", topoName, mergedLogConfig);
            state.setTopologyLogConfig(topoId, mergedLogConfig);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogConfig getLogConfig(String name) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void debug(String topoName, String componentId, boolean enable, double samplingPercentage)
            throws NotAliveException, AuthorizationException, TException {
        debugCalls.mark();
        try {
            IStormClusterState state = getStormClusterState();
            String topoId = StormCommon.getStormId(state, topoName);
            Map<String, Object> topoConf = tryReadTopoConf(topoId, getBlobStore());
            // make sure samplingPct is within bounds.
            double spct = Math.max(Math.min(samplingPercentage, 100.0), 0.0);
            // while disabling we retain the sampling pct.
            checkAuthorization(topoName, topoConf, "debug");
            if (topoId == null) {
                throw new NotAliveException(topoName);
            }
            boolean hasCompId = componentId != null && !componentId.isEmpty();
            
            DebugOptions options = new DebugOptions();
            options.set_enable(enable);
            if (enable) {
                options.set_samplingpct(spct);
            }
            StormBase updates = new StormBase();
            //For backwards compatability
            updates.set_component_executors(Collections.emptyMap());
            String key = hasCompId ? componentId : topoId;
            updates.put_to_component_debug(key, options);
            
            LOG.info("Nimbus setting debug to {} for storm-name '{}' storm-id '{}' sanpling pct '{}'" + 
                    (hasCompId ? " component-id '" + componentId + "'" : ""),
                    enable, topoName, topoId, spct);
            synchronized(getSubmitLock()) {
                state.updateStorm(topoId, updates);
            }
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void setWorkerProfiler(String topoId, ProfileRequest profileRequest) throws TException {
        try {
            setWorkerProfilerCalls.mark();
            Map<String, Object> topoConf = tryReadTopoConf(topoId, getBlobStore());
            String topoName = (String) topoConf.get(Config.TOPOLOGY_NAME);
            checkAuthorization(topoName, topoConf, "setWorkerProfiler");
            IStormClusterState state = getStormClusterState();
            state.setWorkerProfileRequest(topoId, profileRequest);
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public List<ProfileRequest> getComponentPendingProfileActions(String id, String componentId, ProfileAction action)
            throws TException {
        try {
            getComponentPendingProfileActionsCalls.mark();
            CommonTopoInfo info = getCommonTopoInfo(id, "getComponentPendingProfileActions");
            Map<String, String> nodeToHost = info.assignment.get_node_host();
            Map<List<? extends Number>, List<Object>> exec2hostPort = new HashMap<>();
            for (Entry<List<Long>, NodeInfo> entry: info.assignment.get_executor_node_port().entrySet()) {
                NodeInfo ni = entry.getValue();
                List<Object> hostPort = Arrays.asList(nodeToHost.get(ni.get_node()), ni.get_port_iterator().next().intValue());
                exec2hostPort.put(entry.getKey(), hostPort);
            }
            List<Map<String, Object>> nodeInfos = StatsUtil.extractNodeInfosFromHbForComp(exec2hostPort, info.taskToComponent, false, componentId);
            List<ProfileRequest> ret = new ArrayList<>();
            for (Map<String, Object> ni : nodeInfos) {
                String niHost = (String) ni.get("host");
                int niPort = ((Integer) ni.get("port")).intValue();
                ProfileRequest newestMatch = null;
                long reqTime = -1;
                for (ProfileRequest req : getStormClusterState().getTopologyProfileRequests(id)) {
                    String expectedHost = req.get_nodeInfo().get_node();
                    int expectedPort = req.get_nodeInfo().get_port_iterator().next().intValue();
                    ProfileAction expectedAction = req.get_action();
                    if (niHost.equals(expectedHost) && niPort == expectedPort && action == expectedAction) {
                        long time = req.get_time_stamp();
                        if (time > reqTime) {
                            reqTime = time;
                            newestMatch = req;
                        }
                    }
                }
                if (newestMatch != null) {
                    ret.add(newestMatch);
                }
            }
            LOG.info("Latest profile actions for topology {} component {} {}", id, componentId, ret);
            return ret;
        } catch (Exception e) {
            if (e instanceof TException) {
                throw (TException)e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uploadNewCredentials(String name, Credentials creds)
            throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String beginCreateBlob(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyAlreadyExistsException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String beginUpdateBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void uploadBlobChunk(String session, ByteBuffer chunk) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void finishBlobUpload(String session) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void cancelBlobUpload(String session) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public BeginDownloadResult beginBlobDownload(String key)
            throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteBuffer downloadBlobChunk(String session) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ListBlobsResult listBlobs(String session) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int updateBlobReplication(String key, int replication)
            throws AuthorizationException, KeyNotFoundException, TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void createStateInZookeeper(String key) throws TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String beginFileUpload() throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void finishFileUpload(String location) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String beginFileDownload(String file) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteBuffer downloadChunk(String id) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNimbusConf() throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TopologyInfo getTopologyInfo(String id) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TopologyInfo getTopologyInfoWithOpts(String id, GetInfoOptions options)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TopologyPageInfo getTopologyPageInfo(String id, String window, boolean is_include_sys)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SupervisorPageInfo getSupervisorPageInfo(String id, String host, boolean is_include_sys)
            throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ComponentPageInfo getComponentPageInfo(String topology_id, String component_id, String window,
            boolean is_include_sys) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTopologyConf(String id) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StormTopology getTopology(String id) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TopologyHistoryInfo getTopologyHistory(String user) throws AuthorizationException, TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClusterSummary getClusterInfo() throws AuthorizationException, TException {
        getClusterInfoCalls.mark();
        checkAuthorization(null, null, "getClusterInfo");
        try {
            return getClusterInfoImpl();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
