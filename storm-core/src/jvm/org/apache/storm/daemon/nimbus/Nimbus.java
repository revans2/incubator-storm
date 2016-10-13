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
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.generated.WorkerResources;
import org.apache.storm.metric.ClusterMetricsConsumerExecutor;
import org.apache.storm.nimbus.DefaultTopologyValidator;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.nimbus.ITopologyValidator;
import org.apache.storm.nimbus.NimbusInfo;
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
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.UptimeComputer;
import org.apache.storm.utils.VersionInfo;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
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
    
    public static final BinaryOperator<Map<String, WorkerResources>> MERGE_ID_TO_WORKER_RESOURCES = (orig, update) -> {
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

    private static <K, V> Map<K, V> merge(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> ... others) {
        Map<K, V> ret = new HashMap<>(first);
        for (Map<? extends K, ? extends V> other: others) {
            ret.putAll(other);
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
                nodePort.add(slot.getPort());
                
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
                nodePort.add(slot.getPort());
                
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
    
//    (defn convert-assignments-to-worker->resources [new-scheduler-assignments]
//            "convert {topology-id -> SchedulerAssignment} to
//                     {topology-id -> {[node port] [mem-on-heap mem-off-heap cpu]}}
//             Make sure this can deal with other non-RAS schedulers
//             later we may further support map-for-any-resources"
//            (map-val (fn [^SchedulerAssignment assignment]
//                       (->> assignment
//                            .getExecutorToSlot
//                            .values
//                            (#(into {} (for [^WorkerSlot slot %]
//                                        {[(.getNodeId slot) (.getPort slot)]
//                                         [(.getAllocatedMemOnHeap slot) (.getAllocatedMemOffHeap slot) (.getAllocatedCpu slot)]
//                                         })))))
//                     new-scheduler-assignments))
    
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
    private final AtomicReference<Map<String, WorkerResources>> idToWorkerResources;
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

    public AtomicReference<Map<String, WorkerResources>> getIdToWorkerResources() {
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

    //TODO replace this ASAP
    private static final clojure.lang.IFn FIXME_MK_ASSIGNMENTS = clojure.java.api.Clojure.var("org.apache.storm.daemon.nimbuslegacy", "mk-assignments-scratch");    
    
    void doRebalance(String topoId, StormBase stormBase) {
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
        FIXME_MK_ASSIGNMENTS.invoke(this, topoId);
    }
    
    private String toTopoId(String topoName) throws NotAliveException {
        String topoId = StormCommon.getStormId(getStormClusterState(), topoName);
        if (topoId == null) {
            throw new NotAliveException(topoName);
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
    public Map<String, Set<List<Integer>>> computeTopologyToExecutors(List<String> topoIds) throws KeyNotFoundException, AuthorizationException, InvalidTopologyException, IOException {
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
    public Map<String, SchedulerAssignment> computeTopologyToSchedulerAssignment(Map<String, Assignment> existingAssignments,
            Map<String, Set<List<Integer>>> topologyToAliveExecutors) {
        Map<String, SchedulerAssignment> ret = new HashMap<>();
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
}
