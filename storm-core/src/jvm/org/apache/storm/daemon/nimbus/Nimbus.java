/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.daemon.nimbus;

import static org.apache.storm.metric.StormMetricsRegistry.registerMeter;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.Config;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

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
    
    @SuppressWarnings("deprecation")
    public static TimeCacheMap<String, AutoCloseable> fileCacheMap(Map<String, Object> conf) {
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS)),
                (id, stream) -> {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
    
    public static IScheduler makeScheduler(Map<String, Object> conf, INimbus inimbus) {
        String schedClass = (String) conf.get(Config.STORM_SCHEDULER);
        IScheduler scheduler = inimbus.getForcedScheduler();
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
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_BLOBSTORE_EXPIRATION_SECS)),
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
        return new TimeCacheMap<>(Utils.getInt(conf.get(Config.NIMBUS_BLOBSTORE_EXPIRATION_SECS)));
    }

//    private final Map<String, Object> conf;
//    private final NimbusInfo nimbusHostPortInfo;
//    private final INimbus inimbus;
//    private final IAuthorizer authorizationHandler;
//    private final IAuthorizer ImpersonationAuthorizationHandler;
//    private final AtomicLong submittedCount;
//    private final IStormClusterState stormClusterState;
//    private final Object submitLock;
//    private final Object credUpdateLock;
//    private final Object logUpdateLock;
//    private final AtomicReference<Map<List<Integer>, Map<String, Object>>> heartbeatsCache;
//    private final TimeCacheMap<String, AutoCloseable> downloaders;
//    private final TimeCacheMap<String, AutoCloseable> uploaders;
//    private final BlobStore blobStore;
//    private final TimeCacheMap<String, OutputStream> blobDownloaders;
//    private final TimeCacheMap<String, OutputStream> blobUploaders;
//    
//    //TODO need to replace Exception with something better
//    public Nimbus(Map<String, Object> conf, INimbus inimbus) throws Exception {
//        this.conf = conf;
//        this.nimbusHostPortInfo = NimbusInfo.fromConf(conf);
//        this.inimbus = inimbus;
//        this.authorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(Config.NIMBUS_AUTHORIZER), conf);
//        this.ImpersonationAuthorizationHandler = StormCommon.mkAuthorizationHandler((String) conf.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER), conf);
//        this.submittedCount = new AtomicLong(0);
//        //TODO need to change CLusterUtils to have a Map option
//        List<ACL> acls = null;
//        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
//            acls = ZK_ACLS;
//        }
//        this.stormClusterState = ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.NIMBUS));
//        //TODO we need a better lock for this...
//        this.submitLock = new Object();
//        this.credUpdateLock = new Object();
//        this.logUpdateLock = new Object();
//        IScheduler forcedSchduler = inimbus.getForcedScheduler();
//        this.heartbeatsCache = new AtomicReference<>(new HashMap<>());
//        this.downloaders = fileCacheMap(conf);
//        this.uploaders = fileCacheMap(conf);
//        this.blobStore = Utils.getNimbusBlobStore(conf, nimbusHostPortInfo);
//        this.blobDownloaders = makeBlobCachMap(conf);
//        this.blobUploaders = makeBlobCachMap(conf);
//    }
//
//               :blob-listers (mk-bloblist-cache-map conf)
//               :uptime (Utils/makeUptimeComputer)
//               :validator (Utils/newInstance (conf NIMBUS-TOPOLOGY-VALIDATOR))
//               :timer (StormTimer. nil
//                        (reify Thread$UncaughtExceptionHandler
//                          (^void uncaughtException
//                            [this ^Thread t ^Throwable e]
//                            (log-error e "Error when processing event")
//                            (Utils/exitProcess 20 "Error when processing an event"))))
//
//               :scheduler (Nimbus/makeScheduler conf inimbus)
//               :leader-elector (Zookeeper/zkLeaderElector conf blob-store)
//               :id->sched-status (atom {})
//               :node-id->resources (atom {}) ;;resources of supervisors
//               :id->resources (atom {}) ;;resources of topologies
//               :id->worker-resources (atom {}) ; resources of workers per topology
//               :cred-renewers (AuthUtils/GetCredentialRenewers conf)
//               :topology-history-lock (Object.)
//               :topo-history-state (ConfigUtils/nimbusTopoHistoryState conf)
//               :nimbus-autocred-plugins (AuthUtils/getNimbusAutoCredPlugins conf)
//               :nimbus-topology-action-notifier (create-tology-action-notifier conf)
//               :cluster-consumer-executors (mk-cluster-metrics-consumer-executors conf)
//               }))
//    
}
