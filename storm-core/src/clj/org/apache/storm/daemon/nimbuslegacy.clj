;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.daemon.nimbuslegacy
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args]
           [org.apache.storm.stats StatsUtil]
           [org.apache.storm.metric StormMetricsRegistry])
  (:import [org.apache.storm.daemon.nimbus Nimbus TopologyResources TopologyStateTransition Nimbus$Dissoc TopologyActions])
  (:import [org.apache.storm.generated KeyNotFoundException TopologyStatus])
  (:import [org.apache.storm.blobstore LocalFsBlobStore])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [org.apache.commons.io FileUtils])
  (:import [javax.security.auth Subject])
  (:import [org.apache.storm.security.auth NimbusPrincipal])
  (:import [java.nio ByteBuffer]
           [java.util Collections List HashMap]
           [org.apache.storm.generated NimbusSummary])
  (:import [java.nio ByteBuffer]
           [java.util Collections List HashMap ArrayList Iterator Map])
  (:import [org.apache.storm.blobstore AtomicOutputStream BlobStoreAclHandler
            InputStreamWithMeta KeyFilter KeySequenceNumber BlobSynchronizer BlobStoreUtils])
  (:import [java.io File FileOutputStream FileInputStream])
  (:import [java.net InetAddress ServerSocket BindException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [org.apache.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils]
           [org.apache.storm.logging ThriftAccessLogger])
  (:import [org.apache.storm.scheduler DefaultScheduler])
  (:import [org.apache.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:import [org.apache.storm.nimbus NimbusInfo])
  (:import [org.apache.storm.scheduler.resource ResourceUtils])
  (:import [org.apache.storm.utils TimeCacheMap Time TimeCacheMap$ExpiredCallback Utils ConfigUtils TupleUtils ThriftTopologyUtils
            BufferFileInputStream BufferInputStream])
  (:import [org.apache.storm.generated NotAliveException AlreadyAliveException StormTopology ErrorInfo ClusterWorkerHeartbeat
            ExecutorInfo InvalidTopologyException Nimbus$Iface Nimbus$Processor SubmitOptions TopologyInitialStatus
            KillOptions RebalanceOptions ClusterSummary SupervisorSummary TopologySummary TopologyInfo TopologyHistoryInfo
            ExecutorSummary AuthorizationException GetInfoOptions NumErrorsChoice SettableBlobMeta ReadableBlobMeta
            BeginDownloadResult ListBlobsResult ComponentPageInfo TopologyPageInfo LogConfig LogLevel LogLevelAction
            ProfileRequest ProfileAction NodeInfo LSTopoHistory SupervisorPageInfo WorkerSummary WorkerResources ComponentType
            TopologyActionOptions])
  (:import [org.apache.storm.daemon Shutdownable StormCommon DaemonCommon])
  (:import [org.apache.storm.validation ConfigValidation])
  (:import [org.apache.storm.cluster ClusterStateContext DaemonType StormClusterStateImpl ClusterUtils])
  (:use [org.apache.storm util config log converter])
  (:require [org.apache.storm [converter :as converter]])
  (:require [org.apache.storm.ui.core :as ui])
  (:require [clojure.set :as set])
  (:import [org.apache.storm.daemon.common StormBase Assignment])
  (:import [org.apache.storm.zookeeper Zookeeper])
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm config])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [org.apache.storm.metric ClusterMetricsConsumerExecutor]
           [org.apache.storm.metric.api IClusterMetricsConsumer$ClusterInfo DataPoint IClusterMetricsConsumer$SupervisorInfo]
           [org.apache.storm Config])
  (:import [org.apache.storm.utils VersionInfo LocalState]
           [org.json.simple JSONValue])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:import [org.apache.storm StormTimer])
  (:gen-class
    :methods [^{:static true} [launch [org.apache.storm.scheduler.INimbus] void]]))

(defn validate-port-available[conf]
  (try
    (let [socket (ServerSocket. (conf NIMBUS-THRIFT-PORT))]
      (.close socket))
    (catch BindException e
      (log-error e (conf NIMBUS-THRIFT-PORT) " is not available. Check if another process is already listening on " (conf NIMBUS-THRIFT-PORT))
      (System/exit 0))))

(defn launch-server! [conf inimbus]
  (StormCommon/validateDistributedMode conf)
  (validate-port-available conf)
  (let [service-handler (Nimbus. conf inimbus)
        _ (.launchServer service-handler)
        server (ThriftServer. conf (Nimbus$Processor. service-handler)
                              ThriftConnectionType/NIMBUS)]
    (Utils/addShutdownHookWithForceKillIn1Sec (fn []
                                                  (.shutdown service-handler)
                                                  (.stop server)))
    (log-message "Starting nimbus server for storm version '"
                 Nimbus/STORM_VERSION
                 "'")
    (.serve server)
    service-handler))

(defn -launch [inimbus]
  (let [conf (merge
               (clojurify-structure (ConfigUtils/readStormConfig))
               (clojurify-structure (ConfigUtils/readYamlConfig "storm-cluster-auth.yaml" false)))]
  (launch-server! conf inimbus)))

(defn standalone-nimbus []
  (reify INimbus
    (prepare [this conf local-dir]
      )
    (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
      (->> supervisors
           (mapcat (fn [^SupervisorDetails s]
                     (for [p (.getMeta s)]
                       (WorkerSlot. (.getId s) p))))
           set ))
    (assignSlots [this topology slots]
      )
    (getForcedScheduler [this]
      nil )
    (getHostName [this supervisors node-id]
      (if-let [^SupervisorDetails supervisor (get supervisors node-id)]
        (.getHost supervisor)))
    ))

(defn -main []
  (Utils/setupDefaultUncaughtExceptionHandler)
  (-launch (standalone-nimbus)))
