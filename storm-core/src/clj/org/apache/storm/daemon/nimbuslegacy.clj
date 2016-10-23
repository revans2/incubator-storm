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

(defmulti setup-jar cluster-mode)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn get-key-seq-from-blob-store [blob-store]
  (let [key-iter (.listKeys blob-store)]
    (iterator-seq key-iter)))

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;;TODO inline this when it is translated
(defn get-launch-time-secs 
  [base storm-id]
  (if base (:launch-time-secs base)
    (throw
      (NotAliveException. (str storm-id)))))

;;TODO inline this when it's use is translated
(defn- between?
  "val >= lower and val <= upper"
  [val lower upper]
  (and (>= val lower)
    (<= val upper)))

(defn mk-reified-nimbus [nimbus conf blob-store]
  nimbus)

(defn mk-nimbus
  [conf inimbus blob-store leader-elector group-mapper cluster-state]
  (.prepare inimbus conf (ConfigUtils/masterInimbusDir conf))
  (Nimbus. conf inimbus cluster-state nil blob-store leader-elector group-mapper))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defserverfn service-handler [nimbus]
  (let [conf (.getConf nimbus)
        blob-store (.getBlobStore nimbus)]
    (log-message "Starting Nimbus with conf " conf)
    (.prepare ^org.apache.storm.nimbus.ITopologyValidator (.getValidator nimbus) conf)

    ;add to nimbuses
    (.addNimbusHost (.getStormClusterState nimbus) (.toHostPortString (.getNimbusHostPortInfo nimbus))
      (NimbusSummary.
        (.getHost (.getNimbusHostPortInfo nimbus))
        (.getPort (.getNimbusHostPortInfo nimbus))
        (Time/currentTimeSecs)
        false ;is-leader
        Nimbus/STORM_VERSION))

    (.addToLeaderLockQueue (.getLeaderElector nimbus))
    (when (instance? LocalFsBlobStore blob-store)
      ;register call back for blob-store
      (.blobstore (.getStormClusterState nimbus) (fn [] (.blobSync nimbus)))
      (.setupBlobstore nimbus))

    (doseq [consumer (.getClusterConsumerExecutors nimbus)]
      (.prepare consumer))

    (when (.isLeader nimbus)
      (doseq [storm-id (.activeStorms (.getStormClusterState nimbus))]
        (.transition nimbus storm-id TopologyActions/STARTUP nil)))

    (.scheduleRecurring (.getTimer nimbus)
      0
      (conf NIMBUS-MONITOR-FREQ-SECS)
      (fn []
        (when-not (conf ConfigUtils/NIMBUS_DO_NOT_REASSIGN)
          (locking (.getSubmitLock nimbus)
            (.mkAssignments nimbus)))
        (.doCleanup nimbus)))
    ;; Schedule Nimbus inbox cleaner
    (.scheduleRecurring (.getTimer nimbus)
      0
      (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
      (fn [] (Nimbus/cleanInbox (.getInbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))))
    ;; Schedule nimbus code sync thread to sync code from other nimbuses.
    (if (instance? LocalFsBlobStore blob-store)
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf NIMBUS-CODE-SYNC-FREQ-SECS)
        (fn [] (.blobSync nimbus))))
    ;; Schedule topology history cleaner
    (when-let [interval (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)
        (fn [] (.cleanTopologyHistory nimbus (conf LOGVIEWER-CLEANUP-AGE-MINS)))))
    (.scheduleRecurring (.getTimer nimbus)
      0
      (conf NIMBUS-CREDENTIAL-RENEW-FREQ-SECS)
      (fn []
        (.renewCredentials nimbus)))

    (def nimbus:num-supervisors (StormMetricsRegistry/registerGauge "nimbus:num-supervisors"
      (fn [] (.size (.supervisors (.getStormClusterState nimbus) nil)))))

    (StormMetricsRegistry/startMetricsReporters conf)

    (if (.getClusterConsumerExecutors nimbus)
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf STORM-CLUSTER-METRICS-CONSUMER-PUBLISH-INTERVAL-SECS)
        (fn []
          (when (.isLeader nimbus)
            (.sendClusterMetricsToExecutors nimbus)))))

    (mk-reified-nimbus nimbus conf blob-store)))

(defn validate-port-available[conf]
  (try
    (let [socket (ServerSocket. (conf NIMBUS-THRIFT-PORT))]
      (.close socket))
    (catch BindException e
      (log-error e (conf NIMBUS-THRIFT-PORT) " is not available. Check if another process is already listening on " (conf NIMBUS-THRIFT-PORT))
      (System/exit 0))))

(defn launch-server! [conf nimbus]
  (StormCommon/validateDistributedMode conf)
  (validate-port-available conf)
  (let [service-handler (service-handler (mk-nimbus conf nimbus nil nil nil nil))
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

;; distributed implementation

(defmethod setup-jar :distributed [conf tmp-jar-location stormroot]
           (let [src-file (File. tmp-jar-location)]
             (if-not (.exists src-file)
               (throw
                (IllegalArgumentException.
                 (str tmp-jar-location " to copy to " stormroot " does not exist!"))))
             (FileUtils/copyFile src-file (File. (ConfigUtils/masterStormJarPath stormroot)))
             ))

;; local implementation

(defmethod setup-jar :local [conf & args]
  nil
  )

(defn -launch [nimbus]
  (let [conf (merge
               (clojurify-structure (ConfigUtils/readStormConfig))
               (clojurify-structure (ConfigUtils/readYamlConfig "storm-cluster-auth.yaml" false)))]
  (launch-server! conf nimbus)))

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
