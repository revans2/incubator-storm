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
  (let [principal-to-local (AuthUtils/GetPrincipalToLocalPlugin conf)
        admin-users (or (.get conf NIMBUS-ADMINS) [])
        get-common-topo-info
          (fn [^String storm-id operation]
            (let [storm-cluster-state (.getStormClusterState nimbus)
                  topology-conf (clojurify-structure (Nimbus/tryReadTopoConf storm-id blob-store))
                  storm-name (topology-conf TOPOLOGY-NAME)
                  _ (.checkAuthorization nimbus
                                          storm-name
                                          topology-conf
                                          operation)
                  topology (Nimbus/tryReadTopology storm-id blob-store)
                  task->component (clojurify-structure (StormCommon/stormTaskInfo topology topology-conf))
                  base (clojurify-storm-base (.stormBase storm-cluster-state storm-id nil))
                  launch-time-secs (get-launch-time-secs base storm-id)
                  assignment (clojurify-assignment (.assignmentInfo storm-cluster-state storm-id nil))
                  beats (get (.get (.getHeartbeatsCache nimbus)) storm-id)
                  all-components (set (vals task->component))]
              {:storm-name storm-name
               :storm-cluster-state storm-cluster-state
               :all-components all-components
               :launch-time-secs launch-time-secs
               :assignment assignment
               :beats (or beats {})
               :topology topology
               :topology-conf topology-conf
               :task->component task->component
               :base base}))
        set-resources-default-if-not-set
          (fn [^HashMap component-resources-map component-id topology-conf]
              (let [resource-map (or (.get component-resources-map component-id) (HashMap.))]
                (ResourceUtils/checkIntialization resource-map component-id topology-conf)
                resource-map))
        get-last-error (fn [storm-cluster-state storm-id component-id]
                         (if-let [e (clojurify-error  (.lastError storm-cluster-state
                                                 storm-id
                                                 component-id))]
                           (doto (ErrorInfo. (:error e) (:time-secs e))
                             (.set_host (:host e))
                             (.set_port (:port e)))))]
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (.submitTopologyWithOpts nimbus storm-name uploadedJarLocation serializedConf topology submitOptions))

      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (.submitTopology nimbus storm-name uploadedJarLocation serializedConf topology))

      (^void killTopology [this ^String name]
        (.killTopology nimbus name))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (.killTopologyWithOpts nimbus storm-name options))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (.rebalance nimbus storm-name options))

      (activate [this storm-name]
         (.activate nimbus storm-name))

      (deactivate [this storm-name]
         (.deactivate nimbus storm-name))

      (debug [this storm-name component-id enable? samplingPct]
         (.debug nimbus storm-name component-id enable? samplingPct))

      (^void setWorkerProfiler
        [this ^String id ^ProfileRequest profileRequest]
         (.setWorkerProfiler nimbus id profileRequest))

      (^List getComponentPendingProfileActions
        [this ^String id ^String component_id ^ProfileAction action]
        (.getComponentPendingProfileActions nimbus id component_id action))

      (^void setLogConfig [this ^String id ^LogConfig log-config-msg]
        (.setLogConfig nimbus id log-config-msg))

      (uploadNewCredentials [this storm-name credentials]
        (.uploadNewCredentials nimbus storm-name credentials))

      (beginFileUpload [this]
        (.beginFileUpload nimbus))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (.uploadChunk nimbus location chunk))

      (^void finishFileUpload [this ^String location]
        (.finishFileUpload nimbus location))

      (^String beginFileDownload
        [this ^String file]
        (.beginFileDownload nimbus file))

      (^ByteBuffer downloadChunk [this ^String id]
        (.downloadChunk nimbus id))

      (^String getNimbusConf [this]
        (.getNimbusConf nimbus))

      (^LogConfig getLogConfig [this ^String id]
        (.getLogConfig nimbus id))

      (^String getTopologyConf [this ^String id]
        (.getTopologyConf nimbus id))

      (^StormTopology getTopology [this ^String id]
        (.getTopology nimbus id))

      (^StormTopology getUserTopology [this ^String id]
        (.getUserTopology nimbus id))

      (^ClusterSummary getClusterInfo [this]
        (.getClusterInfo nimbus))

      (^TopologyInfo getTopologyInfoWithOpts [this ^String storm-id ^GetInfoOptions options]
        (.getTopologyInfoWithOpts nimbus storm-id options))

      (^TopologyInfo getTopologyInfo [this ^String topology-id]
        (.getTopologyInfo nimbus topology-id))

      (^String beginCreateBlob [this
                                ^String blob-key
                                ^SettableBlobMeta blob-meta]
        (.beginCreateBlob nimbus, blob-key, blob-meta))

      (^String beginUpdateBlob [this ^String blob-key]
        (let [^AtomicOutputStream os (.updateBlob (.getBlobStore nimbus)
                                       blob-key (Nimbus/getSubject))]
          (let [session-id (Utils/uuid)]
            (.put (.getBlobUploaders nimbus) session-id os)
            (log-message "Created upload session for " blob-key
              " with id " session-id)
            (str session-id))))

      (^void createStateInZookeeper [this ^String blob-key]
        (let [storm-cluster-state (.getStormClusterState nimbus)
              blob-store (.getBlobStore nimbus)
              nimbus-host-port-info (.getNimbusHostPortInfo nimbus)
              conf (.getConf nimbus)]
          (if (instance? LocalFsBlobStore blob-store)
              (.setupBlobstore storm-cluster-state blob-key nimbus-host-port-info (Nimbus/getVerionForKey blob-key nimbus-host-port-info conf)))
          (log-debug "Created state in zookeeper" storm-cluster-state blob-store nimbus-host-port-info)))

      (^void uploadBlobChunk [this ^String session ^ByteBuffer blob-chunk]
        (let [uploaders (.getBlobUploaders nimbus)]
          (if-let [^AtomicOutputStream os (.get uploaders session)]
            (let [chunk-array (.array blob-chunk)
                  remaining (.remaining blob-chunk)
                  array-offset (.arrayOffset blob-chunk)
                  position (.position blob-chunk)]
              (.write os chunk-array (+ array-offset position) remaining)
              (.put uploaders session os))
            (throw (RuntimeException. (str "Blob for session " session
                                           " does not exist (or timed out)"))))))

      (^void finishBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (.getBlobUploaders nimbus) session)]
          (do
            (.close os)
            (log-message "Finished uploading blob for session "
              session
              ". Closing session.")
            (.remove (.getBlobUploaders nimbus) session))
          (throw (RuntimeException. (str "Blob for session " session
                                         " does not exist (or timed out)")))))

      (^void cancelBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (.getBlobUploaders nimbus) session)]
          (do
            (.cancel os)
            (log-message "Canceled uploading blob for session "
              session
              ". Closing session.")
            (.remove (.getBlobUploaders nimbus) session))
          (throw (RuntimeException. (str "Blob for session " session
                                         " does not exist (or timed out)")))))

      (^ReadableBlobMeta getBlobMeta [this ^String blob-key]
        (let [^ReadableBlobMeta ret (.getBlobMeta (.getBlobStore nimbus)
                                      blob-key (Nimbus/getSubject))]
          ret))

      (^void setBlobMeta [this ^String blob-key ^SettableBlobMeta blob-meta]
        (->> (ReqContext/context)
          (.subject)
          (.setBlobMeta (.getBlobStore nimbus) blob-key blob-meta)))

      (^BeginDownloadResult beginBlobDownload [this ^String blob-key]
        (let [^InputStreamWithMeta is (.getBlob (.getBlobStore nimbus)
                                        blob-key (Nimbus/getSubject))]
          (let [session-id (Utils/uuid)
                ret (BeginDownloadResult. (.getVersion is) (str session-id))]
            (.set_data_size ret (.getFileLength is))
            (.put (.getBlobDownloaders nimbus) session-id (BufferInputStream. is (Utils/getInt (conf STORM-BLOBSTORE-INPUTSTREAM-BUFFER-SIZE-BYTES) (int 65536))))
            (log-message "Created download session for " blob-key
              " with id " session-id)
            ret)))

      (^ByteBuffer downloadBlobChunk [this ^String session]
        (let [downloaders (.getBlobDownloaders nimbus)
              ^BufferInputStream is (.get downloaders session)]
          (when-not is
            (throw (RuntimeException.
                     "Could not find input stream for session " session)))
          (let [ret (.read is)]
            (.put downloaders session is)
            (when (empty? ret)
              (.close is)
              (.remove downloaders session))
            (log-debug "Sending " (alength ret) " bytes")
            (ByteBuffer/wrap ret))))

      (^void deleteBlob [this ^String blob-key]
        (let [subject (->> (ReqContext/context)
                           (.subject))]
          (.deleteBlob (.getBlobStore nimbus) blob-key subject)
          (when (instance? LocalFsBlobStore blob-store)
            (.removeBlobstoreKey (.getStormClusterState nimbus) blob-key)
            (.removeKeyVersion (.getStormClusterState nimbus) blob-key))
          (log-message "Deleted blob for key " blob-key)))

      (^ListBlobsResult listBlobs [this ^String session]
        (let [listers (.getBlobListers nimbus)
              ^Iterator keys-it (or
                                 (if (clojure.string/blank? session)
                                   (.listKeys (.getBlobStore nimbus))
                                   (.get listers session))
                                 (throw (RuntimeException. (str "Blob list for session "
                                                                session
                                                                " does not exist (or timed out)"))))
              ;; Create a new session id if the user gave an empty session string.
              ;; This is the use case when the user wishes to list blobs
              ;; starting from the beginning.
              session (if (clojure.string/blank? session)
                        (let [new-session (Utils/uuid)]
                          (log-message "Creating new session for downloading list " new-session)
                          new-session)
                        session)]
          (if-not (.hasNext keys-it)
            (do
              (.remove listers session)
              (log-message "No more blobs to list for session " session)
              ;; A blank result communicates that there are no more blobs.
              (ListBlobsResult. (ArrayList. 0) (str session)))
            (let [^List list-chunk (->> keys-it
                                     (iterator-seq)
                                     (take 100) ;; Limit to next 100 keys
                                     (ArrayList.))]
              (log-message session " downloading " (.size list-chunk) " entries")
              (.put listers session keys-it)
              (ListBlobsResult. list-chunk (str session))))))

      (^int getBlobReplication [this ^String blob-key]
        (->> (ReqContext/context)
          (.subject)
          (.getBlobReplication (.getBlobStore nimbus) blob-key)))

      (^int updateBlobReplication [this ^String blob-key ^int replication]
        (->> (ReqContext/context)
          (.subject)
          (.updateBlobReplication (.getBlobStore nimbus) blob-key replication)))

      (^TopologyPageInfo getTopologyPageInfo
        [this ^String topo-id ^String window ^boolean include-sys?]
        (.mark Nimbus/getTopologyPageInfoCalls)
        (let [topo-info (get-common-topo-info topo-id "getTopologyPageInfo")
              {:keys [storm-name
                      storm-cluster-state
                      launch-time-secs
                      assignment
                      beats
                      task->component
                      topology
                      topology-conf
                      base]} topo-info
              j-base (thriftify-storm-base base)
              exec->node+port (:executor->node+port assignment)
              node->host (:node->host assignment)
              worker->resources (.getWorkerResourcesForTopology nimbus topo-id)
              worker-summaries (StatsUtil/aggWorkerStats topo-id 
                                                         storm-name
                                                         task->component
                                                         beats
                                                         exec->node+port
                                                         node->host
                                                         worker->resources
                                                         include-sys?
                                                         true)  ;; this is the topology page, so we know the user is authorized 
              topo-page-info (StatsUtil/aggTopoExecsStats topo-id
                                                          exec->node+port
                                                          task->component
                                                          beats
                                                          topology
                                                          window
                                                          include-sys?
                                                          storm-cluster-state)]

          (doseq [[spout-id component-aggregate-stats] (.get_id_to_spout_agg_stats topo-page-info)]
            (let [common-stats (.get_common_stats component-aggregate-stats)
                  resources (ResourceUtils/getSpoutsResources topology topology-conf)]
              (.set_resources_map common-stats (set-resources-default-if-not-set resources spout-id topology-conf))))

          (doseq [[bolt-id component-aggregate-stats] (.get_id_to_bolt_agg_stats topo-page-info)]
            (let [common-stats (.get_common_stats component-aggregate-stats)
                  resources (ResourceUtils/getBoltsResources topology topology-conf)]
              (.set_resources_map common-stats (set-resources-default-if-not-set resources bolt-id topology-conf))))

          (.set_workers topo-page-info worker-summaries)
          (when-let [owner (:owner base)]
            (.set_owner topo-page-info owner))
          (when-let [sched-status (.get (.get (.getIdToSchedStatus nimbus)) topo-id)]
            (.set_sched_status topo-page-info sched-status))
          (when-let [resources (.getResourcesForTopology nimbus topo-id)]
            (.set_requested_memonheap topo-page-info (.getRequestedMemOnHeap resources))
            (.set_requested_memoffheap topo-page-info (.getRequestedMemOffHeap resources))
            (.set_requested_cpu topo-page-info (.getRequestedCpu resources))
            (.set_assigned_memonheap topo-page-info (.getAssignedMemOnHeap resources))
            (.set_assigned_memoffheap topo-page-info (.getAssignedMemOffHeap resources))
            (.set_assigned_cpu topo-page-info (.getAssignedCpu resources)))
          (doto topo-page-info
            (.set_name storm-name)
            (.set_status (Nimbus/extractStatusStr j-base))
            (.set_uptime_secs (Time/deltaSecs launch-time-secs))
            (.set_topology_conf (JSONValue/toJSONString
                                  (clojurify-structure (Nimbus/tryReadTopoConf
                                                       topo-id
                                                       (.getBlobStore nimbus)))))
            (.set_replication_count (.getBlobReplicationCount nimbus (ConfigUtils/masterStormCodeKey topo-id))))
          (when-let [debug-options
                     (get-in topo-info [:base :component->debug topo-id])]
            (.set_debug_options
              topo-page-info
              (converter/thriftify-debugoptions debug-options)))
          topo-page-info))

      (^SupervisorPageInfo getSupervisorPageInfo
        [this
         ^String supervisor-id
         ^String host 
         ^boolean include-sys?]
        (.mark Nimbus/getSupervisorPageInfoCalls)
        (let [storm-cluster-state (.getStormClusterState nimbus)
              supervisor-infos (.allSupervisorInfo storm-cluster-state)
              host->supervisor-id (Utils/reverseMap (map-val (fn [info] (.get_hostname info)) supervisor-infos))
              supervisor-ids (if (nil? supervisor-id)
                                (get host->supervisor-id host)
                                  [supervisor-id])
              page-info (SupervisorPageInfo.)]
              (doseq [sid supervisor-ids]
                (let [supervisor-info (get supervisor-infos sid)
                      _ (log-message "SID: " sid " SI: " supervisor-info " ALL: " supervisor-infos)
                      sup-sum (.makeSupervisorSummary nimbus sid supervisor-info)
                      _ (.add_to_supervisor_summaries page-info sup-sum)
                      topo-id->assignments (clojurify-structure (.topologyAssignments storm-cluster-state))
                      supervisor-topologies (clojurify-structure (Nimbus/topologiesOnSupervisor topo-id->assignments sid))
                      user-topologies (clojurify-structure (.filterAuthorized nimbus "getTopology" supervisor-topologies))]
                  (doseq [storm-id supervisor-topologies]
                      (let [topo-info (get-common-topo-info storm-id "getSupervisorPageInfo")
                            {:keys [storm-name
                                    assignment
                                    beats
                                    task->component]} topo-info
                            exec->node+port (:executor->node+port assignment)
                            node->host (:node->host assignment)
                            worker->resources (.getWorkerResourcesForTopology nimbus storm-id)]
                        (doseq [worker-summary (StatsUtil/aggWorkerStats storm-id 
                                                                         storm-name
                                                                         task->component
                                                                         beats
                                                                         exec->node+port
                                                                         node->host
                                                                         worker->resources
                                                                         include-sys?
                                                                         (boolean (get user-topologies storm-id))
                                                                         sid)]
                          (.add_to_worker_summaries page-info worker-summary)))))) 
              page-info))

      (^ComponentPageInfo getComponentPageInfo
        [this
         ^String topo-id
         ^String component-id
         ^String window
         ^boolean include-sys?]
        (.mark Nimbus/getComponentPageInfoCalls)
        (let [info (get-common-topo-info topo-id "getComponentPageInfo")
              {:keys [topology topology-conf]} info
              {:keys [executor->node+port node->host]} (:assignment info)
              ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                           executor->node+port)
              comp-page-info (StatsUtil/aggCompExecsStats executor->host+port
                                                         (:task->component info)
                                                         (:beats info)
                                                         window
                                                         include-sys?
                                                         topo-id
                                                         (:topology info)
                                                         component-id)]
          (if (.equals (.get_component_type comp-page-info) ComponentType/SPOUT)
            (.set_resources_map comp-page-info 
              (set-resources-default-if-not-set (ResourceUtils/getSpoutsResources topology topology-conf) component-id topology-conf))
            (.set_resources_map comp-page-info
              (set-resources-default-if-not-set (ResourceUtils/getBoltsResources topology topology-conf) component-id topology-conf)))

          (doto comp-page-info
            (.set_topology_name (:storm-name info))
            (.set_errors (.errors (:storm-cluster-state info)
                                     topo-id
                                     component-id))
            (.set_topology_status (Nimbus/extractStatusStr (thriftify-storm-base (:base info)))))
          (when-let [debug-options
                     (get-in info [:base :component->debug component-id])]
            (.set_debug_options
              comp-page-info
              (converter/thriftify-debugoptions debug-options)))
          ;; Add the event logger details.
          (let [component->tasks (clojurify-structure (Utils/reverseMap (:task->component info)))]
            (if (contains? component->tasks StormCommon/EVENTLOGGER_COMPONENT_ID)
              (let [eventlogger-tasks (sort (get component->tasks
                                                 StormCommon/EVENTLOGGER_COMPONENT_ID))
                    ;; Find the task the events from this component route to.
                    task-index (mod (TupleUtils/listHashCode [component-id])
                                    (count eventlogger-tasks))
                    task-id (nth eventlogger-tasks task-index)
                    eventlogger-exec (first (filter (fn [[start stop]]
                                                      (between? task-id start stop))
                                                    (keys executor->host+port)))
                    [host port] (get executor->host+port eventlogger-exec)]
                (if (and host port)
                  (doto comp-page-info
                    (.set_eventlog_host host)
                    (.set_eventlog_port port))))))
          comp-page-info))

      (^TopologyHistoryInfo getTopologyHistory [this ^String user]
        (let [storm-cluster-state (.getStormClusterState nimbus)
              assigned-topology-ids (.assignments storm-cluster-state nil)
              user-group-match-fn (fn [topo-id user conf]
                                    (let [topology-conf (clojurify-structure (Nimbus/tryReadTopoConf topo-id (.getBlobStore nimbus)))
                                          groups (ConfigUtils/getTopoLogsGroups topology-conf)]
                                      (or (nil? user)
                                          (some #(= % user) admin-users)
                                          (.isUserPartOf nimbus user groups)
                                          (some #(= % user) (ConfigUtils/getTopoLogsUsers topology-conf)))))
              active-ids-for-user (filter #(user-group-match-fn % user (.getConf nimbus)) assigned-topology-ids)
              topo-history-list (.readTopologyHistory nimbus user admin-users)]
          (TopologyHistoryInfo. (distinct (concat active-ids-for-user topo-history-list)))))

      Shutdownable
      (shutdown [this]
        (.mark Nimbus/shutdownCalls)
        (log-message "Shutting down master")
        (.close (.getTimer nimbus))
        (.disconnect (.getStormClusterState nimbus))
        (.cleanup (.getDownloaders nimbus))
        (.cleanup (.getUploaders nimbus))
        (.shutdown (.getBlobStore nimbus))
        (.close (.getLeaderElector nimbus))
        (when (.getNimbusTopologyActionNotifier nimbus) (.cleanup (.getNimbusTopologyActionNotifier nimbus)))
        (log-message "Shut down master"))
      DaemonCommon
      (isWaiting [this]
        (.isTimerWaiting (.getTimer nimbus))))))

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
