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
(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args]
           [backtype.storm.security.auth SingleUserPrincipal NimbusPrincipal]
           [backtype.storm.generated BlobReplication])
  (:import [javax.security.auth Subject])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [org.apache.commons.io FileUtils])
  (:import [java.nio ByteBuffer]
           [java.util Collections List HashMap])
  (:import [java.io FileNotFoundException File FileOutputStream FileInputStream])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [java.util Iterator])
  (:import [backtype.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils])
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster TopologyResources Topologies SchedulerAssignment SchedulerAssignmentImpl
            SupervisorResources DefaultScheduler ExecutorDetails])
  (:import [backtype.storm.scheduler.resource ResourceUtils])
  (:import [backtype.storm.utils TimeCacheMap TimeCacheMap$ExpiredCallback Utils ThriftTopologyUtils BufferInputStream])
  (:import [backtype.storm.generated AuthorizationException AlreadyAliveException BeginDownloadResult
                                     ClusterSummary ComponentPageInfo SupervisorPageInfo ErrorInfo ExecutorInfo ExecutorSummary GetInfoOptions
                                     InvalidTopologyException KeyNotFoundException KillOptions
                                     ListBlobsResult LogConfig LogLevel LogLevelAction
                                     Nimbus$Iface Nimbus$Processor NumErrorsChoice NotAliveException
                                     ReadableBlobMeta RebalanceOptions SubmitOptions SupervisorSummary StormTopology
                                     WorkerSummary SettableBlobMeta TopologyHistoryInfo TopologyInfo TopologyInitialStatus
                                     TopologyPageInfo TopologyStats TopologySummary
                                     ProfileRequest ProfileAction NodeInfo ComponentType OwnerResourceSummary])
  (:import [backtype.storm.blobstore AtomicOutputStream
                                     BlobStore
                                     BlobStoreAclHandler
                                     ClientBlobStore
                                     InputStreamWithMeta
                                     KeyFilter
                                     KeyNotFoundMessageException])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.cluster ClusterStateContext DaemonType])
  (:use [backtype.storm util config log timer local-state])
  (:require [backtype.storm [cluster :as cluster] [stats :as stats]])
  (:require [clojure.set :as set])
  (:require [clojure.core.reducers :as reducers])
  (:import [backtype.storm.daemon.common Assignment StormBase])
  (:use [backtype.storm.daemon common])
  (:use [clojure.string :only [blank?]])
  (:use [clojure.set :only [intersection]])
  (:use [org.apache.storm.pacemaker pacemaker-state-factory])
  (:use [backtype.storm.converter])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [backtype.storm.utils VersionInfo])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:require [metrics.gauges :refer [defgauge]])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

(defmeter nimbus:num-submitTopologyWithOpts-calls)
(defmeter nimbus:num-submitTopology-calls)
(defmeter nimbus:num-killTopologyWithOpts-calls)
(defmeter nimbus:num-killTopology-calls)
(defmeter nimbus:num-rebalance-calls)
(defmeter nimbus:num-activate-calls)
(defmeter nimbus:num-deactivate-calls)
(defmeter nimbus:num-setLogConfig-calls)
(defmeter nimbus:num-uploadNewCredentials-calls)
(defmeter nimbus:num-beginFileUpload-calls)
(defmeter nimbus:num-uploadChunk-calls)
(defmeter nimbus:num-finishFileUpload-calls)
(defmeter nimbus:num-beginFileDownload-calls)
(defmeter nimbus:num-downloadChunk-calls)
(defmeter nimbus:num-getNimbusConf-calls)
(defmeter nimbus:num-getLogConfig-calls)
(defmeter nimbus:num-getTopologyConf-calls)
(defmeter nimbus:num-getTopology-calls)
(defmeter nimbus:num-getUserTopology-calls)
(defmeter nimbus:num-getClusterInfo-calls)
(defmeter nimbus:num-getTopologyInfoWithOpts-calls)
(defmeter nimbus:num-getTopologyInfo-calls)
(defmeter nimbus:num-getTopologyPageInfo-calls)
(defmeter nimbus:num-getComponentPageInfo-calls)
(defmeter nimbus:num-shutdown-calls)
(defmeter nimbus:num-getComponentPendingProfileActions-calls)
(defmeter nimbus:num-beginCreateBlob-calls)
(defmeter nimbus:num-beginUpdateBlob-calls)
(defmeter nimbus:num-deleteBlob-calls)
(defmeter nimbus:num-listBlobs-calls)
(defmeter nimbus:num-getTopologyHistory-calls)
(defmeter nimbus:num-setWorkerProfiler-calls)
(defmeter nimbus:num-getSupervisorPageInfo-calls)
(defmeter nimbus:num-getOwnerResourceSummaries-calls)

(def STORM-VERSION (VersionInfo/getVersion))

(declare delay-event)
(declare transition!)
(declare update-blob-store)
(declare mk-assignments)
(declare read-storm-topology-as-subject)
(declare read-storm-conf-as-subject)
(declare compute-executor->component)

(defn mk-file-cache-map
  "Constructs a TimeCacheMap instance with a nimbus file timeout whose expiration
  callback invokes close on the value held by an expired entry."
  [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)))))

(defn mk-blob-cache-map
  "Constructs a TimeCacheMap instance with a blob store timeout whose
  expiration callback invokes cancel on the value held by an expired entry when
  that value is an AtomicOutputStream and calls close otherwise."
  [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-BLOBSTORE-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (if (instance? AtomicOutputStream stream)
                    (.cancel stream)
                    (.close stream))))))

(defn mk-bloblist-cache-map
  "Constructs a TimeCacheMap instance with a blobstore timeout and no callback
  function."
  [conf]
  (TimeCacheMap. (int (conf NIMBUS-BLOBSTORE-EXPIRATION-SECS))))

(defn mk-scheduler [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)
        scheduler (cond
                    forced-scheduler
                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
                        forced-scheduler)
    
                    (conf STORM-SCHEDULER)
                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
                        (-> (conf STORM-SCHEDULER) new-instance))
    
                    :else
                    (do (log-message "Using default scheduler")
                        (DefaultScheduler.)))]
    (.prepare scheduler conf)
    scheduler
    ))

(def NIMBUS-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL) 
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (conf NIMBUS-AUTHORIZER) conf)
     :impersonation-authorization-handler (mk-authorization-handler (conf NIMBUS-IMPERSONATION-AUTHORIZER) conf)
     :submitted-count (atom 0)
     :storm-cluster-state (cluster/mk-storm-cluster-state conf
                                                          :acls (when
                                                                    (Utils/isZkAuthenticationConfiguredStormServer
                                                                     conf)
                                                                  NIMBUS-ZK-ACLS)
                                                          :context (ClusterStateContext. DaemonType/NIMBUS))
     :submit-lock (Object.)
     :sched-lock (Object.)
     :cred-update-lock (Object.)
     :log-update-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders (mk-file-cache-map conf)
     :uploaders (mk-file-cache-map conf)
     :blob-store (Utils/getNimbusBlobStore conf)
     :blob-downloaders (mk-blob-cache-map conf)
     :blob-uploaders (mk-blob-cache-map conf)
     :blob-listers (mk-bloblist-cache-map conf)
     :uptime (uptime-computer)
     :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))
     :timer (mk-timer :kill-fn (fn [t]
                                 (log-error t "Error when processing event")
                                 (exit-process! 20 "Error when processing an event")
                                 ))
     :scheduler (mk-scheduler conf inimbus)
     :id->sched-status (atom {})
     :id->topology-conf (atom {}) ; cache of topology conf
     :id->system-topology (atom {}) ; cache of system topology
     :node-id->resources (atom {}) ; resources of supervisors
     :id->resources (atom {}) ; resources of topologies
     :id->worker-resources (atom {}) ; resources of workers per topology
     :cred-renewers (AuthUtils/GetCredentialRenewers conf)
     :topology-history-lock (Object.)
     :topo-history-state (nimbus-topo-history-state conf)
     :nimbus-autocred-plugins (AuthUtils/getNimbusAutoCredPlugins conf)
     }))

(defn inbox [nimbus]
  (master-inbox (:conf nimbus)))

(defn- get-subject
  []
  (let [req (ReqContext/context)]
    (.subject req)))

(defn get-nimbus-subject
  []
  (let [subject (Subject.)
        principal (NimbusPrincipal.)
        principals (.getPrincipals subject)]
    (.add principals principal)
    subject))

(def nimbus-subject
  (get-nimbus-subject))

(defn get-system-topology [storm-conf ^StormTopology topology storm-id nimbus]
  (when (not (get @(:id->system-topology nimbus) storm-id))
            (swap! (:id->system-topology nimbus) assoc storm-id (system-topology! storm-conf topology)))
    (get @(:id->system-topology nimbus) storm-id))

(defn get-key-seq-from-blob-store [blob-store]
  (let [key-iter (.listKeys blob-store nimbus-subject)]
    (iterator-seq key-iter)))

(defn- get-nimbus-subject []
  (let [nimbus-subject (Subject.)
        nimbus-principal (NimbusPrincipal.)
        principals (.getPrincipals nimbus-subject)]
    (.add principals nimbus-principal)
    nimbus-subject))

(defn- read-storm-topology-as-subject [storm-id blob-store subject]
  (Utils/deserialize
    (.readBlob blob-store (master-stormcode-key storm-id) subject) StormTopology))

(defn read-storm-conf-as-subject [conf storm-id blob-store subject]
  (clojurify-structure
    (Utils/fromCompressedJsonConf
      (.readBlob blob-store (master-stormconf-key storm-id) subject))))

(defn- read-storm-topology-as-nimbus [storm-id nimbus]
  (read-storm-topology-as-subject storm-id (:blob-store nimbus) (get-nimbus-subject)))

(defn read-storm-conf-as-nimbus [conf storm-id nimbus]
  (when (not (get (:id->topology-conf nimbus) storm-id))
    (swap! (:id->topology-conf nimbus) assoc storm-id
      (read-storm-conf-as-subject conf storm-id (:blob-store nimbus) (get-nimbus-subject))))
  (get @(:id->topology-conf nimbus) storm-id))

(defn try-read-storm-conf [conf storm-id nimbus]
  (try-cause
    (read-storm-conf-as-nimbus conf storm-id nimbus)
    (catch KeyNotFoundException e
      (throw (NotAliveException. (str storm-id))))))

;; used when we want to ignore the exception
(defn try-read-storm-conf-or-nil [conf storm-id nimbus]
  (try-cause
    (read-storm-conf-as-nimbus conf storm-id nimbus)
    (catch KeyNotFoundException e
      nil)))

(defn try-read-storm-conf-from-name [conf storm-name nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        id (get-storm-id storm-cluster-state storm-name)]
    (try-read-storm-conf conf id nimbus)))

(defn try-read-storm-topology [storm-id nimbus]
  (try-cause
    (read-storm-topology-as-nimbus storm-id nimbus)
    (catch KeyNotFoundException e
      (throw (NotAliveException. (str storm-id))))))

(defn set-topology-status! [nimbus storm-id status]
  (let [storm-cluster-state (:storm-cluster-state nimbus)]
   (.update-storm! storm-cluster-state
                   storm-id
                   {:status status})
   (log-message "Updated " storm-id " with status " status)
   ))

(defn kill-transition [nimbus storm-id]
  (fn [kill-time]
    (let [delay (if kill-time
                  kill-time
                  (get (try-read-storm-conf (:conf nimbus) storm-id nimbus)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :remove)
      {
        :status {:type :killed}
        :topology-action-options {:delay-secs delay :action :kill}})
    ))

(defn rebalance-transition [nimbus storm-id status]
  (fn [time num-workers executor-overrides resource-overrides topology-config-overrides subject]
    (let [delay (if time
                  time
                  (get (try-read-storm-conf (:conf nimbus) storm-id nimbus)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :do-rebalance)
      {:status {:type :rebalancing}
       :prev-status status
       :topology-action-options (-> {:delay-secs delay :action :rebalance}
                                    (assoc-non-nil :num-workers num-workers)
                                    (assoc-non-nil :component->executors executor-overrides)
                                    (assoc-non-nil :component->resources resource-overrides)
                                    (assoc-non-nil :topology->config topology-config-overrides)
                                    (assoc-non-nil :principal (if subject
                                                                (-> subject
                                                                (.getPrincipals)
                                                                (.iterator)
                                                                (.next)
                                                                (.getName)))))
       })))

(defn do-rebalance [nimbus storm-id status storm-base]
  (let [rebalance-options (:topology-action-options storm-base)]
    (.update-storm! (:storm-cluster-state nimbus)
      storm-id
        (-> {}
          (assoc-non-nil :component->executors (:component->executors rebalance-options))
          (assoc-non-nil :num-workers (:num-workers rebalance-options))))
    ;; update blob store
    (update-blob-store nimbus storm-id rebalance-options (Utils/principalNameToSubject (:principal rebalance-options))))
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defn state-transitions [nimbus storm-id status storm-base]
  {:active {:inactivate :inactive
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:startup (fn [] (delay-event nimbus
                                         storm-id
                                         (-> storm-base
                                             :topology-action-options
                                             :delay-secs)
                                         :remove)
                             nil)
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                                      storm-id)
                      nil)
            }
   :rebalancing {:startup (fn [] (delay-event nimbus
                                              storm-id
                                              (-> storm-base
                                                  :topology-action-options
                                                  :delay-secs)
                                              :do-rebalance)
                                 nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status storm-base)
                                 {:status {:type (:type (:prev-status storm-base))}
                                  :prev-status :rebalancing
                                  :topology-action-options nil})
                 }})

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
     (locking (:submit-lock nimbus)
       (let [system-events #{:startup}
             [event & event-args] (if (keyword? event) [event] event)
             storm-base (-> nimbus :storm-cluster-state  (.storm-base storm-id nil))
             status (:status storm-base)]
         ;; handles the case where event was scheduled but topology has been removed
         (if-not status
           (log-message "Cannot apply event " event " to " storm-id " because topology no longer exists")
           (let [get-event (fn [m e]
                             (if (contains? m e)
                               (m e)
                               (let [msg (str "No transition for event: " event
                                              ", status: " status,
                                              " storm-id: " storm-id)]
                                 (if error-on-no-transition?
                                   (throw-runtime msg)
                                   (do (when-not (contains? system-events event)
                                         (log-message msg))
                                       nil))
                                 )))
                 transition (-> (state-transitions nimbus storm-id status storm-base)
                                (get (:type status))
                                (get-event event))
                 transition (if (or (nil? transition)
                                    (keyword? transition))
                              (fn [] transition)
                              transition)
                 storm-base-updates (apply transition event-args)
                 storm-base-updates (if (keyword? storm-base-updates) ;if it's just a symbol, that just indicates new status.
                                      {:status {:type storm-base-updates}}
                                      storm-base-updates)]

             (when storm-base-updates
               (.update-storm! (:storm-cluster-state nimbus) storm-id storm-base-updates)))))
       )))

(defn transition-name! [nimbus storm-name event & args]
  (let [storm-id (get-storm-id (:storm-cluster-state nimbus) storm-name)]
    (when-not storm-id
      (throw (NotAliveException. storm-name)))
    (apply transition! nimbus storm-id event args)))

(defn delay-event [nimbus storm-id delay-secs event]
  (log-message "Delaying event " event " for " delay-secs " secs for " storm-id)
  (schedule (:timer nimbus)
            (or delay-secs 0)
            #(transition! nimbus storm-id event false)
            ))

;; active -> reassign in X secs

;; killed -> wait kill time then shutdown
;; active -> reassign in X secs
;; inactive -> nothing
;; rebalance -> wait X seconds then rebalance
;; swap... (need to handle kill during swap, etc.)
;; event transitions are delayed by timer... anything else that comes through (e.g. a kill) override the transition? or just disable other transitions during the transition?


(defmulti clean-inbox cluster-mode)

;; swapping design
;; -- need 2 ports per worker (swap port and regular port)
;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;; -- how to define worker resources? port range + number of workers?


;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn- assigned-slots
  "Returns a map from node-id to a set of ports"
  [storm-cluster-state]
  (let [assignments (.assignments storm-cluster-state nil)
        ]
    (defaulted
      (apply merge-with set/union
             (for [a assignments
                   [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :executor->node+port)]
               {node #{port}}
               ))
      {})
    ))

(defn- supervisor-info
  [storm-cluster-state id include-non-readable? min-age]
  (try
    (let [ret (.supervisor-info storm-cluster-state id)]
      (if (<= min-age (or (:uptime-secs ret) 0))
         ret
         (log-debug "DISALLOWING SUPERVISOR (too young): " (pr-str ret) " min-age " min-age)))
    (catch RuntimeException
      rte
      (log-message (str "The supervisor [" id "] is sending older heartbeat version"))
      (log-debug-error  rte (str "The supervisor [" id "] is sending older heartbeat version"))
      (if include-non-readable?
        (backtype.storm.daemon.common.SupervisorInfo. 0 "unknown" id nil nil nil 0 (.getMessage rte) nil)))))

;; public for testing
(defn all-supervisor-info
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil false))
  ([storm-cluster-state callback]  (all-supervisor-info storm-cluster-state callback false))
  ([storm-cluster-state callback include-non-readable?] (all-supervisor-info storm-cluster-state callback include-non-readable? 0))
  ([storm-cluster-state callback include-non-readable? min-age]
     (let [supervisor-ids (.supervisors storm-cluster-state callback)]
       (into {}
             (mapcat
              (fn [id]
                (if-let [info (supervisor-info storm-cluster-state id include-non-readable? min-age)]
                  [[id info]]
                  ))
              supervisor-ids)))))

(defn- setup-storm-code [conf storm-id tmp-jar-location storm-conf topology blob-store]
  (let [subject (get-subject)]
    (if tmp-jar-location ;;in local mode there is no jar
      (.createBlob blob-store (master-stormjar-key storm-id) (FileInputStream. tmp-jar-location) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject))
    (.createBlob blob-store (master-stormcode-key storm-id) (Utils/serialize topology) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject)
    (.createBlob blob-store (master-stormconf-key storm-id) (Utils/toCompressedJsonConf storm-conf) (SettableBlobMeta. BlobStoreAclHandler/DEFAULT) subject)))

(defnk update-storm-code [storm-id blob-store :topology-conf nil :topology nil :subject (get-subject)]
  (if topology-conf
    (let [blob-stream (.updateBlob blob-store (master-stormconf-key storm-id) subject)]
      (log-debug "updating topology-conf stored at key: " (master-stormconf-key storm-id) " conf: " topology-conf " for subject: " subject)
      (try
        (.write blob-stream (Utils/toCompressedJsonConf topology-conf))
        (.close blob-stream)
        (catch Exception e
          (.cancel blob-stream)
          (throw (RuntimeException. e))))))
  (if topology
    (let [blob-stream (.updateBlob blob-store (master-stormcode-key storm-id) subject)]
      (log-debug "updating storm topology stored at key: " (master-stormcode-key storm-id) " topology: " topology "for subject: " subject)
      (try
        (.write blob-stream (Utils/serialize topology))
        (.close blob-stream)
        (catch Exception e
          (.cancel blob-stream)
          (throw (RuntimeException. e)))))))

(defn update-topology-resources [nimbus storm-id resource-overrides subject]
  (let [blob-store (:blob-store nimbus)
        ^StormTopology topology (read-storm-topology-as-subject storm-id blob-store subject)]
    (ResourceUtils/updateStormTopologyResources topology resource-overrides)
    (update-storm-code storm-id blob-store :topology topology :subject subject)
    (swap! (:id->system-topology nimbus) dissoc storm-id)
    nil))

(defn update-topology-config [nimbus storm-id topology-config-override subject]
  (let [blob-store (:blob-store nimbus)
        conf (:conf nimbus)
        current-config (read-storm-conf-as-subject conf storm-id blob-store subject)
        merged-config (merge current-config topology-config-override)]
    (update-storm-code storm-id blob-store :topology-conf merged-config :subject subject)
    (swap! (:id->topology-conf nimbus) assoc storm-id merged-config)
    nil))

(defn update-blob-store [nimbus storm-id rebalance-options subject]
  (if (not (empty? (:component->resources rebalance-options)))
    (update-topology-resources nimbus storm-id (:component->resources rebalance-options) subject))
  (if (not (empty? (:topology->config rebalance-options)))
    (update-topology-config nimbus storm-id (:topology->config rebalance-options) subject)))

(defn fixup-storm-base
  [storm-base topo-conf]
  (assoc storm-base
         :owner (.get topo-conf TOPOLOGY-SUBMITTER-USER)
         :principal (.get topo-conf TOPOLOGY-SUBMITTER-PRINCIPAL)))

(defn read-topology-details 
  ([nimbus storm-id]
    (read-topology-details nimbus storm-id (.storm-base (:storm-cluster-state nimbus) storm-id nil)))
  ([nimbus storm-id storm-base]
    (when (nil? storm-base) (throw (KeyNotFoundException. (str "Topology " storm-id " does not appear to be running any more"))))
    (let [conf (:conf nimbus)
          topology-conf (read-storm-conf-as-nimbus conf storm-id nimbus)
          storm-base (if (nil? (:principal storm-base))
                       (let [new-sb (fixup-storm-base storm-base topology-conf)]
                           (.update-storm! (:storm-cluster-state nimbus) storm-id new-sb)
                           new-sb)
                       storm-base)
          topology (read-storm-topology-as-nimbus storm-id nimbus)
          executor->component (->> (compute-executor->component nimbus storm-id)
                                   (map-key (fn [[start-task end-task]]
                                              (ExecutorDetails. (int start-task) (int end-task)))))]
      (TopologyDetails. storm-id
                        topology-conf
                        topology
                        (or (:num-workers storm-base) 0)
                        executor->component
                        (or (:launch-time-secs storm-base) 0) (:owner storm-base)))))

;; Does not assume that clocks are synchronized. Executor heartbeat is only used so that
;; nimbus knows when it's received a new heartbeat. All timing is done by nimbus and
;; tracked through heartbeat-cache
(defn- update-executor-cache [curr hb timeout]
  (let [reported-time (:time-secs hb)
        {last-nimbus-time :nimbus-time
         last-reported-time :executor-reported-time} curr
        reported-time (cond reported-time reported-time
                            last-reported-time last-reported-time
                            :else 0)
        nimbus-time (if (or (not last-nimbus-time)
                        (not= last-reported-time reported-time))
                      (current-time-secs)
                      last-nimbus-time
                      )]
      {:is-timed-out (and
                       nimbus-time
                       (>= (time-delta nimbus-time) timeout))
       :nimbus-time nimbus-time
       :executor-reported-time reported-time
       :heartbeat hb}))

(defn update-heartbeat-cache [cache executor-beats all-executors timeout]
  (let [cache (select-keys cache all-executors)]
    (into {}
      (for [executor all-executors :let [curr (cache executor)]]
        [executor
         (update-executor-cache curr (get executor-beats executor) timeout)]
         ))))

(defn update-heartbeats! [nimbus storm-id all-executors existing-assignment]
  (log-debug "Updating heartbeats for " storm-id " " (pr-str all-executors))
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        executor-beats (.executor-beats storm-cluster-state storm-id (:executor->node+port existing-assignment))
        cache (update-heartbeat-cache (@(:heartbeats-cache nimbus) storm-id)
                                      executor-beats
                                      all-executors
                                      ((:conf nimbus) NIMBUS-TASK-TIMEOUT-SECS))]
      (swap! (:heartbeats-cache nimbus) assoc storm-id cache)))

(defn- update-all-heartbeats! [nimbus existing-assignments topology->executors]
  "update all the heartbeats for all the topologies's executors"
  (doseq [[tid assignment] existing-assignments
          :let [all-executors (topology->executors tid)]]
    (update-heartbeats! nimbus tid all-executors assignment)))

(defn- alive-executors
  [nimbus ^TopologyDetails topology-details all-executors existing-assignment]
  (log-debug "Computing alive executors for " (.getId topology-details) "\n"
             "Executors: " (pr-str all-executors) "\n"
             "Assignment: " (pr-str existing-assignment) "\n"
             "Heartbeat cache: " (pr-str (@(:heartbeats-cache nimbus) (.getId topology-details)))
             )
  ;; TODO: need to consider all executors associated with a dead executor (in same slot) dead as well,
  ;; don't just rely on heartbeat being the same
  (let [conf (:conf nimbus)
        storm-id (.getId topology-details)
        executor-start-times (:executor->start-time-secs existing-assignment)
        heartbeats-cache (@(:heartbeats-cache nimbus) storm-id)]
    (->> all-executors
        (filter (fn [executor]
          (let [start-time (get executor-start-times executor)
                is-timed-out (-> heartbeats-cache (get executor) :is-timed-out)]
            (if (and start-time
                   (or
                    (< (time-delta start-time)
                       (conf NIMBUS-TASK-LAUNCH-SECS))
                    (not is-timed-out)
                    ))
              true
              (do
                (log-message "Executor " storm-id ":" executor " not alive")
                false))
            )))
        doall)))


(defn- to-executor-id [task-ids]
  [(first task-ids) (last task-ids)])

(defn- compute-executors [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        component->executors (:component->executors storm-base)
        storm-conf (read-storm-conf-as-nimbus conf storm-id nimbus)
        topology (read-storm-topology-as-nimbus storm-id nimbus)
        task->component (storm-task-info (get-system-topology storm-conf topology storm-id nimbus) storm-conf)]
    (if (nil? component->executors)
      []
      (->> task->component
           reverse-map
           (map-val sort)
           (join-maps component->executors)
           (map-val (partial apply partition-fixed))
           (mapcat second)
           (map to-executor-id)))))

(defn- compute-executor->component [nimbus storm-id]
  (let [conf (:conf nimbus)
        blob-store (:blob-store nimbus)
        executors (compute-executors nimbus storm-id)
        storm-conf (read-storm-conf-as-nimbus conf storm-id nimbus)
        task->component (storm-task-info
                          (get-system-topology storm-conf
                                               (read-storm-topology-as-nimbus storm-id nimbus)
                                               storm-id nimbus)
                          storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
        executor->component))

(defn- compute-topology->executors [nimbus storm-ids]
  "compute a topology-id -> executors map"
  (into {} (reducers/map (fn [tid]
             {tid (set (compute-executors nimbus tid))}) storm-ids)))

(defn- compute-topology->alive-executors [nimbus existing-assignments topologies topology->executors scratch-topology-id]
  "compute a topology-id -> alive executors map"
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors nimbus topology-details all-executors assignment)))]]
             {tid alive-executors})))

(defn- compute-supervisor->dead-ports [nimbus existing-assignments topology->executors topology->alive-executors]
  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
                                  :let [all-executors (topology->executors tid)
                                        alive-executors (topology->alive-executors tid)
                                        dead-executors (set/difference all-executors alive-executors)
                                        dead-slots (->> (:executor->node+port assignment)
                                                        (filter #(contains? dead-executors (first %)))
                                                        vals)]]
                              dead-slots))
        supervisor->dead-ports (->> dead-slots
                                    (apply concat)
                                    (map (fn [[sid port]] {sid #{port}}))
                                    (apply (partial merge-with set/union)))]
    (or supervisor->dead-ports {})))

(defn- compute-topology->scheduler-assignment [nimbus existing-assignments topology->alive-executors]
  "convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  (into {} (for [[tid assignment] existing-assignments
                 :let [alive-executors (topology->alive-executors tid)
                       executor->node+port (:executor->node+port assignment)
                       worker->resources (:worker->resources assignment)
                       ;; making a map from node+port to WorkerSlot with allocated resources
                       node+port->slot (into {} (for [[_ [node port]] executor->node+port]
                                                  {[node port]
                                                   (WorkerSlot. node port)}))
                       slot->resources (into {} (for [[[node port] [mem-on-heap mem-off-heap cpu]] worker->resources]
                                                  {(WorkerSlot. node port)
                                                   (doto (backtype.storm.generated.WorkerResources.)
                                                         (.set_mem_off_heap mem-off-heap)
                                                         (.set_cpu cpu)
                                                         (.set_mem_on_heap mem-on-heap))}))
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (get node+port->slot [node port])}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot slot->resources nil)})))

(defn- read-all-supervisor-details
  [nimbus supervisor->dead-ports topologies missing-assignment-topologies]
  "return a map: {supervisor-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        supervisor-infos (all-supervisor-info storm-cluster-state nil false ((:conf nimbus) NIMBUS-SUPERVISOR-MIN-AGE-SECS))
        supervisor-details (for [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info) (:resources-map info)))
        all-scheduling-slots (->> (.allSlotsAvailableForScheduling
                                    (:inimbus nimbus)
                                    supervisor-details
                                    topologies
                                    (set missing-assignment-topologies))
                                  (map (fn [s] {(.getNodeId s) #{(.getPort s)}}))
                                  (apply merge-with set/union))]
    (into {} (for [[sid supervisor-info] supervisor-infos
                   :let [hostname (:hostname supervisor-info)
                         scheduler-meta (:scheduler-meta supervisor-info)
                         dead-ports (supervisor->dead-ports sid)
                         ;; hide the dead-ports from the all-ports
                         ;; these dead-ports can be reused in next round of assignments
                         all-ports (-> (get all-scheduling-slots sid)
                                       (set/difference dead-ports)
                                       (as-> ports (map int ports)))
                         supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports (:resources-map supervisor-info))]]
               {sid supervisor-details}))))

(defn- compute-topology->executor->node+port [scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {executor [node port]}}"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                              {[(.getStartTask executor) (.getEndTask executor)]
                               [(.getNodeId slot) (.getPort slot)]})))))
           scheduler-assignments))

;; NEW NOTES
;; only assign to supervisors who are there and haven't timed out
;; need to reassign workers with executors that have timed out (will this make it brittle?)
;; need to read in the topology and storm-conf from disk
;; if no slots available and no slots used by this storm, just skip and do nothing
;; otherwise, package rest of executors into available slots (up to how much it needs)

;; in the future could allocate executors intelligently (so that "close" tasks reside on same machine)

;; TODO: slots that have dead executor should be reused as long as supervisor is active


;; (defn- assigned-slots-from-scheduler-assignments [topology->assignment]
;;   (->> topology->assignment
;;        vals
;;        (map (fn [^SchedulerAssignment a] (.getExecutorToSlot a)))
;;        (mapcat vals)
;;        (map (fn [^WorkerSlot s] {(.getNodeId s) #{(.getPort s)}}))
;;        (apply merge-with set/union)
;;        ))

(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
  (if scheduler-assignment
    (count (.getSlots scheduler-assignment))
    0 ))

(defn convert-assignments-to-worker->resources [new-scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {[node port] [mem-on-heap mem-off-heap cpu]}}
   Make sure this can deal with other non-RAS schedulers
   later we may further support map-for-any-resources"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getScheduledResources
                  (#(into {} (for [[^WorkerSlot slot resources]  %]
                              {[(.getNodeId slot) (.getPort slot)]
                               [(.get_mem_on_heap resources) (.get_mem_off_heap resources) (.get_cpu resources)]})))))
           new-scheduler-assignments))

(defn compute-new-topology->executor->node+port [new-scheduler-assignments existing-assignments]
  (let [new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                              existing-assignments
                                              :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                              (not (= node+port (old-executor->node+port executor)))))
                                       executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (log-message "Reassigning " topology-id " to " new-slots-cnt " slots")
          (log-message "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))

(defn is-fragmented? [conf supervisor-resource]
  (let [min-memory (+ (Utils/getDouble (conf TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB) (double 256))
                      (Utils/getDouble (conf TOPOLOGY-ACKER-RESOURCES-ONHEAP-MEMORY-MB) (double 128)))
        min-cpu (+ (Utils/getDouble (conf TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT) (double 50))
                   (Utils/getDouble (conf TOPOLOGY-ACKER-CPU-PCORE-PERCENT) (double 50)))]
    (or (> min-cpu (.getAvailableCpu ^SupervisorResources supervisor-resource))
        (> min-memory (.getAvailableMem ^SupervisorResources supervisor-resource)))))

(defn fragmented-memory [supervisor-infos conf]
  (reduce (fn [acc x] (+ acc (if (is-fragmented? conf x) (max (.getAvailableMem x) 0) 0))) 0 (vals supervisor-infos)))

(defn fragmented-cpu [supervisor-infos conf]
  (reduce (fn [acc x] (+ acc (if (is-fragmented? conf x) (max (.getAvailableCpu x) 0) 0))) 0 (vals supervisor-infos)))

;; public so it can be mocked out
(defn compute-new-scheduler-assignments [nimbus existing-assignments topologies scratch-topology-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        topology->executors (compute-topology->executors nimbus (keys existing-assignments))
        ;; update the executors heartbeats first.
        _ (update-all-heartbeats! nimbus existing-assignments topology->executors)
        topology->alive-executors (compute-topology->alive-executors nimbus
                                                                     existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id)
        supervisor->dead-ports (compute-supervisor->dead-ports nimbus
                                                               existing-assignments
                                                               topology->executors
                                                               topology->alive-executors)
        topology->scheduler-assignment (compute-topology->scheduler-assignment nimbus
                                                                               existing-assignments
                                                                               topology->alive-executors)

        missing-assignment-topologies (->> topologies
                                           .getTopologies
                                           (map (memfn getId))
                                           (filter (fn [t]
                                                     (let [alle (get topology->executors t)
                                                           alivee (get topology->alive-executors t)]
                                                       (or (empty? alle)
                                                           (not= alle alivee)
                                                           (< (-> topology->scheduler-assignment
                                                                  (get t)
                                                                  num-used-workers )
                                                              (-> topologies (.getById t) .getNumWorkers)))))))
        supervisors (read-all-supervisor-details nimbus
                                                 supervisor->dead-ports
                                                 topologies
                                                 missing-assignment-topologies)
        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment topologies conf)]

    ;; set the status map with existing topology statuses
    (.setStatusMap cluster (deref (:id->sched-status nimbus)))
    ;; call scheduler.schedule to schedule all the topologies
    ;; the new assignments for all the topologies are in the cluster object.
    (let [before-schedule (current-time-millis)]
      (.schedule (:scheduler nimbus) topologies cluster)
      (log-message "Scheduling took " (- (current-time-millis) before-schedule)
                   " ms for " (.. topologies getTopologies size)))

    ;;merge with existing statuses
    (reset! (:id->sched-status nimbus) (merge (deref (:id->sched-status nimbus)) (.getStatusMap cluster)))
    (reset! (:node-id->resources nimbus) (.getSupervisorsResourcesMap cluster))
    ; Remove both of swaps below at first opportunity. This is a hack for multitenant swags with RAS Bridge Scheduler.
    (swap! (:id->resources nimbus) merge (.getTopologyResourcesMap cluster))
    ; Remove this also at first chance
    (swap! (:id->worker-resources nimbus) merge 
           (into {} (map (fn [[k v]] [k (map-val #(->WorkerResources (.get_mem_on_heap %) (.get_mem_off_heap %) (.get_cpu %)) v)])
                         (.getWorkerResourcesMap cluster))))

    (.getAssignments cluster)))

(defn- get-resources-for-topology [nimbus topo-id storm-base]
  (or (get @(:id->resources nimbus) topo-id)
      (try
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              topology-details (read-topology-details nimbus topo-id storm-base)
              assignmentInfo (thriftify-assignment (.assignment-info storm-cluster-state topo-id nil))
              topo-resources (TopologyResources. topology-details assignmentInfo)]
          (swap! (:id->resources nimbus) assoc topo-id topo-resources)
          topo-resources)
        (catch KeyNotFoundException e
          ; This can happen when a topology is first coming up.
          ; It's thrown by the blobstore code.
          (log-error e "Failed to get topology details")
          (TopologyResources. )))))

(defn- get-worker-resources-for-topology [nimbus topo-id]
  (or (get @(:id->worker-resources nimbus) topo-id)
      (try
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              assigned-resources (->> (.assignment-info storm-cluster-state topo-id nil)
                                      :worker->resources)
              worker-resources (into {} (map #(identity {(WorkerSlot. (first (key %)) (second (key %)))
                                                         (->WorkerResources (nth (val %) 0)
                                                                            (nth (val %) 1)
                                                                            (nth (val %) 2))}) assigned-resources))]
          (swap! (:id->worker-resources nimbus) assoc topo-id worker-resources)
          worker-resources))))

(defn changed-executors [executor->node+port new-executor->node+port]
  (let [executor->node+port (if executor->node+port (sort executor->node+port) nil)
        new-executor->node+port (if new-executor->node+port (sort new-executor->node+port) nil)
        slot-assigned (reverse-map executor->node+port)
        new-slot-assigned (reverse-map new-executor->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

(defn newly-added-slots [existing-assignment new-assignment]
  (let [old-slots (-> (:executor->node+port existing-assignment)
                      vals
                      set)
        new-slots (-> (:executor->node+port new-assignment)
                      vals
                      set)]
    (set/difference new-slots old-slots)))

(defn basic-supervisor-details-map [storm-cluster-state min-age]
  (let [infos (all-supervisor-info storm-cluster-state nil true min-age)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil (:resources-map info))]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

(defn- fixup-assignment
  [assignment td]
  (assoc assignment
         :owner (.getTopologySubmitter td)))

;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)
        ;; read all the topologies
        tds (locking (:submit-lock nimbus)
                     (into {} (reducers/map
                        (fn [tid] {tid (read-topology-details nimbus tid)})
                        (.active-storms storm-cluster-state))))
        topologies (Topologies. tds)
        ;; read all the assignments
        assigned-topology-ids (.assignments storm-cluster-state nil)
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          (let [assignment (.assignment-info storm-cluster-state tid nil)
                                                td (.get tds tid)
                                                assignment (if (and (not (:owner assignment)) (not (nil? td)))
                                                             (let [new-assignment (fixup-assignment assignment td)]
                                                               (.set-assignment! storm-cluster-state tid new-assignment)
                                                               new-assignment)
                                                             assignment)]
                                            {tid assignment}))))
        ;; make the new assignments for topologies
        new-scheduler-assignments (locking (:sched-lock nimbus) (compute-new-scheduler-assignments
                                       nimbus
                                       existing-assignments
                                       topologies
                                       scratch-topology-id))

        topology->executor->node+port (compute-new-topology->executor->node+port new-scheduler-assignments existing-assignments)

        topology->executor->node+port (merge (into {} (for [id assigned-topology-ids] {id nil})) topology->executor->node+port)
        new-assigned-worker->resources (convert-assignments-to-worker->resources new-scheduler-assignments)
        now-secs (current-time-secs)

        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state (conf NIMBUS-SUPERVISOR-MIN-AGE-SECS))

        ;; construct the final Assignments by adding start-times etc into it
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                        :let [existing-assignment (get existing-assignments topology-id)
                                              all-nodes (->> executor->node+port vals (map first) set)
                                              node->host (->> all-nodes
                                                              (mapcat (fn [node]
                                                                        (if-let [host (.getHostName inimbus basic-supervisor-details-map node)]
                                                                          [[node host]]
                                                                          )))
                                                              (into {}))
                                              all-node->host (merge (:node->host existing-assignment) node->host)
                                              reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                              start-times (merge (:executor->start-time-secs existing-assignment)
                                                                (into {}
                                                                      (for [id reassign-executors]
                                                                        [id now-secs]
                                                                        )))
                                              worker->resources (get new-assigned-worker->resources topology-id)
                                              assignment-impl (.get new-scheduler-assignments topology-id)
                                              shared-off-heap (if assignment-impl
                                                                (.getNodeIdToTotalSharedOffHeapMemory assignment-impl)
                                                                {})]]
                                   {topology-id (Assignment. ;; construct the record common/Assignment, not the java class
                                                 (conf STORM-LOCAL-DIR)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times
                                                 worker->resources
                                                 shared-off-heap
                                                 (.getTopologySubmitter (.get tds topology-id)))}))]

    (when (not= new-assignments existing-assignments)
      (log-debug "RESETTING id->resources and id->worker-resources cache!")
      (log-message "Fragmentation after scheduling is: "
                   (fragmented-memory @(:node-id->resources nimbus) conf) " MB "
                   (fragmented-cpu @(:node-id->resources nimbus) conf) " CPU %")
      (doseq [[sup-id item] @(:node-id->resources nimbus)]
        (log-message "Node Id: " sup-id
                     " Total Mem: " (.getTotalMem ^SupervisorResources item)
                     " Used Mem: " (.getUsedMem ^SupervisorResources item)
                     " Avialble Mem: " (.getAvailableMem ^SupervisorResources item)
                     " Total CPU: " (.getTotalCpu ^SupervisorResources item)
                     " Used CPU: " (.getUsedCpu ^SupervisorResources item)
                     " Available CPU: " (.getAvailableCpu ^SupervisorResources item)
                     " fragmented: " (is-fragmented? conf item)))
      (reset! (:id->resources nimbus) {})
      (reset! (:id->worker-resources nimbus) {}))
    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (locking (:sched-lock nimbus)
      (doseq [[topology-id assignment] new-assignments
              :let [existing-assignment (get existing-assignments topology-id)
                    topology-details (.getById topologies topology-id)]]
        (if (= existing-assignment assignment)
          (log-debug "Assignment for " topology-id " hasn't changed")
          (do
            (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
            (.set-assignment! storm-cluster-state topology-id assignment)
            )))
      (->> new-assignments
            (map (fn [[topology-id assignment]]
              (let [existing-assignment (get existing-assignments topology-id)]
                [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))]
                )))
            (into {})
            (.assignSlots inimbus topologies)))))

(defn- start-storm [nimbus storm-name storm-id topology-initial-status owner principal]
  {:pre [(#{:active :inactive} topology-initial-status)]}
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        blob-store (:blob-store nimbus)
        storm-conf (try-read-storm-conf conf storm-id nimbus)
        topology (get-system-topology storm-conf (try-read-storm-topology storm-id nimbus) storm-id nimbus)
        num-executors (->> (all-components topology) (map-val num-start-executors))]
    (log-message "Activating " storm-name ": " storm-id)
    (.activate-storm! storm-cluster-state
                      storm-id
                      (StormBase. storm-name
                                  (current-time-secs)
                                  {:type topology-initial-status}
                                  (storm-conf TOPOLOGY-WORKERS)
                                  num-executors
                                  owner
                                  nil
                                  nil
                                  principal
                                  (storm-conf TOPOLOGY-VERSION)))))

(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (get-storm-id storm-cluster-state storm-name)))

(defn check-storm-active! [nimbus storm-name active?]
  (if (= (not active?)
         (storm-active? (:storm-cluster-state nimbus)
                        storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))

(defn check-authorization! 
  ([nimbus storm-name storm-conf operation context]
     (let [aclHandler (:authorization-handler nimbus)
           impersonation-authorizer (:impersonation-authorization-handler nimbus)
           ctx (or context (ReqContext/context))
           check-conf (if storm-conf storm-conf (if storm-name {TOPOLOGY-NAME storm-name}))]
       (if (.isImpersonating ctx)
         (do
          (log-warn "principal: " (.realPrincipal ctx) " is trying to impersonate principal: " (.principal ctx))
          (if impersonation-authorizer
           (when-not (.permit impersonation-authorizer ctx operation check-conf)
             (log-thrift-access (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-denied")
             (throw (AuthorizationException. (str "principal " (.realPrincipal ctx) " is not authorized to impersonate
                        principal " (.principal ctx) " from host " (.remoteAddress ctx) " Please see SECURITY.MD to learn
                        how to configure impersonation acls."))))
           (log-warn "impersonation attempt but " NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. potential
                      security risk, please see SECURITY.MD to learn how to configure impersonation authorizer."))))

       (if aclHandler
         (if-not (.permit aclHandler ctx operation check-conf)
           (do
             (log-thrift-access (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-denied")
             (throw (AuthorizationException. (str operation (if storm-name (str " on topology " storm-name)) " is not authorized"))))
           (log-thrift-access (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-granted")))))
  ([nimbus storm-name storm-conf operation]
     (check-authorization! nimbus storm-name storm-conf operation (ReqContext/context))))

;; no-throw version of check-authorization!
(defn is-authorized?
  [nimbus conf blob-store operation topology-id]
  (let [topology-conf (try-read-storm-conf conf topology-id nimbus)
        storm-name (topology-conf TOPOLOGY-NAME)]
    (try (check-authorization! nimbus storm-name topology-conf operation)
         true
      (catch AuthorizationException e false))))

(defn code-ids [blob-store]
  (let [to-id (reify KeyFilter
          (filter [this key] (get-id-from-blob-key key)))]
   (set (.filterAndListKeys blob-store to-id nil))))

(defn cleanup-storm-ids [storm-cluster-state blob-store]
  (let [heartbeat-ids (set (.heartbeat-storms storm-cluster-state))
        error-ids (set (.error-topologies storm-cluster-state))
        code-ids (code-ids blob-store)
        backpressure-ids (set (.backpressure-topologies storm-cluster-state))
        assigned-ids (set (.active-storms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids backpressure-ids code-ids) assigned-ids)
    ))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (name t))
    ))

(defn mapify-serializations [sers]
  (->> sers
       (map (fn [e] (if (map? e) e {e nil})))
       (apply merge)
       ))

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (merge storm-conf (component-conf component))
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

(defn normalize-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)]
    (doseq [[_ component] (all-components ret)]
      (.set_json_conf
        (.get_common component)
        (->> {TOPOLOGY-TASKS (component-parallelism storm-conf component)}
             (merge (component-conf component))
             to-json )))
    ret ))

(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                         #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                              .get_json_conf
                              from-json)
                         (ThriftTopologyUtils/getComponentIds topology))
        total-conf (merge conf storm-conf)

        get-merged-conf-val (fn [k merge-fn]
                              (merge-fn
                               (concat
                                (mapcat #(get % k) component-confs)
                                (or (get storm-conf k)
                                    (get conf k)))))]
    ;; topology level serialization registrations take priority
    ;; that way, if there's a conflict, a user can force which serialization to use
    ;; append component conf to storm-conf
    (merge storm-conf
           {TOPOLOGY-KRYO-DECORATORS (get-merged-conf-val TOPOLOGY-KRYO-DECORATORS distinct)
            TOPOLOGY-KRYO-REGISTER (get-merged-conf-val TOPOLOGY-KRYO-REGISTER mapify-serializations)
            TOPOLOGY-ACKER-EXECUTORS (total-conf TOPOLOGY-ACKER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

(defn blob-rm [blob-store key]
  (try
    (.deleteBlob blob-store key (get-nimbus-subject))
  (catch Exception e)))

(defn rm-from-blob-store [id blob-store]
  (blob-rm blob-store (master-stormjar-key id))
  (blob-rm blob-store (master-stormconf-key id))
  (blob-rm blob-store (master-stormcode-key id)))

(defn do-cleanup [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        blob-store (:blob-store nimbus)
        conf (:conf nimbus)
        submit-lock (:submit-lock nimbus)]
    (let [to-cleanup-ids (locking submit-lock
                           (cleanup-storm-ids storm-cluster-state blob-store))]
      (when-not (empty? to-cleanup-ids)
        (doseq [id to-cleanup-ids]
          (log-message "Cleaning up " id)
          (.teardown-heartbeats! storm-cluster-state id)
          (.teardown-topology-errors! storm-cluster-state id)
          (.teardown-topology-log-config! storm-cluster-state id)
          (.teardown-topology-profiler-requests storm-cluster-state id)
          (.remove-backpressure! storm-cluster-state id)
          (rm-from-blob-store id blob-store)
          (swap! (:id->topology-conf nimbus) dissoc id)
          (swap! (:id->system-topology nimbus) dissoc id)
          (swap! (:heartbeats-cache nimbus) dissoc id))))))

(defn- file-older-than? [now seconds file]
  (<= (+ (.lastModified file) (to-millis seconds)) (to-millis now)))

(defn clean-inbox [dir-location seconds]
  "Deletes jar files in dir older than seconds."
  (let [now (current-time-secs)
        pred #(and (.isFile %) (file-older-than? now seconds %))
        files (filter pred (file-seq (File. dir-location)))]
    (doseq [f files]
      (if (.delete f)
        (log-message "Cleaning inbox ... deleted: " (.getName f))
        ;; This should never happen
        (log-error "Cleaning inbox ... error deleting: " (.getName f))
        ))))

(defn clean-topology-history
  "Deletes topologies from history older than minutes."
  [mins nimbus]
  (locking (:topology-history-lock nimbus)
    (let [cutoff-age (- (current-time-secs) (* mins 60))
          topo-history-state (:topo-history-state nimbus)
          curr-history (vec (ls-topo-hist topo-history-state))
          new-history (vec (filter (fn [line]
                                     (> (line :timestamp) cutoff-age)) curr-history))]
      (ls-topo-hist! topo-history-state new-history))))

(defn cleanup-corrupt-topologies! [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        code-ids (set (code-ids (:blob-store nimbus)))
        active-topologies (set (.active-storms storm-cluster-state))
        corrupt-topologies (set/difference active-topologies code-ids)]
    (doseq [corrupt corrupt-topologies]
      (log-message "Corrupt topology " corrupt " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...")
      (.remove-storm! storm-cluster-state corrupt)
      )))

(defn- get-errors [storm-cluster-state storm-id component-id]
  (->> (.errors storm-cluster-state storm-id component-id)
       (map #(doto (ErrorInfo. (:error %) (:time-secs %))
                   (.set_host (:host %))
                   (.set_port (:port %))))))

(defn- get-last-error
  [storm-cluster-state storm-id component-id]
  (if-let [e (.last-error storm-cluster-state storm-id component-id)]
    (doto (ErrorInfo. (:error e) (:time-secs e))
                      (.set_host (:host e))
                      (.set_port (:port e)))))

(defn- get-topology-errors [storm-cluster-state topology-id]
  (->> (.get-topology-errors storm-cluster-state topology-id)
    (map #(doto (ErrorInfo. (:error %) (:time-secs %))
            (.set_host (:host %))
            (.set_port (:port %))))))

(defn- thriftify-executor-id [[first-task-id last-task-id]]
  (ExecutorInfo. (int first-task-id) (int last-task-id)))

(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name! [name]
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
            (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
  (if (clojure.string/blank? name) 
    (throw (InvalidTopologyException. 
            ("Topology name cannot be blank"))))))

(defn add-topology-to-history-log
  [storm-id nimbus topology-conf]
  (log-message "Adding topo to history log: " storm-id)
  (locking (:topology-history-lock nimbus)
    (let [topo-history-state (:topo-history-state nimbus)
          users (get-topo-logs-users topology-conf)
          groups (get-topo-logs-groups topology-conf)
          curr-history (vec (ls-topo-hist topo-history-state))
          new-history (conj curr-history {:topoid storm-id :timestamp (current-time-secs)
                                          :users users :groups groups})]
      (ls-topo-hist! topo-history-state new-history))))

(defn igroup-mapper
  [storm-conf]
  (AuthUtils/GetGroupMappingServiceProviderPlugin storm-conf))

(defn user-groups
  [user storm-conf]
  (if (blank? user) [] (.getGroups (igroup-mapper storm-conf) user)))

(defn does-users-group-intersect?
  "Check to see if any of the users groups intersect with the list of groups passed in"
  [user groups-to-check storm-conf]
  (let [groups (user-groups user storm-conf)]
    (> (.size (intersection (set groups) (set groups-to-check))) 0)))

(defn read-topology-history
  [nimbus user admin-users]
  (let [topo-history-state (:topo-history-state nimbus)
        curr-history (vec (ls-topo-hist topo-history-state))
        topo-user-can-access (fn [line user storm-conf]
                               (if (nil? user)
                                 (line :topoid)
                                 (if (or (some #(= % user) admin-users)
                                       (does-users-group-intersect? user (line :groups) storm-conf)
                                       (some #(= % user) (line :users)))
                                   (line :topoid)
                                   nil)))]
    (remove nil? (map #(topo-user-can-access % user (:conf nimbus)) curr-history))))

(defn renew-credentials [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        blob-store (:blob-store nimbus)
        renewers (:cred-renewers nimbus)
        update-lock (:cred-update-lock nimbus)
        assigned-ids (set (.active-storms storm-cluster-state))]
    (when-not (empty? assigned-ids)
      (doseq [id assigned-ids]
        (locking update-lock
          (let [orig-creds (.credentials storm-cluster-state id nil)
                topology-conf (try-read-storm-conf-or-nil (:conf nimbus) id nimbus)]
            (when (and topology-conf orig-creds)
              (let [new-creds (HashMap. orig-creds)]
                (doseq [renewer renewers]
                  (log-message "Renewing Creds For " id " with " renewer)
                  (.renew renewer new-creds (Collections/unmodifiableMap topology-conf)))
                (when-not (= orig-creds new-creds)
                  (.set-credentials! storm-cluster-state id new-creds topology-conf))))))))))

(defn validate-topology-size [topo-conf nimbus-conf topology]
  (let [workers-count (get topo-conf TOPOLOGY-WORKERS)
        workers-allowed (get nimbus-conf NIMBUS-SLOTS-PER-TOPOLOGY)
        num-executors (->> (all-components topology) (map-val num-start-executors))
        executors-count (reduce + (vals num-executors))
        executors-allowed (get nimbus-conf NIMBUS-EXECUTORS-PER-TOPOLOGY)]
    (when (and 
           (not (nil? executors-allowed))
           (> executors-count executors-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " executors-allowed " executors."))))
    (when (and
           (not (nil? workers-allowed))
           (> workers-count workers-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " workers-allowed " workers."))))))

(defn- set-logger-timeouts [log-config]
  (let [timeout-secs (.get_reset_log_level_timeout_secs log-config)
       timeout (time/plus (time/now) (time/secs timeout-secs))]
   (if (time/after? timeout (time/now))
     (.set_reset_log_level_timeout_epoch log-config (coerce/to-long timeout))
     (.unset_reset_log_level_timeout_epoch log-config))))

(defn make-supervisor-summary 
  [nimbus id info]
    (let [ports (set (:meta info)) ;;TODO: this is only true for standalone
          sup-sum (SupervisorSummary. (:hostname info)
                                      (:uptime-secs info)
                                      (count ports)
                                      (count (:used-ports info))
                                      id)]
      (.set_total_resources sup-sum (map-val double (:resources-map info)))
      (when-let [supervisor-resources (.get @(:node-id->resources nimbus) id)]
        (.set_used_mem sup-sum (.getUsedMem supervisor-resources))
        (.set_used_cpu sup-sum (.getUsedCpu supervisor-resources))
        (if (is-fragmented? (:conf nimbus) supervisor-resources) 
          (.set_fragmented_mem sup-sum (max (.getAvailableMem supervisor-resources) 0.0)))
        (if (is-fragmented? (:conf nimbus) supervisor-resources) 
          (.set_fragmented_cpu sup-sum (max (.getAvailableCpu supervisor-resources) 0.0))))
      (when-let [version (:version info)] (.set_version sup-sum version))
      sup-sum))

(defn user-and-supervisor-topos
  [nimbus conf blob-store assignments supervisor-id]
  (let [topo-id->supervisors 
          (into {} (for [[topo-id assignment] assignments] 
                     {topo-id (into #{} 
                                    (map #(first (second %)) 
                                         (:executor->node+port assignment)))}))
        supervisor-topologies (keys (filter #(get (val %) supervisor-id) topo-id->supervisors))]
    {:supervisor-topologies supervisor-topologies
     :user-topologies (into #{} (filter (partial is-authorized? nimbus 
                                                 conf 
                                                 blob-store 
                                                 "getTopology") 
                  supervisor-topologies))}))

(defn topology-assignments 
  [storm-cluster-state]
  (let [assigned-topology-ids (.assignments storm-cluster-state nil)]
    (into {} (for [tid assigned-topology-ids]
               {tid (.assignment-info storm-cluster-state tid nil)}))))

(defn get-launch-time-secs 
  [base storm-id]
  (if base (:launch-time-secs base)
    (throw
      (NotAliveException. (str storm-id)))))

(defn estimated-worker-count-for-ras-topo [storm-conf topology]
  (let [componet-parallelism-mp (into {} (map #(vector (first %) (component-parallelism storm-conf (second %))) (all-components topology)))
        bolt-memory-requirement (into {} (map #(vector (first %) (get (second %) TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB)) (into {} (ResourceUtils/getBoltsResources topology storm-conf))))
        spout-memory-requirement (into {} (map #(vector (first %) (get (second %) TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB)) (into {} (ResourceUtils/getSpoutsResources topology storm-conf))))
        component-memory-requirement (merge bolt-memory-requirement spout-memory-requirement)
        total-memory-required (reduce + (vals (merge-with * component-memory-requirement componet-parallelism-mp)))
        worker-heap-memory (get storm-conf WORKER-HEAP-MEMORY-MB)
        ]
    (/ total-memory-required worker-heap-memory)))

(defn num-acker-executors [storm-conf topology]
  (or
    (get storm-conf TOPOLOGY-ACKER-EXECUTORS)
      (if (Utils/isRAS storm-conf)
        (estimated-worker-count-for-ras-topo storm-conf topology)
        (get storm-conf TOPOLOGY-WORKERS))))

(defn sum-topo-resources [^TopologyResources a ^TopologyResources b]
  (.add a b))

(defserverfn service-handler [conf inimbus]
  (.prepare inimbus conf (master-inimbus-dir conf))
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)
        blob-store (:blob-store nimbus)
        principal-to-local (AuthUtils/GetPrincipalToLocalPlugin conf)
        admin-users (or (.get conf NIMBUS-ADMINS) [])
        get-common-topo-info
          (fn [^String storm-id operation]
            (let [storm-cluster-state (:storm-cluster-state nimbus)
                  topology-conf (try-read-storm-conf conf storm-id nimbus)
                  storm-name (topology-conf TOPOLOGY-NAME)
                  _ (check-authorization! nimbus
                                          storm-name
                                          topology-conf
                                          operation)
                  topology (get-system-topology topology-conf (try-read-storm-topology storm-id nimbus) storm-id nimbus)
                  task->component (storm-task-info topology topology-conf)
                  base (.storm-base storm-cluster-state storm-id nil)
                  launch-time-secs (get-launch-time-secs base storm-id)
                  assignment (.assignment-info storm-cluster-state storm-id nil)
                  beats (map-val :heartbeat (get @(:heartbeats-cache nimbus)
                                                 storm-id))
                  all-components (set (vals task->component))]
              {:storm-name storm-name
               :storm-cluster-state storm-cluster-state
               :all-components all-components
               :launch-time-secs launch-time-secs
               :assignment assignment
               :beats beats
               :topology topology
               :topology-conf topology-conf
               :task->component task->component
               :base base}))
        set-resources-default-if-not-set
          (fn [^HashMap component-resources-map component-id topology-conf]
              (let [resource-map (or (.get component-resources-map component-id) (HashMap.))]
                (ResourceUtils/checkInitialization resource-map component-id topology-conf)
                resource-map))
        get-last-error (fn [storm-cluster-state storm-id component-id]
                         (if-let [e (.last-error storm-cluster-state
                                                 storm-id
                                                 component-id)]
                           (ErrorInfo. (:error e) (:time-secs e))))]

    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)
    (cleanup-corrupt-topologies! nimbus)
    (doseq [storm-id (.active-storms (:storm-cluster-state nimbus))]
      (transition! nimbus storm-id :startup))
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (when-not (conf NIMBUS-DO-NOT-REASSIGN)
                            (try-cause
                              (mk-assignments nimbus)
                              (catch KeyNotFoundException e
                                (log-error e "Failed make new assignments! Will retry..."))))
                          (do-cleanup nimbus)
                          ))
    ;; Schedule Nimbus inbox cleaner
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
                        (fn []
                          (clean-inbox (inbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))
                          ))
    (when-let [interval (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
      (schedule-recurring (:timer nimbus)
                        0
                        (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)
                        (fn []
                          (clean-topology-history (conf LOGVIEWER-CLEANUP-AGE-MINS) nimbus)
                          )))
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CREDENTIAL-RENEW-FREQ-SECS)
                        (fn []
                          (renew-credentials nimbus)))

    (defgauge nimbus:fragmented-memory
              (fn [] (fragmented-memory @(:node-id->resources nimbus) conf)))

    (defgauge nimbus:fragmented-cpu
              (fn [] (fragmented-cpu @(:node-id->resources nimbus) conf)))

    (defgauge nimbus:available-memory
      (fn [] (reduce  + (for [x (vals @(:node-id->resources nimbus))] (.getAvailableMem ^SupervisorResources x)))))

    (defgauge nimbus:available-cpu
      (fn [] (reduce  + (for [x (vals @(:node-id->resources nimbus))] (.getAvailableCpu ^SupervisorResources x)))))

    (defgauge nimbus:total-memory
      (fn [] (reduce  + (for [x (vals @(:node-id->resources nimbus))] (.getTotalMem ^SupervisorResources x)))))

    (defgauge nimbus:total-cpu
      (fn [] (reduce  + (for [x (vals @(:node-id->resources nimbus))] (.getTotalCpu ^SupervisorResources x)))))

    (defgauge nimbus:num-supervisors
      (fn [] (.size (.supervisors (:storm-cluster-state nimbus) nil))))

    (start-metrics-reporters conf)

    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (try
          (mark! nimbus:num-submitTopologyWithOpts-calls)
          (assert (not-nil? submitOptions))
          (validate-topology-name! storm-name)
          (check-authorization! nimbus storm-name nil "submitTopology")
          (check-storm-active! nimbus storm-name false)
          (let [topo-conf (from-json serializedConf)]
            (try
              (validate-configs-with-schemas topo-conf)
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)
                       storm-name
                       topo-conf
                       topology)
          ;; server side validation for the blobstore map set
          (Utils/validateTopologyBlobStoreMap topo-conf blob-store))
          (swap! (:submitted-count nimbus) inc)
          (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))
                credentials (.get_creds submitOptions)
                credentials (when credentials (.get_creds credentials))
                topo-conf (from-json serializedConf)
                storm-conf-submitted (normalize-conf
                            conf
                            (-> topo-conf
                              (assoc STORM-ID storm-id)
                              (assoc TOPOLOGY-NAME storm-name))
                            topology)
                req (ReqContext/context)
                principal (.principal req)
                submitter-principal (if principal (.toString principal))
                submitter-user (.toLocal principal-to-local principal)
                system-user (. System (getProperty "user.name")) ;should only be used for non-secure mode
                topo-acl (distinct (remove nil? (conj (.get storm-conf-submitted TOPOLOGY-USERS) submitter-principal, submitter-user)))
                submitter-principal (if submitter-principal submitter-principal "")
                submitter-user (if submitter-user submitter-user system-user)
                storm-conf (-> storm-conf-submitted
                               (assoc TOPOLOGY-SUBMITTER-PRINCIPAL submitter-principal)
                               (assoc TOPOLOGY-SUBMITTER-USER submitter-user) ;if there is no kerberos principal, then use the user name
                               (assoc TOPOLOGY-USERS topo-acl)
                               (assoc STORM-ZOOKEEPER-SUPERACL (.get conf STORM-ZOOKEEPER-SUPERACL)))
                storm-conf (if (Utils/isZkAuthenticationConfiguredStormServer conf)
                                storm-conf
                                (dissoc storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
                storm-conf (if (conf STORM-TOPOLOGY-CLASSPATH-BEGINNING-ENABLED)
                             storm-conf
                             (dissoc storm-conf TOPOLOGY-CLASSPATH-BEGINNING))
                total-storm-conf (merge conf storm-conf)
                total-storm-conf (-> total-storm-conf (assoc TOPOLOGY-ACKER-EXECUTORS (num-acker-executors total-storm-conf topology )))
                topology (normalize-topology total-storm-conf topology)

                storm-cluster-state (:storm-cluster-state nimbus)]
            (log-message "Config.TOPOLOGY_ACKER_EXECUTORS set to: " (get total-storm-conf TOPOLOGY-ACKER-EXECUTORS))
            (when credentials (doseq [nimbus-autocred-plugin (:nimbus-autocred-plugins nimbus)]
              (.populateCredentials nimbus-autocred-plugin credentials (Collections/unmodifiableMap storm-conf))))
            (if (and (conf SUPERVISOR-RUN-WORKER-AS-USER) (or (nil? submitter-user) (.isEmpty (.trim submitter-user)))) 
              (throw (AuthorizationException. "Could not determine the user to run this topology as.")))
            (get-system-topology total-storm-conf topology storm-id nimbus) ;; this validates the structure of the topology
            (validate-topology-size storm-conf conf topology)
            (when (and (Utils/isZkAuthenticationConfiguredStormServer conf)
                       (not (Utils/isZkAuthenticationConfiguredTopology storm-conf)))
                (throw (IllegalArgumentException. "The cluster is configured for zookeeper authentication, but no payload was provided.")))
            (log-message "Received topology submission for "
                         storm-name
                         " with conf "
                         (redact-value storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (:submit-lock nimbus)
              (check-storm-active! nimbus storm-name false)
              ;;cred-update-lock is not needed here because creds are being added for the first time.
              (.set-credentials! storm-cluster-state storm-id credentials total-storm-conf)
              (setup-storm-code conf storm-id uploadedJarLocation total-storm-conf topology (:blob-store nimbus))
              (.setup-heartbeats! storm-cluster-state storm-id)
              (if (total-storm-conf TOPOLOGY-BACKPRESSURE-ENABLE)
                (.setup-backpressure! storm-cluster-state storm-id))
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE :inactive
                                              TopologyInitialStatus/ACTIVE :active}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions)) submitter-user submitter-principal))))
          (catch Throwable e
            (log-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))
      
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (mark! nimbus:num-submitTopology-calls)
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))
      
      (^void killTopology [this ^String name]
        (mark! nimbus:num-killTopology-calls)
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (mark! nimbus:num-killTopologyWithOpts-calls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              storm-id (topology-conf STORM-ID)]
          (check-authorization! nimbus storm-name topology-conf "killTopology")
          (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options)                         
                         )]
            (transition-name! nimbus storm-name [:kill wait-amt] true))
          (if (topology-conf TOPOLOGY-BACKPRESSURE-ENABLE)
            (.remove-backpressure! (:storm-cluster-state nimbus) storm-id))
          (add-topology-to-history-log storm-id
            nimbus topology-conf)))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (mark! nimbus:num-rebalance-calls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "rebalance"))
        ;; Set principal in RebalanceOptions to nil because users are not suppose to set this
        (.set_principal options nil)
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options))
              num-workers (if (.is_set_num_workers options)
                            (.get_num_workers options))
              executor-overrides (if (.is_set_num_executors options)
                                   (.get_num_executors options)
                                   {})
              resource-overrides (if (.is_set_topology_resources_overrides options)
                                   (into {} (.get_topology_resources_overrides options))
                                   {})
              topology-config-overrides (if (.is_set_topology_conf_overrides options)
                                          (into {} (Utils/parseJson(.get_topology_conf_overrides options)))
                                          {})
              topology-config-overrides-cleansed (-> topology-config-overrides
                                                   (dissoc TOPOLOGY-SUBMITTER-PRINCIPAL)
                                                   (dissoc TOPOLOGY-SUBMITTER-USER)
                                                   (dissoc STORM-ZOOKEEPER-SUPERACL))
              topology-config-overrides-cleansed (if (Utils/isZkAuthenticationConfiguredStormServer conf)
                                                   topology-config-overrides-cleansed
                                                   (dissoc topology-config-overrides-cleansed STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
              topology-config-overrides-cleansed (if (conf STORM-TOPOLOGY-CLASSPATH-BEGINNING-ENABLED)
                                                   topology-config-overrides-cleansed
                                                   (dissoc topology-config-overrides-cleansed TOPOLOGY-CLASSPATH-BEGINNING))
              subject (get-subject)]
          (log-message "Topology: " storm-name " with Subject: " subject " Overrides - [Resources]: " resource-overrides " [Topology Conf]: " topology-config-overrides-cleansed)
          (doseq [[c num-executors] executor-overrides]
            (when (<= num-executors 0)
              (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
              ))
          (transition-name! nimbus storm-name [:rebalance wait-amt num-workers executor-overrides resource-overrides topology-config-overrides-cleansed subject] true)
          ))

      (activate [this storm-name]
        (mark! nimbus:num-activate-calls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "activate"))
        (transition-name! nimbus storm-name :activate true)
        )

      (deactivate [this storm-name]
        (mark! nimbus:num-deactivate-calls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)]
          (check-authorization! nimbus storm-name topology-conf "deactivate"))
        (transition-name! nimbus storm-name :inactivate true))

      (^void setWorkerProfiler
        [this ^String id ^ProfileRequest profileRequest]
        (mark! nimbus:num-setWorkerProfiler-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setWorkerProfiler")
              storm-cluster-state (:storm-cluster-state nimbus)]
              (.set-worker-profile-request storm-cluster-state id profileRequest)))

      (^List getComponentPendingProfileActions
        [this ^String id ^String component_id ^ProfileAction action]
        (mark! nimbus:num-getComponentPendingProfileActions-calls)
        (let [info (get-common-topo-info id "getComponentPendingProfileActions")
              storm-cluster-state (:storm-cluster-state info)
              task->component (:task->component info)
              {:keys [executor->node+port node->host]} (:assignment info)
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                    executor->node+port)
              nodeinfos (stats/extract-nodeinfos-from-hb-for-comp executor->host+port task->component false component_id)
              all-pending-actions-for-topology (.get-topology-profile-requests storm-cluster-state id true)
              latest-profile-actions (remove nil? (map (fn [nodeInfo]
                                                         (->> all-pending-actions-for-topology
                                                              (filter #(and (= (:host nodeInfo) (.get_node (.get_nodeInfo %)))
                                                                         (= (:port nodeInfo) (first (.get_port (.get_nodeInfo  %))))))
                                                              (filter #(= action (.get_action %)))
                                                              (sort-by #(.get_time_stamp %) >)
                                                              first))
                                                    nodeinfos))]
          (log-message "Latest profile actions for topology " id " component " component_id " " (pr-str latest-profile-actions))
          latest-profile-actions))

      (^void setLogConfig [this ^String id ^LogConfig log-config-msg] 
        (mark! nimbus:num-setLogConfig-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setLogConfig")
              storm-cluster-state (:storm-cluster-state nimbus)
              merged-log-config (or (.topology-log-config storm-cluster-state id nil) (LogConfig.))
              named-loggers (.get_named_logger_level merged-log-config)]
            (doseq [[_ level] named-loggers]
              (.set_action level LogLevelAction/UNCHANGED))
            (doseq [[logger-name log-config] (.get_named_logger_level log-config-msg)]
              (let [action (.get_action log-config)]
                (if (clojure.string/blank? logger-name)
                  (throw (RuntimeException. "Named loggers need a valid name. Use ROOT for the root logger")))
                (condp = action
                  LogLevelAction/UPDATE
                    (do (set-logger-timeouts log-config)
                          (.put_to_named_logger_level merged-log-config logger-name log-config))
                  LogLevelAction/REMOVE
                    (let [named-loggers (.get_named_logger_level merged-log-config)]
                      (if (and (not (nil? named-loggers))
                               (.containsKey named-loggers logger-name))
                        (.remove named-loggers logger-name))))))
            (log-message "Setting log config for " storm-name ":" merged-log-config)
            (.set-topology-log-config! storm-cluster-state id merged-log-config)))

      (uploadNewCredentials [this storm-name credentials]
        (mark! nimbus:num-uploadNewCredentials-calls)
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              storm-id (get-storm-id storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id nimbus)
              creds (when credentials (.get_creds credentials))]
          (check-authorization! nimbus storm-name topology-conf "uploadNewCredentials")
          (locking (:cred-update-lock nimbus) (.set-credentials! storm-cluster-state storm-id creds topology-conf))))

      (beginFileUpload [this]
        (mark! nimbus:num-beginFileUpload-calls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [fileloc (str (inbox nimbus) "/stormjar-" (uuid) ".jar")]
          (.put (:uploaders nimbus)
                fileloc
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (mark! nimbus:num-uploadChunk-calls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.write channel chunk)
          (.put uploaders location channel)
          ))

      (^void finishFileUpload [this ^String location]
        (mark! nimbus:num-finishFileUpload-calls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.close channel)
          (log-message "Finished uploading file from client: " location)
          (.remove uploaders location)))

      (^String beginFileDownload [this ^String file]
        (mark! nimbus:num-beginFileDownload-calls)
        (check-authorization! nimbus nil nil "fileDownload")
        (let [is (BufferInputStream. (.getBlob (:blob-store nimbus) file nil) ^Integer (Utils/getInt (conf STORM-BLOBSTORE-INPUTSTREAM-BUFFER-SIZE-BYTES) (int 65536)))
              id (uuid)]
          (.put (:downloaders nimbus) id is)
          id
          ))

      (^ByteBuffer downloadChunk [this ^String id]
        (mark! nimbus:num-downloadChunk-calls)
        (check-authorization! nimbus nil nil "fileDownload")
        (let [downloaders (:downloaders nimbus)
              ^BufferInputStream is (.get downloaders id)]
          (when-not is
            (throw (RuntimeException.
                    "Could not find input stream for id " id)))
          (let [ret (.read is)]
            (.put downloaders id is)
            (when (empty? ret)
              (.close is)
              (.remove downloaders id))
            (ByteBuffer/wrap ret)
            )))

      (^String getNimbusConf [this]
        (mark! nimbus:num-getNimbusConf-calls)
        (check-authorization! nimbus nil nil "getNimbusConf")
        (to-json (:conf nimbus)))

      (^String getSchedulerConf [this]
       (check-authorization! nimbus nil nil "getSchedulerConf")
       (to-json (.config (:scheduler nimbus))))
      (^LogConfig getLogConfig [this ^String id]
        (mark! nimbus:num-getLogConfig-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "getLogConfig")
             storm-cluster-state (:storm-cluster-state nimbus)
             log-config (.topology-log-config storm-cluster-state id nil)]
           (if log-config log-config (LogConfig.))))

      (^String getTopologyConf [this ^String id]
        (mark! nimbus:num-getTopologyConf-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopologyConf")
              (to-json topology-conf)))

      (^List getOwnerResourceSummaries [this ^String owner]
        (mark! nimbus:num-getOwnerResourceSummaries-calls)
        (let [_ (check-authorization! nimbus nil nil "getOwnerResourceSummaries")
              storm-cluster-state (:storm-cluster-state nimbus)
              assignments (topology-assignments storm-cluster-state)
              storm-id->bases (into {} (topology-bases storm-cluster-state))
              query-bases (if (nil? owner)
                            storm-id->bases ;; all your bases are belong to us
                            (filter #(= owner (:owner (val %))) storm-id->bases))
              cluster-scheduler-config (.config (:scheduler nimbus))
              owners-with-guarantees (if (nil? owner)
                                       (zipmap (keys cluster-scheduler-config) (repeat []))
                                       {owner []})
              default-resources (TopologyResources. )]
              ;; for each owner, get resources, configs, and aggregate
              (for [[owner owner-bases] (merge-with concat (group-by #(:owner (val %)) query-bases) owners-with-guarantees)] 
                (let [scheduler-config (get cluster-scheduler-config owner)
                      topo-confs (into {} 
                                       (filter (comp some? val)
                                         (into {} 
                                           (map #(into {} {% (try-read-storm-conf-or-nil conf % nimbus)}) (keys owner-bases)))))
                      resources (into {}
                                      (map #(into {} {% (get-resources-for-topology nimbus % (get owner-bases %))})
                                                  (keys topo-confs)))
                      ras-topos (if (Utils/isRAS conf)
                                  (keys topo-confs)
                                  nil)
                      ras-resources (select-keys resources ras-topos)
                      total-aggregates (if (empty? resources)
                                         default-resources
                                         (reduce sum-topo-resources (vals resources)))
                      ras-aggregates (if (empty? ras-resources)
                                       default-resources
                                       (reduce sum-topo-resources (vals ras-resources)))
                      requested-total-memory (+ (.getRequestedMemOnHeap total-aggregates)
                                                (.getRequestedMemOffHeap total-aggregates))
                      assigned-total-memory (+ (.getAssignedMemOnHeap total-aggregates)
                                               (.getAssignedMemOffHeap total-aggregates)) 
                      assigned-ras-total-memory (+ (.getAssignedMemOnHeap ras-aggregates)
                                                   (.getAssignedMemOffHeap ras-aggregates))
                      total-cpu (.getAssignedCpu total-aggregates)
                      ras-total-cpu (.getAssignedCpu ras-aggregates)
                      owner-assignments (map #(get assignments (first %)) owner-bases) 
                      total-executors (reduce + (flatten (map #(->> (:executor->node+port %) keys count) owner-assignments)))
                      total-workers (reduce + (flatten (map #(->> (:executor->node+port %) vals set count) owner-assignments)))
                      total-tasks (reduce + (flatten (map #(->> (:executor->node+port %) keys (mapcat executor-id->tasks) count) owner-assignments)))
                      memory-guarantee (get-in scheduler-config ["memory"])
                      cpu-guarantee (get-in scheduler-config ["cpu"])
                      isolated-nodes (get-in scheduler-config ["MultitenantScheduler"])
                      memory-guarantee-remaining (- (or memory-guarantee 0)
                                                    (or assigned-ras-total-memory 0))
                      cpu-guarantee-remaining (- (or cpu-guarantee 0) 
                                                 (or ras-total-cpu 0))
                      summary (doto (OwnerResourceSummary. owner)
                                (.set_total_topologies (count owner-bases))
                                (.set_total_executors total-executors)
                                (.set_total_workers total-workers)
                                (.set_memory_usage assigned-total-memory)
                                (.set_cpu_usage total-cpu)
                                (.set_total_tasks total-tasks) 
                                (.set_requested_on_heap_memory (.getRequestedMemOnHeap total-aggregates))
                                (.set_requested_off_heap_memory (.getRequestedMemOffHeap total-aggregates))
                                (.set_requested_total_memory requested-total-memory)
                                (.set_requested_cpu (.getRequestedCpu total-aggregates))
                                (.set_assigned_on_heap_memory (.getAssignedMemOnHeap total-aggregates))
                                (.set_assigned_off_heap_memory (.getAssignedMemOffHeap total-aggregates))
                                (.set_assigned_ras_total_memory assigned-ras-total-memory)
                                (.set_assigned_ras_cpu ras-total-cpu)
                                (.set_requested_shared_off_heap_memory (.getRequestedSharedMemOffHeap total-aggregates))
                                (.set_requested_regular_off_heap_memory (.getRequestedNonSharedMemOffHeap total-aggregates))
                                (.set_requested_shared_on_heap_memory (.getRequestedSharedMemOnHeap total-aggregates))
                                (.set_requested_regular_on_heap_memory (.getRequestedNonSharedMemOnHeap total-aggregates))
                                (.set_assigned_shared_off_heap_memory (.getAssignedSharedMemOffHeap total-aggregates))
                                (.set_assigned_regular_off_heap_memory (.getAssignedNonSharedMemOffHeap total-aggregates))
                                (.set_assigned_shared_on_heap_memory (.getAssignedSharedMemOnHeap total-aggregates))
                                (.set_assigned_regular_on_heap_memory (.getAssignedNonSharedMemOnHeap total-aggregates)))]

                      (when memory-guarantee
                        (.set_memory_guarantee summary memory-guarantee)
                        (.set_memory_guarantee_remaining summary memory-guarantee-remaining))

                      (when cpu-guarantee
                        (.set_cpu_guarantee summary cpu-guarantee)
                        (.set_cpu_guarantee_remaining summary cpu-guarantee-remaining))

                      (when isolated-nodes
                        (.set_isolated_node_guarantee summary isolated-nodes))

                      summary))))

      (^StormTopology getTopology [this ^String id]
        (mark! nimbus:num-getTopology-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopology")
              (get-system-topology topology-conf (try-read-storm-topology id nimbus) id nimbus)))

      (^StormTopology getUserTopology [this ^String id]
        (mark! nimbus:num-getUserTopology-calls)
        (let [topology-conf (try-read-storm-conf conf id nimbus)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getUserTopology")
              (try-read-storm-topology id nimbus)))

      (^ClusterSummary getClusterInfo [this]
        (mark! nimbus:num-getClusterInfo-calls)
        (check-authorization! nimbus nil nil "getClusterInfo")
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state nil true)
              ;; TODO: need to get the port info about supervisors...
              ;; in standalone just look at metadata, otherwise just say N/A?
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                            (make-supervisor-summary nimbus id info))
              nimbus-uptime ((:uptime nimbus))
              bases (topology-bases storm-cluster-state)
              topology-summaries (dofor [[id base] bases :when base]
	                                  (let [assignment (.assignment-info storm-cluster-state id nil)
                                                topo-summ (TopologySummary. id
                                                            (:storm-name base)
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 (mapcat executor-id->tasks)
                                                                 count) 
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 count)                                                            
                                                            (->> (:executor->node+port assignment)
                                                                 vals
                                                                 set
                                                                 count)
                                                            (time-delta (:launch-time-secs base))
                                                            (extract-status-str base))]
                                               (when-let [owner (:owner base)] (.set_owner topo-summ owner))
                                               (when-let [topology-version (:topology-version base)] (.set_topology_version topo-summ topology-version))
                                               (when-let [sched-status (.get @(:id->sched-status nimbus) id)] (.set_sched_status topo-summ sched-status))
                                               (when-let [resources (get-resources-for-topology nimbus id base)]
                                                 (.set_requested_memonheap topo-summ (.getRequestedMemOnHeap resources))
                                                 (.set_requested_memoffheap topo-summ (.getRequestedMemOffHeap resources))
                                                 (.set_requested_cpu topo-summ (.getRequestedCpu resources))
                                                 (.set_assigned_memonheap topo-summ (.getAssignedMemOnHeap resources))
                                                 (.set_assigned_memoffheap topo-summ (.getAssignedMemOffHeap resources))
                                                 (.set_assigned_cpu topo-summ (.getAssignedCpu resources)))
                                               topo-summ
                                          ))]
          (ClusterSummary. supervisor-summaries
                           nimbus-uptime
                           topology-summaries)
          ))
      
      (^TopologyInfo getTopologyInfoWithOpts [this ^String storm-id ^GetInfoOptions options]
        (mark! nimbus:num-getTopologyInfoWithOpts-calls)
        (let [{:keys [storm-name
                      storm-cluster-state
                      all-components
                      launch-time-secs
                      assignment
                      beats
                      task->component
                      base]} (get-common-topo-info storm-id "getTopologyInfo")
              num-err-choice (or (.get_num_err_choice options)
                                 NumErrorsChoice/ALL)
              errors-fn (condp = num-err-choice
                          NumErrorsChoice/NONE (fn [& _] ()) ;; empty list only
                          NumErrorsChoice/ONE (comp #(remove nil? %)
                                                    list
                                                    stats/get-last-error)
                          NumErrorsChoice/ALL get-errors
                          ;; Default
                          (do
                            (log-warn "Got invalid NumErrorsChoice '"
                                      num-err-choice
                                      "'")
                            get-errors))
              errors (->> all-components
                          (map (fn [c] [c (errors-fn storm-cluster-state storm-id c)]))
                          (into {}))
              executor-summaries (dofor [[executor [node port]] (:executor->node+port assignment)]
                                        (let [host (-> assignment :node->host (get node))
                                              heartbeat (get beats executor)
                                              stats (:stats heartbeat)
                                              stats (if stats
                                                      (stats/thriftify-executor-stats stats))]
                                          (doto
                                              (ExecutorSummary. (thriftify-executor-id executor)
                                                                (-> executor first task->component)
                                                                host
                                                                port
                                                                (nil-to-zero (:uptime heartbeat)))
                                            (.set_stats stats))
                                          ))
              topo-info  (TopologyInfo. storm-id
                           storm-name
                           (time-delta launch-time-secs)
                           executor-summaries
                           (extract-status-str base)
                           errors
                           )]
            (when-let [owner (:owner base)] (.set_owner topo-info owner))
            (when-let [sched-status (.get @(:id->sched-status nimbus) storm-id)] (.set_sched_status topo-info sched-status))
            (when-let [resources (get-resources-for-topology nimbus storm-id base)]
              (.set_requested_memonheap topo-info (.getRequestedMemOnHeap resources))
              (.set_requested_memoffheap topo-info (.getRequestedMemOffHeap resources))
              (.set_requested_cpu topo-info (.getRequestedCpu resources))
              (.set_assigned_memonheap topo-info (.getAssignedMemOnHeap resources))
              (.set_assigned_memoffheap topo-info (.getAssignedMemOffHeap resources))
              (.set_assigned_cpu topo-info (.getAssignedCpu resources)))
            topo-info
          ))

      (^TopologyInfo getTopologyInfo [this ^String storm-id]
        (mark! nimbus:num-getTopologyInfo-calls)
        (.getTopologyInfoWithOpts this
                                  storm-id
                                  (doto (GetInfoOptions.) (.set_num_err_choice NumErrorsChoice/ALL))))

      (^TopologyPageInfo getTopologyPageInfo
        [this ^String storm-id ^String window ^boolean include-sys?]
        (mark! nimbus:num-getTopologyPageInfo-calls)
        (let [topo-info (get-common-topo-info storm-id "getTopologyPageInfo")
              {:keys [storm-name
                      storm-cluster-state
                      launch-time-secs
                      assignment
                      beats
                      task->component
                      topology
                      topology-conf
                      base]} topo-info
              exec->node+port (:executor->node+port assignment)
              worker->resources (get-worker-resources-for-topology nimbus storm-id)
              last-err-fn (partial get-last-error
                                   storm-cluster-state
                                   storm-id)
              worker-summaries (stats/agg-worker-stats storm-id 
                                                       topo-info
                                                       worker->resources
                                                       include-sys?
                                                       true)  ;; this is the topology page, so we know the user is authorized
              topo-page-info (stats/agg-topo-execs-stats storm-id
                                                         exec->node+port
                                                         task->component
                                                         beats
                                                         topology
                                                         window
                                                         include-sys?
                                                         last-err-fn)
              spout-resources (ResourceUtils/getSpoutsResources topology topology-conf)
              bolt-resources (ResourceUtils/getBoltsResources topology topology-conf)]

          (doseq [[spout-id component-aggregate-stats] (.get_id_to_spout_agg_stats topo-page-info)]
            (let [common-stats (.get_common_stats component-aggregate-stats)]
              (.set_resources_map common-stats (set-resources-default-if-not-set spout-resources spout-id topology-conf))))
          (doseq [[bolt-id component-aggregate-stats] (.get_id_to_bolt_agg_stats topo-page-info)]
            (let [common-stats (.get_common_stats component-aggregate-stats)]
              (.set_resources_map common-stats (set-resources-default-if-not-set bolt-resources bolt-id topology-conf))))
          (.set_workers topo-page-info worker-summaries)
          (when-let [owner (:owner base)]
            (.set_owner topo-page-info owner))
          (when-let [topology-version (:topology-version base)]
            (.set_topology_version topo-page-info topology-version))
          (when-let [sched-status (.get @(:id->sched-status nimbus) storm-id)]
            (.set_sched_status topo-page-info sched-status))
          (when-let [resources (get-resources-for-topology nimbus storm-id base)]
            (.set_requested_memonheap topo-page-info (.getRequestedMemOnHeap resources))
            (.set_requested_memoffheap topo-page-info (.getRequestedMemOffHeap resources))
            (.set_requested_cpu topo-page-info (.getRequestedCpu resources))
            (.set_assigned_memonheap topo-page-info (.getAssignedMemOnHeap resources))
            (.set_assigned_memoffheap topo-page-info (.getAssignedMemOffHeap resources))
            (.set_assigned_cpu topo-page-info (.getAssignedCpu resources))
            (.set_requested_shared_off_heap_memory topo-page-info (.getRequestedSharedMemOffHeap resources))
            (.set_requested_regular_off_heap_memory topo-page-info (.getRequestedNonSharedMemOffHeap resources))
            (.set_requested_shared_on_heap_memory topo-page-info (.getRequestedSharedMemOnHeap resources))
            (.set_requested_regular_on_heap_memory topo-page-info (.getRequestedNonSharedMemOnHeap resources))
            (.set_assigned_shared_off_heap_memory topo-page-info (.getAssignedSharedMemOffHeap resources))
            (.set_assigned_regular_off_heap_memory topo-page-info (.getAssignedNonSharedMemOffHeap resources))
            (.set_assigned_shared_on_heap_memory topo-page-info (.getAssignedSharedMemOnHeap resources))
            (.set_assigned_regular_on_heap_memory topo-page-info (.getAssignedNonSharedMemOnHeap resources)))

          (doto topo-page-info
            (.set_name storm-name)
            (.set_status (extract-status-str base))
            (.set_uptime_secs (time-delta launch-time-secs))
            (.set_topology_conf (to-json (try-read-storm-conf conf
                                                              storm-id
                                                              nimbus)))
            (.set_errors (get-topology-errors (:storm-cluster-state topo-info)
                           storm-id)))))

      (^ComponentPageInfo getComponentPageInfo
        [this
         ^String topology-id
         ^String component-id
         ^String window
         ^boolean include-sys?]
        (mark! nimbus:num-getComponentPageInfo-calls)
        (let [info (get-common-topo-info topology-id "getComponentPageInfo")
              {:keys [topology topology-conf]} info
              {:keys [executor->node+port node->host]} (:assignment info)
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                           executor->node+port)
              ret (stats/agg-comp-execs-stats executor->host+port
                                              (:task->component info)
                                              (:beats info)
                                              window
                                              include-sys?
                                              topology-id
                                              (:topology info)
                                              component-id)]
          (if (.equals (.get_component_type ret) ComponentType/SPOUT)
            (.set_resources_map ret (set-resources-default-if-not-set (ResourceUtils/getSpoutsResources topology topology-conf) component-id topology-conf))
            (.set_resources_map ret (set-resources-default-if-not-set (ResourceUtils/getBoltsResources topology topology-conf) component-id topology-conf)))
          (doto ret
            (.set_topology_name (:storm-name info))
            (.set_errors (get-errors (:storm-cluster-state info)
                                     topology-id
                                     component-id)))))

      (^SupervisorPageInfo getSupervisorPageInfo
        [this
         ^String supervisor-id
         ^String host 
         ^boolean include-sys?]
        (mark! nimbus:num-getSupervisorPageInfo-calls)
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              host->supervisor-id (reverse-map (map-val :hostname supervisor-infos))
              supervisor-ids (if (nil? supervisor-id)
                                (get host->supervisor-id host)
                                  [supervisor-id])
              page-info (SupervisorPageInfo.)]
              (doseq [sid supervisor-ids]
                (let [supervisor-info (get supervisor-infos sid)
                      sup-sum (make-supervisor-summary nimbus sid supervisor-info)
                      _ (.add_to_supervisor_summaries page-info sup-sum)
                      topo-id->assignments (topology-assignments storm-cluster-state)
                      {:keys [user-topologies 
                              supervisor-topologies]} (user-and-supervisor-topos nimbus
                                                                                conf
                                                                                blob-store
                                                                                topo-id->assignments 
                                                                                sid)]
                  (doseq [storm-id supervisor-topologies]
                      (let [topo-info (get-common-topo-info storm-id "getSupervisorPageInfo")
                            worker->resources (get-worker-resources-for-topology nimbus storm-id)]
                        (doseq [worker-summary (stats/agg-worker-stats storm-id 
                                                                       topo-info
                                                                       worker->resources
                                                                       include-sys?
                                                                       (get user-topologies storm-id)
                                                                       sid)]
                          (.add_to_worker_summaries page-info worker-summary)))))) 
              page-info))

      (^String beginCreateBlob [this
                                ^String blob-key
                                ^SettableBlobMeta blob-meta]
        (mark! nimbus:num-beginCreateBlob-calls)
        (let [session-id (uuid)]
          (.put (:blob-uploaders nimbus)
                session-id
                (->> (ReqContext/context)
                     (.subject)
                     (.createBlob (:blob-store nimbus) blob-key blob-meta)))
          (log-message "Created blob for " blob-key
                       " with session id " session-id)
          (str session-id)))

      (^String beginUpdateBlob [this ^String blob-key]
        (mark! nimbus:num-beginUpdateBlob-calls)
        (let [^AtomicOutputStream os (->> (ReqContext/context)
                                       (.subject)
                                       (.updateBlob (:blob-store nimbus)
                                         blob-key))]
          (let [session-id (uuid)]
            (.put (:blob-uploaders nimbus) session-id os)
            (log-message "Created upload session for " blob-key
              " with id " session-id)
            (str session-id))))

      (^void uploadBlobChunk [this ^String session ^ByteBuffer blob-chunk]
        (let [uploaders (:blob-uploaders nimbus)]
          (if-let [^AtomicOutputStream os (.get uploaders session)]
            (let [chunk-array (.array blob-chunk)
                  remaining (.remaining blob-chunk)
                  array-offset (.arrayOffset blob-chunk)
                  position (.position blob-chunk)]
              (.write os chunk-array (+ array-offset position) remaining)
              (.put uploaders session os))
            (throw-runtime "Blob for session "
                           session
                           " does not exist (or timed out)"))))

      (^void finishBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (:blob-uploaders nimbus) session)]
          (do
            (.close os)
            (log-message "Finished uploading blob for session "
                         session
                         ". Closing session.")
            (.remove (:blob-uploaders nimbus) session))
          (throw-runtime "Blob for session "
                         session
                         " does not exist (or timed out)")))

      (^void cancelBlobUpload [this ^String session]
        (if-let [^AtomicOutputStream os (.get (:blob-uploaders nimbus) session)]
          (do
            (.cancel os)
            (log-message "Canceled uploading blob for session "
                         session
                         ". Closing session.")
            (.remove (:blob-uploaders nimbus) session))
          (throw-runtime "Blob for session "
                         session
                         " does not exist (or timed out)")))

      (^ReadableBlobMeta getBlobMeta [this ^String blob-key]
        (let [^ReadableBlobMeta ret (->> (ReqContext/context)
                                         (.subject)
                                         (.getBlobMeta (:blob-store nimbus)
                                                        blob-key))]
          ret))

      (^void setBlobMeta [this ^String blob-key ^SettableBlobMeta blob-meta]
        (->> (ReqContext/context)
             (.subject)
             (.setBlobMeta (:blob-store nimbus) blob-key blob-meta)))

      (^BeginDownloadResult beginBlobDownload [this ^String blob-key]
        (let [^InputStreamWithMeta is (->> (ReqContext/context)
                                           (.subject)
                                           (.getBlob (:blob-store nimbus)
                                                      blob-key))]
          (let [session-id (uuid)
                ret (BeginDownloadResult. (.getVersion is) (str session-id))]
            (.set_data_size ret (.getFileLength is))
            (.put (:blob-downloaders nimbus) session-id (BufferInputStream. is ^Integer (Utils/getInt (conf STORM-BLOBSTORE-INPUTSTREAM-BUFFER-SIZE-BYTES) (int 65536))))
            (log-message "Created download session for " blob-key
                         " with id " session-id)
            ret)))

      (^ByteBuffer downloadBlobChunk [this ^String session]
        (let [downloaders (:blob-downloaders nimbus)
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
        (mark! nimbus:num-deleteBlob-calls)
        (->> (ReqContext/context)
             (.subject)
             (.deleteBlob (:blob-store nimbus) blob-key))
        (log-message "Deleted blob for key " blob-key))

      (^ListBlobsResult listBlobs [this ^String session]
        (mark! nimbus:num-listBlobs-calls)
        (let [listers (:blob-listers nimbus)
              ^Iterator keys-it (if (clojure.string/blank? session)
                                  (->> (ReqContext/context)
                                       (.subject)
                                       (.listKeys (:blob-store nimbus)))
                                  (.get listers session))
              _ (or keys-it (throw-runtime "Blob list for session "
                                           session
                                           " does not exist (or timed out)"))

              ;; Create a new session id if the user gave an empty session string.
              ;; This is the use case when the user wishes to list blobs
              ;; starting from the beginning.
              session (if (clojure.string/blank? session)
                        (let [new-session (uuid)]
                          (log-message "Creating new session for downloading list " new-session)
                          new-session)
                        session)]
          (if-not (.hasNext keys-it)
            (do
              (.remove listers session)
              (log-message "No more blobs to list for session " session)
              ;; A blank result communicates that there are no more blobs.
              (ListBlobsResult. (java.util.ArrayList. 0) (str session)))
            (let [^List list-chunk (->> keys-it
                                        (iterator-seq)
                                        (take 100) ;; Limit to next 100 keys
                                        (java.util.ArrayList.))
                  _ (log-message session " downloading " (.size list-chunk) " entries")]
              (.put listers session keys-it)
              (ListBlobsResult. list-chunk (str session))))))

      (^TopologyHistoryInfo getTopologyHistory [this ^String user]
        (mark! nimbus:num-getTopologyHistory-calls)
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              bases (topology-bases storm-cluster-state)
              assigned-topology-ids (.assignments storm-cluster-state nil)
              user-group-match-fn (fn [topo-id user conf]
                                    (let [topology-conf (try-read-storm-conf conf topo-id nimbus)
                                          groups (concat (conf NIMBUS-ADMINS-GROUPS) (get-topo-logs-groups topology-conf))]
                                      (or (nil? user)
                                          (some #(= % user) admin-users)
                                          (does-users-group-intersect? user groups conf)
                                          (some #(= % user) (get-topo-logs-users topology-conf)))))
              active-ids-for-user (filter #(user-group-match-fn % user (:conf nimbus)) assigned-topology-ids)
              topo-history-list (read-topology-history nimbus user admin-users)]
          (TopologyHistoryInfo. (distinct (concat active-ids-for-user topo-history-list)))))

      (^BlobReplication getBlobReplication [this ^String blob-key]
        (->> (ReqContext/context)
          (.subject)
          (.getBlobReplication (:blob-store nimbus) blob-key)))

      (^BlobReplication updateBlobReplication [this ^String blob-key ^int replication]
        (->> (ReqContext/context)
          (.subject)
          (.updateBlobReplication (:blob-store nimbus) blob-key replication)))

      Shutdownable
      (shutdown [this]
        (mark! nimbus:num-shutdown-calls)
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (.cleanup (:downloaders nimbus))
        (.cleanup (:uploaders nimbus))
        (.shutdown (:blob-store nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

(defn launch-server! [conf nimbus]
  (validate-distributed-mode! conf)
  (let [service-handler (service-handler conf nimbus)
        server (ThriftServer. conf (Nimbus$Processor. service-handler) 
                              ThriftConnectionType/NIMBUS)]
    (add-shutdown-hook-with-force-kill-in-1-sec (fn []
                                                  (.shutdown service-handler)
                                                  (.stop server)))
    (log-message "Starting nimbus server for storm version '"
                 STORM-VERSION
                 "'")
    (.serve server)
    service-handler))

(defn -launch [nimbus]
  (let [conf (merge
               (read-storm-config)
               (read-yaml-config "storm-cluster-auth.yaml" false))]
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
  (setup-default-uncaught-exception-handler)
  (-launch (standalone-nimbus)))
