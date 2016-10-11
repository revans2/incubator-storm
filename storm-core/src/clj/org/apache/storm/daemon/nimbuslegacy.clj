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
  (:import [org.apache.storm.daemon.nimbus Nimbus TopologyResources TopologyStateTransition Nimbus$Assoc Nimbus$Dissoc TopologyActions])
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

(defmulti blob-sync cluster-mode)

(declare mk-assignments)

(defn mk-assignments-scratch [nimbus storm-id]
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defmulti setup-jar cluster-mode)
(defmulti clean-inbox cluster-mode)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn get-key-seq-from-blob-store [blob-store]
  (let [key-iter (.listKeys blob-store)]
    (iterator-seq key-iter)))

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
                       node+port->slot (into {} (for [[[node port] [mem-on-heap mem-off-heap cpu]] worker->resources]
                                                  {[node port]
                                                   (WorkerSlot. node port mem-on-heap mem-off-heap cpu)}))
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (get node+port->slot [node port])}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))

(defn- read-all-supervisor-details
  [nimbus supervisor->dead-ports topologies missing-assignment-topologies]
  "return a map: {supervisor-id SupervisorDetails}"
  (let [storm-cluster-state (.getStormClusterState nimbus)
        supervisor-infos (.allSupervisorInfo storm-cluster-state)
        supervisor-details (for [[id info] supervisor-infos]
                             (SupervisorDetails. id (.get_meta info) (.get_resources_map info)))
        ;; Note that allSlotsAvailableForScheduling
        ;; only uses the supervisor-details. The rest of the arguments
        ;; are there to satisfy the INimbus interface.
        all-scheduling-slots (->> (.allSlotsAvailableForScheduling
                                    (.getINimbus nimbus)
                                    supervisor-details
                                    topologies
                                    (set missing-assignment-topologies))
                                  (map (fn [s] {(.getNodeId s) #{(.getPort s)}}))
                                  (apply merge-with set/union))]
    (into {} (for [[sid supervisor-info] supervisor-infos
                   :let [hostname (.get_hostname supervisor-info)
                         scheduler-meta (.get_scheduler_meta supervisor-info)
                         dead-ports (supervisor->dead-ports sid)
                         ;; hide the dead-ports from the all-ports
                         ;; these dead-ports can be reused in next round of assignments
                         all-ports (-> (get all-scheduling-slots sid)
                                       (set/difference dead-ports)
                                       (as-> ports (map int ports)))
                         supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports (.get_resources_map supervisor-info))]]
               {sid supervisor-details}))))


;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
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

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn convert-assignments-to-worker->resources [new-scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {[node port] [mem-on-heap mem-off-heap cpu]}}
   Make sure this can deal with other non-RAS schedulers
   later we may further support map-for-any-resources"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  .values
                  (#(into {} (for [^WorkerSlot slot %]
                              {[(.getNodeId slot) (.getPort slot)]
                               [(.getAllocatedMemOnHeap slot) (.getAllocatedMemOffHeap slot) (.getAllocatedCpu slot)]
                               })))))
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

;; public so it can be mocked out
(defn compute-new-scheduler-assignments [nimbus thrift-existing-assignments existing-assignments topologies scratch-topology-id]
  (let [conf (.getConf nimbus)
        storm-cluster-state (.getStormClusterState nimbus)
        topology->executors (clojurify-structure (.computeTopologyToExecutors nimbus (keys thrift-existing-assignments)))
        ;; update the executors heartbeats first.
        _ (.updateAllHeartbeats nimbus thrift-existing-assignments topology->executors)
        topology->alive-executors (clojurify-structure (.computeTopologyToAliveExecutors nimbus
                                                                     thrift-existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id))
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
        cluster (Cluster. (.getINimbus nimbus) supervisors topology->scheduler-assignment conf)]

    ;; set the status map with existing topology statuses
    (.setStatusMap cluster (.get (.getIdToSchedStatus nimbus)))
    ;; call scheduler.schedule to schedule all the topologies
    ;; the new assignments for all the topologies are in the cluster object.
    (.schedule (.getScheduler nimbus) topologies cluster)

    ;;merge with existing statuses
    ;;TODO remove work around for merge to work with java maps
    (.set (.getIdToSchedStatus nimbus) (merge {} (.get (.getIdToSchedStatus nimbus)) (.getStatusMap cluster)))
    (.set (.getNodeIdToResources nimbus) (.getSupervisorsResourcesMap cluster))

    (if-not (conf SCHEDULER-DISPLAY-RESOURCE) 
      (.updateAssignedMemoryForTopologyAndSupervisor cluster topologies))

    ; Remove both of swaps below at first opportunity. This is a hack for non-ras scheduler topology and worker resources
    (.getAndAccumulate (.getIdToResources nimbus) (into {} (map (fn [[k v]] [k (TopologyResources. (nth v 0) (nth v 1) (nth v 2)
                                                    (nth v 3) (nth v 4) (nth v 5))])
                                                      (.getTopologyResourcesMap cluster))) Nimbus/MERGE_ID_TO_RESOURCES)
    ; Remove this also at first chance
    (.getAndAccumulate (.getIdToWorkerResources nimbus)
           (into {} (map (fn [[k v]] [k (map-val #(doto (WorkerResources.)
                                                        (.set_mem_on_heap (nth % 0))
                                                        (.set_mem_off_heap (nth % 1))
                                                        (.set_cpu (nth % 2))) v)])
                         (.getWorkerResourcesMap cluster))) Nimbus/MERGE_ID_TO_WORKER_RESOURCES)

    (.getAssignments cluster)))

(defn- map-diff
  "Returns mappings in m2 that aren't in m1"
  [m1 m2]
  (into {} (filter (fn [[k v]] (not= v (m1 k))) m2)))

(defn get-resources-for-topology [nimbus topo-id]
  (or (get (.get (.getIdToResources nimbus)) topo-id)
      (try
        (let [storm-cluster-state (.getStormClusterState nimbus)
              topology-details (.readTopologyDetails nimbus topo-id)
              assigned-resources (->> (clojurify-assignment (.assignmentInfo storm-cluster-state topo-id nil))
                                      :worker->resources
                                      (vals)
                                        ; Default to [[0 0 0]] if there are no values
                                      (#(or % [[0 0 0]]))
                                        ; [[on-heap, off-heap, cpu]] -> [[on-heap], [off-heap], [cpu]]
                                      (apply map vector)
                                        ; [[on-heap], [off-heap], [cpu]] -> [on-heap-sum, off-heap-sum, cpu-sum]
                                      (map (partial reduce +)))
              worker-resources (TopologyResources. (.getTotalRequestedMemOnHeap topology-details)
                                                    (.getTotalRequestedMemOffHeap topology-details)
                                                    (.getTotalRequestedCpu topology-details)
                                                    (double (nth assigned-resources 0))
                                                    (double (nth assigned-resources 1))
                                                    (double (nth assigned-resources 2)))]
          (.getAndUpdate (.getIdToResources nimbus) (Nimbus$Assoc. topo-id worker-resources))
          worker-resources)
        (catch KeyNotFoundException e
          ; This can happen when a topology is first coming up.
          ; It's thrown by the blobstore code.
          (log-error e "Failed to get topology details")
          (TopologyResources. 0 0 0 0 0 0)))))

(defn- get-worker-resources-for-topology [nimbus topo-id]
  (or (get (.get (.getIdToWorkerResources nimbus)) topo-id)
      (try
        (let [storm-cluster-state (.getStormClusterState nimbus)
              assigned-resources (->> (clojurify-assignment (.assignmentInfo storm-cluster-state topo-id nil))
                                      :worker->resources)
              worker-resources (into {} (map #(identity {(WorkerSlot. (first (key %)) (second (key %)))  
                                                         (doto (WorkerResources.)
                                                             (.set_mem_on_heap (nth (val %) 0))
                                                             (.set_mem_off_heap (nth (val %) 1))
                                                             (.set_cpu (nth (val %) 2)))}) assigned-resources))]
          (.getAndUpdate (.getIdToWorkerResources nimbus) (Nimbus$Assoc. topo-id worker-resources))
          worker-resources))))
          
(defn changed-executors [executor->node+port new-executor->node+port]
  (let [executor->node+port (if executor->node+port (sort executor->node+port) nil)
        new-executor->node+port (if new-executor->node+port (sort new-executor->node+port) nil)
        slot-assigned (clojurify-structure (Utils/reverseMap executor->node+port))
        new-slot-assigned (clojurify-structure (Utils/reverseMap new-executor->node+port))
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


(defn basic-supervisor-details-map [storm-cluster-state]
  (let [infos (.allSupervisorInfo storm-cluster-state)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (.get_hostname info) (.get_scheduler_meta info) nil (.get_resources_map info))]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (if (.isLeader nimbus)
    (let [conf (.getConf nimbus)
        storm-cluster-state (.getStormClusterState nimbus)
        ^INimbus inimbus (.getINimbus nimbus)
        ;; read all the topologies
        topology-ids (.activeStorms storm-cluster-state)
        topologies (into {} (for [tid topology-ids]
                              {tid (.readTopologyDetails nimbus tid)}))
        topologies (Topologies. topologies)
        ;; read all the assignments
        assigned-topology-ids (.assignments storm-cluster-state nil)
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (clojurify-assignment (.assignmentInfo storm-cluster-state tid nil))})))
        thrift-existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (.assignmentInfo storm-cluster-state tid nil)})))
 
        ;; make the new assignments for topologies
        new-scheduler-assignments (compute-new-scheduler-assignments
                                       nimbus
                                       thrift-existing-assignments
                                       existing-assignments
                                       topologies
                                       scratch-topology-id)
        topology->executor->node+port (compute-new-topology->executor->node+port new-scheduler-assignments existing-assignments)

        topology->executor->node+port (merge (into {} (for [id assigned-topology-ids] {id nil})) topology->executor->node+port)
        new-assigned-worker->resources (convert-assignments-to-worker->resources new-scheduler-assignments)
        now-secs (Time/currentTimeSecs)

        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)

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
                                              worker->resources (get new-assigned-worker->resources topology-id)]]
                                   {topology-id (Assignment.
                                                 (conf STORM-LOCAL-DIR)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times
                                                 worker->resources)}))]

    (when (not= new-assignments existing-assignments)
      (log-debug "RESETTING id->resources and id->worker-resources cache!")
      (.set (.getIdToResources nimbus) {})
      (.set (.getIdToWorkerResources nimbus) {}))
    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.setAssignment storm-cluster-state topology-id (thriftify-assignment assignment))
          )))
    (->> new-assignments
          (map (fn [[topology-id assignment]]
            (let [existing-assignment (get existing-assignments topology-id)]
              [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))]
              )))
          (into {})
          (.assignSlots inimbus topologies)))
    (log-message "not a leader, skipping assignments")))

(defn notify-topology-action-listener [nimbus storm-id action]
  (let [topology-action-notifier (.getNimbusTopologyActionNotifier nimbus)]
    (when (not-nil? topology-action-notifier)
      (try (.notify topology-action-notifier storm-id action)
        (catch Exception e
        (log-warn-error e "Ignoring exception from Topology action notifier for storm-Id " storm-id))))))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn- start-storm [nimbus storm-name storm-id topology-initial-status]
  {:pre [(#{TopologyStatus/ACTIVE TopologyStatus/INACTIVE} topology-initial-status)]}
  (let [storm-cluster-state (.getStormClusterState nimbus)
        conf (.getConf nimbus)
        blob-store (.getBlobStore nimbus)
        storm-conf (clojurify-structure (Nimbus/readTopoConf storm-id blob-store))
        topology (StormCommon/systemTopology storm-conf (Nimbus/readStormTopology storm-id blob-store))
        num-executors (->> (clojurify-structure (StormCommon/allComponents topology)) (map-val #(StormCommon/numStartExecutors %)))]
    (log-message "Activating " storm-name ": " storm-id)
    (.activateStorm storm-cluster-state
                      storm-id
      (converter/thriftify-storm-base (StormBase. storm-name
                                  (Time/currentTimeSecs)
                                  {:type topology-initial-status}
                                  (storm-conf TOPOLOGY-WORKERS)
                                  num-executors
                                  (storm-conf TOPOLOGY-SUBMITTER-USER)
                                  nil
                                  nil
                                  {})))
    (notify-topology-action-listener nimbus storm-name "activate")))

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (StormCommon/getStormId storm-cluster-state storm-name)))

(defn check-storm-active! [nimbus storm-name active?]
  (if (= (not active?)
         (storm-active? (.getStormClusterState nimbus)
                        storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))

(defn try-read-storm-conf [conf storm-id blob-store]
  (try-cause
    (clojurify-structure (Nimbus/readTopoConfAsNimbus storm-id blob-store))
    (catch KeyNotFoundException e
       (throw (NotAliveException. (str storm-id))))))

(defn try-read-storm-conf-from-name [conf storm-name nimbus]
  (let [storm-cluster-state (.getStormClusterState nimbus)
        blob-store (.getBlobStore nimbus)
        id (StormCommon/getStormId storm-cluster-state storm-name)]
   (try-read-storm-conf conf id blob-store)))

(defn check-authorization!
  ([nimbus storm-name storm-conf operation context]
     (let [aclHandler (.getAuthorizationHandler nimbus)
           impersonation-authorizer (.getImpersonationAuthorizationHandler nimbus)
           ctx (or context (ReqContext/context))
           check-conf (if storm-conf storm-conf (if storm-name {TOPOLOGY-NAME storm-name}))]
       (if (.isImpersonating ctx)
         (do
          (log-warn "principal: " (.realPrincipal ctx) " is trying to impersonate principal: " (.principal ctx))
          (if impersonation-authorizer
           (when-not (.permit impersonation-authorizer ctx operation check-conf)
             (ThriftAccessLogger/logAccess (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-denied")
             (throw (AuthorizationException. (str "principal " (.realPrincipal ctx) " is not authorized to impersonate
                        principal " (.principal ctx) " from host " (.remoteAddress ctx) " Please see SECURITY.MD to learn
                        how to configure impersonation acls."))))
           (log-warn "impersonation attempt but " NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. potential
                      security risk, please see SECURITY.MD to learn how to configure impersonation authorizer."))))

       (if aclHandler
         (if-not (.permit aclHandler ctx operation check-conf)
           (do
             (ThriftAccessLogger/logAccess (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-denied")
             (throw (AuthorizationException. (str operation (if storm-name (str " on topology " storm-name)) " is not authorized"))))
           (ThriftAccessLogger/logAccess (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation storm-name "access-granted")))))
  ([nimbus storm-name storm-conf operation]
     (check-authorization! nimbus storm-name storm-conf operation (ReqContext/context))))

;; no-throw version of check-authorization!
(defn is-authorized?
  [nimbus conf blob-store operation topology-id]
  (let [topology-conf (try-read-storm-conf conf topology-id blob-store)
        storm-name (topology-conf TOPOLOGY-NAME)]
    (try (check-authorization! nimbus storm-name topology-conf operation)
         true
      (catch AuthorizationException e false))))

(defn code-ids [blob-store]
  (let [to-id (reify KeyFilter
                (filter [this key] (ConfigUtils/getIdFromBlobKey key)))]
    (set (.filterAndListKeys blob-store to-id))))

(defn cleanup-storm-ids [storm-cluster-state blob-store]
  (let [heartbeat-ids (set (.heartbeatStorms storm-cluster-state))
        error-ids (set (.errorTopologies storm-cluster-state))
        code-ids (code-ids blob-store)
        backpressure-ids (set (.backpressureTopologies storm-cluster-state))
        assigned-ids (set (.activeStorms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids backpressure-ids code-ids) assigned-ids)))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (.toString t))
    ))

(defn mapify-serializations [sers]
  (->> sers
       (map (fn [e] (if (map? e) e {e nil})))
       (apply merge)
       ))

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (merge storm-conf (clojurify-structure (StormCommon/componentConf component)))
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (StormCommon/numStartExecutors component))
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

(defn normalize-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)]
    (doseq [[_ component] (clojurify-structure (StormCommon/allComponents ret))]
      (.set_json_conf
        (.get_common component)
        (->> {TOPOLOGY-TASKS (component-parallelism storm-conf component)}
             (merge (clojurify-structure (StormCommon/componentConf component)))
             JSONValue/toJSONString)))
    ret ))

(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                         #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                              .get_json_conf
                              ((fn [c] (if c (JSONValue/parse c))))
                              clojurify-structure)
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
            TOPOLOGY-EVENTLOGGER-EXECUTORS (total-conf TOPOLOGY-EVENTLOGGER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

(defn blob-rm-key [blob-store key storm-cluster-state]
  (try
    (.deleteBlob blob-store key Nimbus/NIMBUS_SUBJECT)
    (if (instance? LocalFsBlobStore blob-store)
      (.removeBlobstoreKey storm-cluster-state key))
    (catch Exception e
      (log-message "Exception" e))))

(defn blob-rm-dependency-jars-in-topology [id blob-store storm-cluster-state]
  (try
    (let [storm-topology (Nimbus/readStormTopologyAsNimbus id blob-store)
          dependency-jars (.get_dependency_jars ^StormTopology storm-topology)]
      (log-message "Removing dependency jars from blobs - " dependency-jars)
      (when-not (empty? dependency-jars)
        (doseq [key dependency-jars]
          (blob-rm-key blob-store key storm-cluster-state))))
    (catch Exception e
      (log-message "Exception" e))))

(defn blob-rm-topology-keys [id blob-store storm-cluster-state]
  (blob-rm-key blob-store (ConfigUtils/masterStormJarKey id) storm-cluster-state)
  (blob-rm-key blob-store (ConfigUtils/masterStormConfKey id) storm-cluster-state)
  (blob-rm-key blob-store (ConfigUtils/masterStormCodeKey id) storm-cluster-state))

(defn force-delete-topo-dist-dir [conf id]
  (Utils/forceDelete (ConfigUtils/masterStormDistRoot conf id)))

(defn do-cleanup [nimbus]
  (if (.isLeader nimbus)
    (let [storm-cluster-state (.getStormClusterState nimbus)
          conf (.getConf nimbus)
          submit-lock (.getSubmitLock nimbus)
          blob-store (.getBlobStore nimbus)]
      (let [to-cleanup-ids (locking submit-lock
                             (cleanup-storm-ids storm-cluster-state blob-store))]
        (when-not (empty? to-cleanup-ids)
          (doseq [id to-cleanup-ids]
            (log-message "Cleaning up " id)
            (.teardownHeartbeats storm-cluster-state id)
            (.teardownTopologyErrors storm-cluster-state id)
            (.removeBackpressure storm-cluster-state id)
            (blob-rm-dependency-jars-in-topology id blob-store storm-cluster-state)
            (force-delete-topo-dist-dir conf id)
            (blob-rm-topology-keys id blob-store storm-cluster-state)
            (.getAndUpdate (.getHeartbeatsCache nimbus) (Nimbus$Dissoc. id))))))

    (log-message "not a leader, skipping cleanup")))

(defn- file-older-than? [now seconds file]
  (<= (+ (.lastModified file) (Time/secsToMillis seconds)) (Time/secsToMillis now)))

(defn clean-inbox [dir-location seconds]
  "Deletes jar files in dir older than seconds."
  (let [now (Time/currentTimeSecs)
        pred #(and (.isFile %) (file-older-than? now seconds %))
        files (filter pred (file-seq (File. dir-location)))]
    (doseq [f files]
      (if (.delete f)
        (log-message "Cleaning inbox ... deleted: " (.getName f))
        ;; This should never happen
        (log-error "Cleaning inbox ... error deleting: " (.getName f))))))

(defn clean-topology-history
  "Deletes topologies from history older than minutes."
  [mins nimbus]
  (locking (.getTopologyHistoryLock nimbus)
    (let [cutoff-age (- (Time/currentTimeSecs) (* mins 60))
          topo-history-state (.getTopologyHistoryState nimbus)]
          (.filterOldTopologies ^LocalState topo-history-state cutoff-age))))

(defn setup-blobstore [nimbus]
  "Sets up blobstore state for all current keys."
  (let [storm-cluster-state (.getStormClusterState nimbus)
        blob-store (.getBlobStore nimbus)
        local-set-of-keys (set (get-key-seq-from-blob-store blob-store))
        all-keys (set (.activeKeys storm-cluster-state))
        locally-available-active-keys (set/intersection local-set-of-keys all-keys)
        keys-to-delete (set/difference local-set-of-keys all-keys)
        conf (.getConf nimbus)
        nimbus-host-port-info (.getNimbusHostPortInfo nimbus)]
    (log-debug "Deleting keys not on the zookeeper" keys-to-delete)
    (doseq [key keys-to-delete]
      (.deleteBlob blob-store key Nimbus/NIMBUS_SUBJECT))
    (log-debug "Creating list of key entries for blobstore inside zookeeper" all-keys "local" locally-available-active-keys)
    (doseq [key locally-available-active-keys]
      (.setupBlobstore storm-cluster-state key (.getNimbusHostPortInfo nimbus) (Nimbus/getVerionForKey key nimbus-host-port-info conf)))))

(defn- get-errors [storm-cluster-state storm-id component-id]
  (->> (map clojurify-error (.errors storm-cluster-state storm-id component-id))
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

;; We will only file at <Storm dist root>/<Topology ID>/<File>
;; to be accessed via Thrift
;; ex., storm-local/nimbus/stormdist/aa-1-1377104853/stormjar.jar
(defn check-file-access [conf file-path]
  (log-debug "check file access:" file-path)
  (try
    (if (not= (.getCanonicalFile (File. (ConfigUtils/masterStormDistRoot conf)))
          (-> (File. file-path) .getCanonicalFile .getParentFile .getParentFile))
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))
    (catch Exception e
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))))

(defn try-read-storm-conf-from-name
  [conf storm-name nimbus]
  (let [storm-cluster-state (.getStormClusterState nimbus)
        blob-store (.getBlobStore nimbus)
        id (StormCommon/getStormId storm-cluster-state storm-name)]
    (try-read-storm-conf conf id blob-store)))

(defn try-read-storm-topology
  [storm-id blob-store]
  (try-cause
    (Nimbus/readStormTopologyAsNimbus storm-id blob-store)
    (catch KeyNotFoundException e
      (throw (NotAliveException. (str storm-id))))))

(defn add-topology-to-history-log
  [storm-id nimbus topology-conf]
  (log-message "Adding topo to history log: " storm-id)
  (locking (.getTopologyHistoryLock nimbus)
    (let [topo-history-state (.getTopologyHistoryState nimbus)
          users (ConfigUtils/getTopoLogsUsers topology-conf)
          groups (ConfigUtils/getTopoLogsGroups topology-conf)]
      (.addTopologyHistory ^LocalState topo-history-state
                           (LSTopoHistory. storm-id (Time/currentTimeSecs) users groups)))))

(defn igroup-mapper
  [storm-conf]
  (AuthUtils/GetGroupMappingServiceProviderPlugin storm-conf))

(defn user-groups
  [user storm-conf]
  (if (clojure.string/blank? user) [] (.getGroups (igroup-mapper storm-conf) user)))

(defn does-users-group-intersect?
  "Check to see if any of the users groups intersect with the list of groups passed in"
  [user groups-to-check storm-conf]
  (let [groups (user-groups user storm-conf)]
    (> (.size (set/intersection (set groups) (set groups-to-check))) 0)))

(defn ->topo-history
  [thrift-topo-hist]
  {
   :topoid (.get_topology_id thrift-topo-hist)
   :timestamp (.get_time_stamp thrift-topo-hist)
   :users (.get_users thrift-topo-hist)
   :groups (.get_groups thrift-topo-hist)})

(defn read-topology-history
  [nimbus user admin-users]
  (let [topo-history-state (.getTopologyHistoryState nimbus)
        curr-history (vec (map ->topo-history (.getTopoHistoryList ^LocalState topo-history-state)))
        topo-user-can-access (fn [line user storm-conf]
                               (if (nil? user)
                                 (line :topoid)
                                 (if (or (some #(= % user) admin-users)
                                       (does-users-group-intersect? user (line :groups) storm-conf)
                                       (some #(= % user) (line :users)))
                                   (line :topoid)
                                   nil)))]
    (remove nil? (map #(topo-user-can-access % user (.getConf nimbus)) curr-history))))

(defn renew-credentials [nimbus]
  (if (.isLeader nimbus)
    (let [storm-cluster-state (.getStormClusterState nimbus)
          blob-store (.getBlobStore nimbus)
          renewers (.getCredRenewers nimbus)
          update-lock (.getCredUpdateLock nimbus)
          assigned-ids (set (.activeStorms storm-cluster-state))]
      (when-not (empty? assigned-ids)
        (doseq [id assigned-ids]
          (locking update-lock
            (let [orig-creds (clojurify-crdentials (.credentials storm-cluster-state id nil))
                  topology-conf (try-read-storm-conf (.getConf nimbus) id blob-store)]
              (if orig-creds
                (let [new-creds (HashMap. orig-creds)]
                  (doseq [renewer renewers]
                    (log-message "Renewing Creds For " id " with " renewer)
                    (.renew renewer new-creds (Collections/unmodifiableMap topology-conf)))
                  (when-not (= orig-creds new-creds)
                    (.setCredentials storm-cluster-state id (thriftify-credentials new-creds) topology-conf)
                    ))))))))
    (log-message "not a leader, skipping credential renewal.")))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn validate-topology-size [topo-conf nimbus-conf topology]
  (let [workers-count (get topo-conf TOPOLOGY-WORKERS)
        workers-allowed (get nimbus-conf NIMBUS-SLOTS-PER-TOPOLOGY)
        num-executors (->> (StormCommon/allComponents topology) clojurify-structure (map-val #(StormCommon/numStartExecutors %)))
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

(defn nimbus-topology-bases [storm-cluster-state]
  (map-val #(clojurify-storm-base %) (clojurify-structure
                                        (StormCommon/topologyBases storm-cluster-state))))

(defn- set-logger-timeouts [log-config]
  (let [timeout-secs (.get_reset_log_level_timeout_secs log-config)
       timeout (time/plus (time/now) (time/secs timeout-secs))]
   (if (time/after? timeout (time/now))
     (.set_reset_log_level_timeout_epoch log-config (coerce/to-long timeout))
     (.unset_reset_log_level_timeout_epoch log-config))))

(defmethod blob-sync :distributed [conf nimbus]
  (if (not (.isLeader nimbus))
    (let [storm-cluster-state (.getStormClusterState nimbus)
          nimbus-host-port-info (.getNimbusHostPortInfo nimbus)
          blob-store-key-set (set (get-key-seq-from-blob-store (.getBlobStore nimbus)))
          zk-key-set (set (.blobstore storm-cluster-state (fn [] (blob-sync conf nimbus))))]
      (log-debug "blob-sync " "blob-store-keys " blob-store-key-set "zookeeper-keys " zk-key-set)
      (let [sync-blobs (doto
                          (BlobSynchronizer. (.getBlobStore nimbus) conf)
                          (.setNimbusInfo nimbus-host-port-info)
                          (.setBlobStoreKeySet blob-store-key-set)
                          (.setZookeeperKeySet zk-key-set))]
        (.syncBlobs sync-blobs)))))

(defmethod blob-sync :local [conf nimbus]
  nil)

(defn make-supervisor-summary 
  [nimbus id info]
    (log-message "INFO: " info " ID: " id)
    (let [ports (set (.get_meta info)) ;;TODO: this is only true for standalone
          _ (log-message "PORTS: " ports)
          sup-sum (SupervisorSummary. (.get_hostname info)
                                      (.get_uptime_secs info)
                                      (count ports)
                                      (count (.get_used_ports info))
                                      id)]
      (.set_total_resources sup-sum (map-val double (.get_resources_map info)))
      (when-let [[total-mem total-cpu used-mem used-cpu] (.get (.get (.getNodeIdToResources nimbus)) id)]
        (.set_used_mem sup-sum (Utils/nullToZero used-mem))
        (.set_used_cpu sup-sum (Utils/nullToZero used-cpu)))
      (when-let [version (.get_version info)] (.set_version sup-sum version))
      sup-sum))

(defn user-and-supervisor-topos
  [nimbus conf blob-store assignments supervisor-id]
  (let [topo-id->supervisors 
          (into {} (for [[topo-id java-assignment] assignments] 
                     (let [assignment (clojurify-assignment java-assignment)]
                       {topo-id (into #{} 
                                      (map #(first (second %)) 
                                           (:executor->node+port assignment)))})))
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
               {tid (.assignmentInfo storm-cluster-state tid nil)}))))

(defn get-launch-time-secs 
  [base storm-id]
  (if base (:launch-time-secs base)
    (throw
      (NotAliveException. (str storm-id)))))

(defn get-cluster-info [nimbus]
  (let [storm-cluster-state (.getStormClusterState nimbus)
        supervisor-infos (.allSupervisorInfo storm-cluster-state)
        ;; TODO: need to get the port info about supervisors...
        ;; in standalone just look at metadata, otherwise just say N/A?
        supervisor-summaries (dofor [[id info] supervisor-infos]
                                    (make-supervisor-summary nimbus id info))
        nimbus-uptime (. (.getUptime nimbus) upTime)
        bases (nimbus-topology-bases storm-cluster-state)
        nimbuses (.nimbuses storm-cluster-state)

        ;;update the isLeader field for each nimbus summary
        _ (let [leader (.getLeader (.getLeaderElector nimbus))
                leader-host (.getHost leader)
                leader-port (.getPort leader)]
            (doseq [nimbus-summary nimbuses]
              (.set_uptime_secs nimbus-summary (Time/deltaSecs (.get_uptime_secs nimbus-summary)))
              (.set_isLeader nimbus-summary (and (= leader-host (.get_host nimbus-summary)) (= leader-port (.get_port nimbus-summary))))))

        topology-summaries (dofor [[id base] bases :when base]
                                  (let [assignment (clojurify-assignment (.assignmentInfo storm-cluster-state id nil))
                                        topo-summ (TopologySummary. id
                                                                    (:storm-name base)
                                                                    (->> (:executor->node+port assignment)
                                                                         keys
                                                                         (mapcat #(clojurify-structure (StormCommon/executorIdToTasks %)))
                                                                         count)
                                                                    (->> (:executor->node+port assignment)
                                                                         keys
                                                                         count)
                                                                    (->> (:executor->node+port assignment)
                                                                         vals
                                                                         set
                                                                         count)
                                                                    (Time/deltaSecs (:launch-time-secs base))
                                                                    (extract-status-str base))]
                                    (when-let [owner (:owner base)] (.set_owner topo-summ owner))
                                    (when-let [sched-status (.get (.get (.getIdToSchedStatus nimbus)) id)] (.set_sched_status topo-summ sched-status))
                                    (when-let [resources (get-resources-for-topology nimbus id)]
                                      (.set_requested_memonheap topo-summ (.getRequestedMemOnHeap resources))
                                      (.set_requested_memoffheap topo-summ (.getRequestedMemOffHeap resources))
                                      (.set_requested_cpu topo-summ (.getRequestedCpu resources))
                                      (.set_assigned_memonheap topo-summ (.getAssignedMemOnHeap resources))
                                      (.set_assigned_memoffheap topo-summ (.getAssignedMemOffHeap resources))
                                      (.set_assigned_cpu topo-summ (.getAssignedCpu resources)))
                                    (.set_replication_count topo-summ (.getBlobReplicationCount nimbus (ConfigUtils/masterStormCodeKey id)))
                                    topo-summ))
        ret (ClusterSummary. supervisor-summaries
                             topology-summaries
                             nimbuses)
        _ (.set_nimbus_uptime_secs ret nimbus-uptime)]
    ret))

(defn- between?
  "val >= lower and val <= upper"
  [val lower upper]
  (and (>= val lower)
    (<= val upper)))

(defn extract-cluster-metrics [^ClusterSummary summ]
  (let [cluster-summ (ui/cluster-summary summ "nimbus")]
    {:cluster-info (IClusterMetricsConsumer$ClusterInfo. (long (Time/currentTimeSecs)))
     :data-points  (map
                     (fn [[k v]] (DataPoint. k v))
                     (select-keys cluster-summ ["supervisors" "topologies" "slotsTotal" "slotsUsed" "slotsFree"
                                                "executorsTotal" "tasksTotal"]))}))
(defn extract-supervisors-metrics [^ClusterSummary summ]
  (let [sups (.get_supervisors summ)
        supervisors-summ ((ui/supervisor-summary sups) "supervisors")]
    (map (fn [supervisor-summ]
           {:supervisor-info (IClusterMetricsConsumer$SupervisorInfo.
                               (supervisor-summ "host")
                               (supervisor-summ "id")
                               (long (Time/currentTimeSecs)))
            :data-points     (map
                               (fn [[k v]] (DataPoint. k v))
                               (select-keys supervisor-summ ["slotsTotal" "slotsUsed" "totalMem" "totalCpu"
                                                             "usedMem" "usedCpu"]))})
         supervisors-summ)))

(defn send-cluster-metrics-to-executors [nimbus]
  (let [cluster-summary (get-cluster-info nimbus)
        cluster-metrics (extract-cluster-metrics cluster-summary)
        supervisors-metrics (extract-supervisors-metrics cluster-summary)]
    (dofor
      [consumer-executor (.getClusterConsumerExecutors nimbus)]
      (do
        (.handleDataPoints consumer-executor (:cluster-info cluster-metrics) (:data-points cluster-metrics))
        (dofor
          [supervisor-metrics supervisors-metrics]
          (do
            (.handleDataPoints consumer-executor (:supervisor-info supervisor-metrics) (:data-points supervisor-metrics))))))))

(defn mk-reified-nimbus [nimbus conf blob-store]
  (let [principal-to-local (AuthUtils/GetPrincipalToLocalPlugin conf)
        admin-users (or (.get conf NIMBUS-ADMINS) [])
        get-common-topo-info
          (fn [^String storm-id operation]
            (let [storm-cluster-state (.getStormClusterState nimbus)
                  topology-conf (try-read-storm-conf conf storm-id blob-store)
                  storm-name (topology-conf TOPOLOGY-NAME)
                  _ (check-authorization! nimbus
                                          storm-name
                                          topology-conf
                                          operation)
                  topology (try-read-storm-topology storm-id blob-store)
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
        (try
          (.mark Nimbus/submitTopologyWithOptsCalls)
          (.assertIsLeader nimbus)
          (assert (not-nil? submitOptions))
          (validate-topology-name! storm-name)
          (check-authorization! nimbus storm-name nil "submitTopology")
          (check-storm-active! nimbus storm-name false)
          (let [topo-conf (if-let [parsed-json (JSONValue/parse serializedConf)]
                            (clojurify-structure parsed-json))]
            (try
              (ConfigValidation/validateFields topo-conf)
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^org.apache.storm.nimbus.ITopologyValidator (.getValidator nimbus)
                       storm-name
                       topo-conf
                       topology))
          (.getAndIncrement (.getSubmittedCount nimbus))
          (let [storm-id (str storm-name "-" (.get (.getSubmittedCount nimbus)) "-" (Time/currentTimeSecs))
                credentials (.get_creds submitOptions)
                credentials (when credentials (.get_creds credentials))
                topo-conf (if-let [parsed-json (JSONValue/parse serializedConf)]
                            (clojurify-structure parsed-json))
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
                system-user (System/getProperty "user.name")
                topo-acl (distinct (remove nil? (conj (.get storm-conf-submitted TOPOLOGY-USERS) submitter-principal, submitter-user)))
                storm-conf (-> storm-conf-submitted
                               (assoc TOPOLOGY-SUBMITTER-PRINCIPAL (if submitter-principal submitter-principal ""))
                               (assoc TOPOLOGY-SUBMITTER-USER (if submitter-user submitter-user system-user)) ;Don't let the user set who we launch as
                               (assoc TOPOLOGY-USERS topo-acl)
                               (assoc STORM-ZOOKEEPER-SUPERACL (.get conf STORM-ZOOKEEPER-SUPERACL)))
                storm-conf (if (Utils/isZkAuthenticationConfiguredStormServer conf)
                                storm-conf
                                (dissoc storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
                storm-conf (if (conf STORM-TOPOLOGY-CLASSPATH-BEGINNING-ENABLED)
                             storm-conf
                             (dissoc storm-conf TOPOLOGY-CLASSPATH-BEGINNING))
                total-storm-conf (merge conf storm-conf)
                topology (normalize-topology total-storm-conf topology)
                storm-cluster-state (.getStormClusterState nimbus)]
            (when credentials (doseq [nimbus-autocred-plugin (.getNimbusAutocredPlugins nimbus)]
              (.populateCredentials nimbus-autocred-plugin credentials (Collections/unmodifiableMap storm-conf))))
            (if (and (conf SUPERVISOR-RUN-WORKER-AS-USER) (or (nil? submitter-user) (.isEmpty (.trim submitter-user))))
              (throw (AuthorizationException. "Could not determine the user to run this topology as.")))
            (StormCommon/systemTopology total-storm-conf topology) ;; this validates the structure of the topology
            (validate-topology-size topo-conf conf topology)
            (when (and (Utils/isZkAuthenticationConfiguredStormServer conf)
                       (not (Utils/isZkAuthenticationConfiguredTopology storm-conf)))
                (throw (IllegalArgumentException. "The cluster is configured for zookeeper authentication, but no payload was provided.")))
            (log-message "Received topology submission for "
                         storm-name
                         " with conf "
                         (Utils/redactValue storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (.getSubmitLock nimbus)
              (check-storm-active! nimbus storm-name false)
              ;;cred-update-lock is not needed here because creds are being added for the first time.
              (.setCredentials storm-cluster-state storm-id (thriftify-credentials credentials) storm-conf)
              (log-message "uploadedJar " uploadedJarLocation)
              (.setupStormCode nimbus conf storm-id uploadedJarLocation total-storm-conf topology)
              (.waitForDesiredCodeReplication nimbus total-storm-conf storm-id)
              (.setupHeatbeats storm-cluster-state storm-id)
              (if (total-storm-conf TOPOLOGY-BACKPRESSURE-ENABLE)
                (.setupBackpressure storm-cluster-state storm-id))
              (notify-topology-action-listener nimbus storm-name "submitTopology")
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE TopologyStatus/INACTIVE
                                              TopologyInitialStatus/ACTIVE TopologyStatus/ACTIVE}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions))))))
          (catch Throwable e
            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))

      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (.mark Nimbus/submitTopologyCalls)
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))

      (^void killTopology [this ^String name]
        (.mark Nimbus/killTopologyCalls)
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (.mark Nimbus/killTopologyWithOptsCalls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              storm-id (topology-conf STORM-ID)
              operation "killTopology"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (let [wait-amt (if (.is_set_wait_secs options)
                           (.get_wait_secs options)
                           )]
            (.transitionName nimbus storm-name TopologyActions/KILL wait-amt true)
            (notify-topology-action-listener nimbus storm-name operation))
          (add-topology-to-history-log (StormCommon/getStormId (.getStormClusterState nimbus) storm-name)
            nimbus topology-conf)))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (.mark Nimbus/rebalanceCalls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "rebalance"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (let [executor-overrides (if (.is_set_num_executors options)
                                     (.get_num_executors options)
                                     {})]
            (doseq [[c num-executors] executor-overrides]
              (when (<= num-executors 0)
                (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
                ))
            (.transitionName nimbus storm-name TopologyActions/REBALANCE options true)

            (notify-topology-action-listener nimbus storm-name operation))))

      (activate [this storm-name]
        (.mark Nimbus/activateCalls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "activate"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (.transitionName nimbus storm-name TopologyActions/ACTIVATE nil true)
          (notify-topology-action-listener nimbus storm-name operation)))

      (deactivate [this storm-name]
        (.mark Nimbus/deactivateCalls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "deactivate"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (.transitionName nimbus storm-name TopologyActions/INACTIVATE nil true)
          (notify-topology-action-listener nimbus storm-name operation)))

      (debug [this storm-name component-id enable? samplingPct]
        (.mark Nimbus/debugCalls)
        (let [storm-cluster-state (.getStormClusterState nimbus)
              storm-id (StormCommon/getStormId storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id blob-store)
              ;; make sure samplingPct is within bounds.
              spct (Math/max (Math/min samplingPct 100.0) 0.0)
              ;; while disabling we retain the sampling pct.
              debug-options (if enable? {:enable enable? :samplingpct spct} {:enable enable?})
              storm-base-updates (assoc {} :component->debug (if (empty? component-id)
                                                               {storm-id debug-options}
                                                               {component-id debug-options}))]
          (check-authorization! nimbus storm-name topology-conf "debug")
          (when-not storm-id
            (throw (NotAliveException. storm-name)))
          (log-message "Nimbus setting debug to " enable? " for storm-name '" storm-name "' storm-id '" storm-id "' sampling pct '" spct "'"
            (if (not (clojure.string/blank? component-id)) (str " component-id '" component-id "'")))
          (locking (.getSubmitLock nimbus)
            (.updateStorm storm-cluster-state storm-id  (converter/thriftify-storm-base storm-base-updates)))))

      (^void setWorkerProfiler
        [this ^String id ^ProfileRequest profileRequest]
        (.mark Nimbus/setWorkerProfilerCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setWorkerProfiler")
              storm-cluster-state (.getStormClusterState nimbus)]
          (.setWorkerProfileRequest storm-cluster-state id profileRequest)))

      (^List getComponentPendingProfileActions
        [this ^String id ^String component_id ^ProfileAction action]
        (.mark Nimbus/getComponentPendingProfileActionsCalls)
        (let [info (get-common-topo-info id "getComponentPendingProfileActions")
              storm-cluster-state (:storm-cluster-state info)
              task->component (:task->component info)
              {:keys [executor->node+port node->host]} (:assignment info)
              ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                    executor->node+port)
              nodeinfos (clojurify-structure (StatsUtil/extractNodeInfosFromHbForComp executor->host+port task->component false component_id))
              all-pending-actions-for-topology (clojurify-profile-request (.getTopologyProfileRequests storm-cluster-state id))
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
        (.mark Nimbus/setLogConfigCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setLogConfig")
              storm-cluster-state (.getStormClusterState nimbus)
              merged-log-config (or (.topologyLogConfig storm-cluster-state id nil) (LogConfig.))
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
            (.setTopologyLogConfig storm-cluster-state id merged-log-config)))

      (uploadNewCredentials [this storm-name credentials]
        (.mark Nimbus/uploadNewCredentialsCalls)
        (let [storm-cluster-state (.getStormClusterState nimbus)
              storm-id (StormCommon/getStormId storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id blob-store)
              creds (when credentials (.get_creds credentials))]
          (check-authorization! nimbus storm-name topology-conf "uploadNewCredentials")
          (locking (.getCredUpdateLock nimbus) (.setCredentials storm-cluster-state storm-id (thriftify-credentials creds) topology-conf))))

      (beginFileUpload [this]
        (.mark Nimbus/beginFileUploadCalls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [fileloc (str (.getInbox nimbus) "/stormjar-" (Utils/uuid) ".jar")]
          (.put (.getUploaders nimbus)
                fileloc
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (.mark Nimbus/uploadChunkCalls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (.getUploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.write channel chunk)
          (.put uploaders location channel)
          ))

      (^void finishFileUpload [this ^String location]
        (.mark Nimbus/finishFileUploadCalls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (.getUploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.close channel)
          (log-message "Finished uploading file from client: " location)
          (.remove uploaders location)
          ))

      (^String beginFileDownload
        [this ^String file]
        (.mark Nimbus/beginFileDownloadCalls)
        (check-authorization! nimbus nil nil "fileDownload")
        (let [is (BufferInputStream. (.getBlob (.getBlobStore nimbus) file nil)
              ^Integer (Utils/getInt (conf STORM-BLOBSTORE-INPUTSTREAM-BUFFER-SIZE-BYTES)
              (int 65536)))
              id (Utils/uuid)]
          (.put (.getDownloaders nimbus) id is)
          id))

      (^ByteBuffer downloadChunk [this ^String id]
        (.mark Nimbus/downloadChunkCalls)
        (check-authorization! nimbus nil nil "fileDownload")
        (let [downloaders (.getDownloaders nimbus)
              ^BufferInputStream is (.get downloaders id)]
          (when-not is
            (throw (RuntimeException.
                    "Could not find input stream for id " id)))
          (let [ret (.read is)]
            (.put downloaders id is)
            (when (empty? ret)
              (.close is)
              (.remove downloaders id))
            (ByteBuffer/wrap ret))))

      (^String getNimbusConf [this]
        (.mark Nimbus/getNimbusConfCalls)
        (check-authorization! nimbus nil nil "getNimbusConf")
        (JSONValue/toJSONString (.getConf nimbus)))

      (^LogConfig getLogConfig [this ^String id]
        (.mark Nimbus/getLogConfigCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "getLogConfig")
             storm-cluster-state (.getStormClusterState nimbus)
             log-config (.topologyLogConfig storm-cluster-state id nil)]
           (if log-config log-config (LogConfig.))))

      (^String getTopologyConf [this ^String id]
        (.mark Nimbus/getTopologyConfCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopologyConf")
              (JSONValue/toJSONString topology-conf)))

      (^StormTopology getTopology [this ^String id]
        (.mark Nimbus/getTopologyCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopology")
              (StormCommon/systemTopology topology-conf (try-read-storm-topology id (.getBlobStore nimbus)))))

      (^StormTopology getUserTopology [this ^String id]
        (.mark Nimbus/getUserTopologyCalls)
        (let [topology-conf (try-read-storm-conf conf id (.getBlobStore nimbus))
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getUserTopology")
              (try-read-storm-topology id blob-store)))

      (^ClusterSummary getClusterInfo [this]
        (.mark Nimbus/getClusterInfoCalls)
        (check-authorization! nimbus nil nil "getClusterInfo")
        (get-cluster-info nimbus))

      (^TopologyInfo getTopologyInfoWithOpts [this ^String storm-id ^GetInfoOptions options]
        (.mark Nimbus/getTopologyInfoWithOptsCalls)
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
                                                    get-last-error)
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
                                            heartbeat (.get beats (StatsUtil/convertExecutor executor))
                                            heartbeat (or heartbeat {})
                                            hb (.get heartbeat "heartbeat")
                                            excutorstats (if hb (.get hb "stats"))
                                            excutorstats (if excutorstats
                                                    (StatsUtil/thriftifyExecutorStats excutorstats))]
                                          (doto
                                              (ExecutorSummary. (thriftify-executor-id executor)
                                                                (-> executor first task->component)
                                                                host
                                                                port
                                                                (Utils/nullToZero (.get heartbeat "uptime")))
                                            (.set_stats excutorstats))
                                          ))
              topo-info  (TopologyInfo. storm-id
                           storm-name
                           (Time/deltaSecs launch-time-secs)
                           executor-summaries
                           (extract-status-str base)
                           errors
                           )]
            (when-let [owner (:owner base)] (.set_owner topo-info owner))
            (when-let [sched-status (.get (.get (.getIdToSchedStatus nimbus)) storm-id)] (.set_sched_status topo-info sched-status))
            (when-let [resources (get-resources-for-topology nimbus storm-id)]
              (.set_requested_memonheap topo-info (.getRequestedMemOnHeap resources))
              (.set_requested_memoffheap topo-info (.getRequestedMemOffHeap resources))
              (.set_requested_cpu topo-info (.getRequestedCpu resources))
              (.set_assigned_memonheap topo-info (.getAssignedMemOnHeap resources))
              (.set_assigned_memoffheap topo-info (.getAssignedMemOffHeap resources))
              (.set_assigned_cpu topo-info (.getAssignedCpu resources)))
            (when-let [component->debug (:component->debug base)]
              ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
              (.set_component_debug topo-info (map-val converter/thriftify-debugoptions component->debug)))
            (.set_replication_count topo-info (.getBlobReplicationCount nimbus (ConfigUtils/masterStormCodeKey storm-id)))
          topo-info))

      (^TopologyInfo getTopologyInfo [this ^String topology-id]
        (.mark Nimbus/getTopologyInfoCalls)
        (.getTopologyInfoWithOpts this
                                  topology-id
                                  (doto (GetInfoOptions.) (.set_num_err_choice NumErrorsChoice/ALL))))


      (^String beginCreateBlob [this
                                ^String blob-key
                                ^SettableBlobMeta blob-meta]
        (let [session-id (Utils/uuid)]
          (.put (.getBlobUploaders nimbus)
            session-id
            (.createBlob (.getBlobStore nimbus) blob-key blob-meta (Nimbus/getSubject)))
          (log-message "Created blob for " blob-key
            " with session id " session-id)
          (str session-id)))

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
              exec->node+port (:executor->node+port assignment)
              node->host (:node->host assignment)
              worker->resources (get-worker-resources-for-topology nimbus topo-id)
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
          (when-let [resources (get-resources-for-topology nimbus topo-id)]
            (.set_requested_memonheap topo-page-info (:requested-mem-on-heap resources))
            (.set_requested_memoffheap topo-page-info (:requested-mem-off-heap resources))
            (.set_requested_cpu topo-page-info (:requested-cpu resources))
            (.set_assigned_memonheap topo-page-info (:assigned-mem-on-heap resources))
            (.set_assigned_memoffheap topo-page-info (:assigned-mem-off-heap resources))
            (.set_assigned_cpu topo-page-info (:assigned-cpu resources)))
          (doto topo-page-info
            (.set_name storm-name)
            (.set_status (extract-status-str base))
            (.set_uptime_secs (Time/deltaSecs launch-time-secs))
            (.set_topology_conf (JSONValue/toJSONString
                                  (try-read-storm-conf conf
                                                       topo-id
                                                       (.getBlobStore nimbus))))
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
                            {:keys [storm-name
                                    assignment
                                    beats
                                    task->component]} topo-info
                            exec->node+port (:executor->node+port assignment)
                            node->host (:node->host assignment)
                            worker->resources (get-worker-resources-for-topology nimbus storm-id)]
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
            (.set_errors (get-errors (:storm-cluster-state info)
                                     topo-id
                                     component-id))
            (.set_topology_status (extract-status-str (:base info))))
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
                                    (let [topology-conf (try-read-storm-conf conf topo-id (.getBlobStore nimbus))
                                          groups (ConfigUtils/getTopoLogsGroups topology-conf)]
                                      (or (nil? user)
                                          (some #(= % user) admin-users)
                                          (does-users-group-intersect? user groups conf)
                                          (some #(= % user) (ConfigUtils/getTopoLogsUsers topology-conf)))))
              active-ids-for-user (filter #(user-group-match-fn % user (.getConf nimbus)) assigned-topology-ids)
              topo-history-list (read-topology-history nimbus user admin-users)]
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

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defserverfn service-handler [conf inimbus blob-store leader-elector cluster-state]
  (.prepare inimbus conf (ConfigUtils/masterInimbusDir conf))
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (Nimbus. conf inimbus cluster-state nil blob-store leader-elector)
        blob-store (.getBlobStore nimbus)]
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
      (.blobstore (.getStormClusterState nimbus) (fn [] (blob-sync conf nimbus)))
      (setup-blobstore nimbus))

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
            (mk-assignments nimbus)))
        (do-cleanup nimbus)))
    ;; Schedule Nimbus inbox cleaner
    (.scheduleRecurring (.getTimer nimbus)
      0
      (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
      (fn [] (clean-inbox (.getInbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))))
    ;; Schedule nimbus code sync thread to sync code from other nimbuses.
    (if (instance? LocalFsBlobStore blob-store)
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf NIMBUS-CODE-SYNC-FREQ-SECS)
        (fn [] (blob-sync conf nimbus))))
    ;; Schedule topology history cleaner
    (when-let [interval (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)
        (fn [] (clean-topology-history (conf LOGVIEWER-CLEANUP-AGE-MINS) nimbus))))
    (.scheduleRecurring (.getTimer nimbus)
      0
      (conf NIMBUS-CREDENTIAL-RENEW-FREQ-SECS)
      (fn []
        (renew-credentials nimbus)))

    (def nimbus:num-supervisors (StormMetricsRegistry/registerGauge "nimbus:num-supervisors"
      (fn [] (.size (.supervisors (.getStormClusterState nimbus) nil)))))

    (StormMetricsRegistry/startMetricsReporters conf)

    (if (.getClusterConsumerExecutors nimbus)
      (.scheduleRecurring (.getTimer nimbus)
        0
        (conf STORM-CLUSTER-METRICS-CONSUMER-PUBLISH-INTERVAL-SECS)
        (fn []
          (when (.isLeader nimbus)
            (send-cluster-metrics-to-executors nimbus)))))

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
  (let [service-handler (service-handler conf nimbus nil nil)
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
