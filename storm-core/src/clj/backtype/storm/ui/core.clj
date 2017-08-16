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

(ns backtype.storm.ui.core
  (:use compojure.core)
  (:use [clojure.java.shell :only [sh]])
  (:use ring.middleware.reload
        ring.middleware.multipart-params
        ring.middleware.multipart-params.temp-file)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log converter stats])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID ACKER-INIT-STREAM-ID ACKER-ACK-STREAM-ID
                                              ACKER-FAIL-STREAM-ID mk-authorization-handler
                                              start-metrics-reporters]]])
  (:use [clojure.string :only [blank? lower-case trim split]])
  (:use [clojure.set :only [intersection]])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions GetInfoOptions NumErrorsChoice TopologyPageInfo
            SupervisorPageInfo TopologyStats CommonAggregateStats ComponentAggregateStats
            ComponentType BoltAggregateStats SpoutAggregateStats
            ExecutorAggregateStats SpecificAggregateStats ComponentPageInfo
            LogConfig LogLevel LogLevelAction WorkerSummary OwnerResourceSummary])
  (:import [backtype.storm.security.auth AuthUtils ReqContext])
  (:import [backtype.storm.generated AuthorizationException ProfileRequest ProfileAction NodeInfo])
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [backtype.storm.utils VersionInfo])
  (:import [java.io File])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [backtype.storm [thrift :as thrift]])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:import [org.apache.commons.lang StringEscapeUtils])
  (:import [org.apache.logging.log4j Level])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))
(def ^:dynamic *UI-ACL-HANDLER* (mk-authorization-handler (*STORM-CONF* NIMBUS-AUTHORIZER) *STORM-CONF*))
(def ^:dynamic *UI-IMPERSONATION-HANDLER* (mk-authorization-handler (*STORM-CONF* NIMBUS-IMPERSONATION-AUTHORIZER) *STORM-CONF*))
(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))
(defmeter ui:num-getTopologyInfo-calls)
(def STORM-VERSION (VersionInfo/getVersion))

(defmacro with-nimbus
  [nimbus-sym & body]
  `(let [context# (ReqContext/context)
         user# (if (.principal context#) (.getName (.principal context#)))]
    (thrift/with-nimbus-connection-as-user
       [~nimbus-sym (*STORM-CONF* NIMBUS-HOST) (*STORM-CONF* NIMBUS-THRIFT-PORT) user#]
       ~@body)))

(defmeter ui:num-cluster-configuration-httpRequests)
(defmeter ui:num-cluster-summary-httpRequests)
(defmeter ui:num-nimbus-summary-httpRequests)
(defmeter ui:num-supervisor-summary-httpRequests)
(defmeter ui:num-all-topologies-summary-httpRequests)
(defmeter ui:num-topology-page-httpRequests)
(defmeter ui:num-topology-workers-page-httpRequests)
(defmeter ui:num-build-visualization-httpRequests)
(defmeter ui:num-mk-visualization-data-httpRequests)
(defmeter ui:num-component-page-httpRequests)
(defmeter ui:num-log-config-httpRequests)
(defmeter ui:num-activate-topology-httpRequests)
(defmeter ui:num-deactivate-topology-httpRequests)
(defmeter ui:num-debug-topology-httpRequests)
(defmeter ui:num-component-op-response-httpRequests)
(defmeter ui:num-topology-op-response-httpRequests)
(defmeter ui:num-topology-op-response-httpRequests)
(defmeter ui:num-topology-op-response-httpRequests)
(defmeter ui:num-main-page-httpRequests)
(defmeter ui:num-web-requests)

(defn assert-authorized-user
  ([op]
    (assert-authorized-user op nil))
  ([op topology-conf]
    (let [context (ReqContext/context)]
      (if (.isImpersonating context)
        (if *UI-IMPERSONATION-HANDLER*
            (if-not (.permit *UI-IMPERSONATION-HANDLER* context op topology-conf)
              (let [principal (.principal context)
                    real-principal (.realPrincipal context)
                    user (if principal (.getName principal) "unknown")
                    real-user (if real-principal (.getName real-principal) "unknown")
                    remote-address (.remoteAddress context)]
                (throw (AuthorizationException.
                         (str "user '" real-user "' is not authorized to impersonate user '" user "' from host '" remote-address "'. Please
                         see SECURITY.MD to learn how to configure impersonation ACL.")))))
          (log-warn " principal " (.realPrincipal context) " is trying to impersonate " (.principal context) " but "
            NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. This is a potential security hole.
            Please see SECURITY.MD to learn how to configure an impersonation authorizer.")))

      (if *UI-ACL-HANDLER*
       (if-not (.permit *UI-ACL-HANDLER* context op topology-conf)
         (let [principal (.principal context)
               user (if principal (.getName principal) "unknown")]
           (throw (AuthorizationException.
                   (str "UI request '" op "' for '" user "' user is not authorized")))))))))

(defn executor-summary-type
  [topology ^ExecutorSummary s]
  (component-type topology (.get_component_id s)))

(defn is-ack-stream
  [stream]
  (let [acker-streams
        [ACKER-INIT-STREAM-ID
         ACKER-ACK-STREAM-ID
         ACKER-FAIL-STREAM-ID]]
    (every? #(not= %1 stream) acker-streams)))

(defn spout-summary?
  [topology s]
  (= :spout (executor-summary-type topology s)))

(defn bolt-summary?
  [topology s]
  (= :bolt (executor-summary-type topology s)))

(defn group-by-comp
  [summs]
  (let [ret (group-by #(.get_component_id ^ExecutorSummary %) summs)]
    (into (sorted-map) ret )))

(defn component-task-summs
  [^TopologyInfo summ topology id]
  (let [spout-summs (filter (partial spout-summary? topology) (.get_executors summ))
        bolt-summs (filter (partial bolt-summary? topology) (.get_executors summ))
        spout-comp-summs (group-by-comp spout-summs)
        bolt-comp-summs (group-by-comp bolt-summs)
        ret (if (contains? spout-comp-summs id)
              (spout-comp-summs id)
              (bolt-comp-summs id))]
    (sort-by #(-> ^ExecutorSummary % .get_executor_info .get_task_start) ret)))

(defn worker-log-link [host port topology-id secure?]
  (let [fname (logs-filename topology-id port)]
    (if (and secure? (*STORM-CONF* LOGVIEWER-HTTPS-PORT))
      (url-format "https://%s:%s/log?file=%s"
                  host
                  (*STORM-CONF* LOGVIEWER-HTTPS-PORT)
                  fname)
      (url-format "http://%s:%s/log?file=%s"
                  host
                  (*STORM-CONF* LOGVIEWER-PORT)
                  fname))))

(defn nimbus-log-link [host]
  (url-format "http://%s:%s/daemonlog?file=nimbus.log" host (*STORM-CONF* LOGVIEWER-PORT)))

(defn supervisor-log-link [scheme host]
  (url-format "%s://%s:%s/daemonlog?file=supervisor.log"
              (name scheme)
              host
              (if (= scheme :https) (*STORM-CONF* LOGVIEWER-HTTPS-PORT) (*STORM-CONF* LOGVIEWER-PORT))))

(defn get-error-time
  [error]
  (if error
    (time-delta (.get_error_time_secs ^ErrorInfo error))))

(defn get-error-data
  [error]
  (if error
    (error-subset (.get_error ^ErrorInfo error))
    ""))

(defn get-error-port
  [error error-host top-id]
  (if error
    (.get_port ^ErrorInfo error)
    ""))

(defn get-error-host
  [error]
  (if error
    (.get_host ^ErrorInfo error)
    ""))

(defn worker-dump-link [scheme host port topology-id]
  (url-format "%s://%s:%s/dumps/%s/%s"
              (name scheme)
              (url-encode host)
              (if (= scheme :https) (*STORM-CONF* LOGVIEWER-HTTPS-PORT) (*STORM-CONF* LOGVIEWER-PORT))
              (url-encode topology-id)
              (str (url-encode host) ":" (url-encode port))))

(defn stats-times
  [stats-map]
  (sort-by #(Integer/parseInt %)
           (-> stats-map
               clojurify-structure
               (dissoc ":all-time")
               keys)))

(defn window-hint
  [window]
  (if (= window ":all-time")
    "All time"
    (pretty-uptime-sec window)))

(defn sanitize-stream-name
  [name]
  (let [sym-regex #"(?![A-Za-z_\-:\.])."]
    (str
     (if (re-find #"^[A-Za-z]" name)
       (clojure.string/replace name sym-regex "_")
       (clojure.string/replace (str \s name) sym-regex "_"))
     (hash name))))

(defn sanitize-transferred
  [transferred]
  (into {}
        (for [[time, stream-map] transferred]
          [time, (into {}
                       (for [[stream, trans] stream-map]
                         [(sanitize-stream-name stream), trans]))])))

(defn visualization-data
  [spout-bolt spout-comp-summs bolt-comp-summs window storm-id]
  (let [components (for [[id spec] spout-bolt]
            [id
             (let [inputs (.get_inputs (.get_common spec))
                   bolt-summs (get bolt-comp-summs id)
                   spout-summs (get spout-comp-summs id)
                   bolt-cap (if bolt-summs
                              (compute-bolt-capacity bolt-summs)
                              0)]
               {:type (if bolt-summs "bolt" "spout")
                :capacity bolt-cap
                :latency (if bolt-summs
                           (get-in
                             (bolt-streams-stats bolt-summs true)
                             [:process-latencies window])
                           (get-in
                             (spout-streams-stats spout-summs true)
                             [:complete-latencies window]))
                :transferred (or
                               (get-in
                                 (spout-streams-stats spout-summs true)
                                 [:transferred window])
                               (get-in
                                 (bolt-streams-stats bolt-summs true)
                                 [:transferred window]))
                :stats (let [mapfn (fn [dat]
                                     (map (fn [^ExecutorSummary summ]
                                            {:host (.get_host summ)
                                             :port (.get_port summ)
                                             :uptime_secs (.get_uptime_secs summ)
                                             :transferred (if-let [stats (.get_stats summ)]
                                                            (sanitize-transferred (.get_transferred stats)))})
                                          dat))]
                         (if bolt-summs
                           (mapfn bolt-summs)
                           (mapfn spout-summs)))
                :link (url-format "/component.html?id=%s&topology_id=%s" id storm-id)
                :inputs (for [[global-stream-id group] inputs]
                          {:component (.get_componentId global-stream-id)
                           :stream (.get_streamId global-stream-id)
                           :sani-stream (sanitize-stream-name (.get_streamId global-stream-id))
                           :grouping (clojure.core/name (thrift/grouping-type group))})})])]
    (into {} (doall components))))

(defn stream-boxes [datmap]
  (let [filter-fn (mk-include-sys-fn true)
        streams
        (vec (doall (distinct
                     (apply concat
                            (for [[k v] datmap]
                              (for [m (get v :inputs)]
                                {:stream (get m :stream)
                                 :sani-stream (get m :sani-stream)
                                 :checked (is-ack-stream (get m :stream))}))))))]
    (map (fn [row]
           {:row row}) (partition 4 4 nil streams))))

(defn- get-topology-info
  ([^Nimbus$Client nimbus id]
   (mark! ui:num-getTopologyInfo-calls)
   (.getTopologyInfo nimbus id))
  ([^Nimbus$Client nimbus id options]
   (mark! ui:num-getTopologyInfo-calls)
   (.getTopologyInfoWithOpts nimbus id options)))

(defn mk-visualization-data
  [id window include-sys?]
  (with-nimbus
    nimbus
    (let [window (if window window ":all-time")
          topology (.getTopology ^Nimbus$Client nimbus id)
          spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          summ (->> (doto
                      (GetInfoOptions.)
                      (.set_num_err_choice NumErrorsChoice/NONE))
                    (get-topology-info nimbus id))
          execs (.get_executors summ)
          spout-summs (filter (partial spout-summary? topology) execs)
          bolt-summs (filter (partial bolt-summary? topology) execs)
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          bolt-comp-summs (filter-key (mk-include-sys-fn include-sys?)
                                      bolt-comp-summs)]
      (visualization-data 
       (merge (hashmap-to-persistent spouts)
              (hashmap-to-persistent bolts))
       spout-comp-summs bolt-comp-summs window id))))

(defn cluster-configuration []
  (with-nimbus nimbus
    (.getNimbusConf ^Nimbus$Client nimbus)))

(defn topology-history-info
  ([user]
   (with-nimbus nimbus
     (topology-history-info (.getTopologyHistory ^Nimbus$Client nimbus user) user)))
  ([history user]
   {"topo-history"
    (for [^String s (.get_topo_ids history)]
      {"host" s})}))

(defn scheduler-configuration []
  (with-nimbus nimbus
    (.getSchedulerConf ^Nimbus$Client nimbus)))

(defn cluster-summary
  ([user]
     (with-nimbus nimbus
        (cluster-summary (.getClusterInfo ^Nimbus$Client nimbus) user)))
  ([^ClusterSummary summ user]
     (let [sups (.get_supervisors summ)
           used-slots (reduce + (map #(.get_num_used_workers ^SupervisorSummary %) sups))
           total-slots (reduce + (map #(.get_num_workers ^SupervisorSummary %) sups))
           free-slots (- total-slots used-slots)
           topologies (.get_topologies_size summ)
           total-tasks (->> (.get_topologies summ)
                            (map #(.get_num_tasks ^TopologySummary %))
                            (reduce +))
           total-executors (->> (.get_topologies summ)
                                (map #(.get_num_executors ^TopologySummary %))
                             (reduce +))
           resourceSummary (if (> (.size sups) 0)
                             (reduce #(map + %1 %2)
                               (for [^SupervisorSummary s sups
                                     :let [sup-total-mem (get (.get_total_resources s) Config/SUPERVISOR_MEMORY_CAPACITY_MB)
                                           sup-total-cpu (get (.get_total_resources s) Config/SUPERVISOR_CPU_CAPACITY)
                                           sup-avail-mem (max (- sup-total-mem (.get_used_mem s)) 0.0)
                                           sup-avail-cpu (max (- sup-total-cpu (.get_used_cpu s)) 0.0)]]
                                 [sup-total-mem sup-total-cpu sup-avail-mem sup-avail-cpu]))
                             [0.0 0.0 0.0 0.0])
           total-mem (nth resourceSummary 0)
           total-cpu (nth resourceSummary 1)
           avail-mem (nth resourceSummary 2)
           avail-cpu (nth resourceSummary 3)]
       {"user" user
        "stormVersion" STORM-VERSION
        "nimbusUptime" (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
        "nimbusUptimeSeconds" (.get_nimbus_uptime_secs summ)
        "nimbusLogLink" (nimbus-log-link (*STORM-CONF* NIMBUS-HOST))
        "host" (*STORM-CONF* NIMBUS-HOST)
        "supervisors" (count sups)
        "topologies" topologies
        "slotsTotal" total-slots
        "slotsUsed"  used-slots
        "slotsFree" free-slots
        "executorsTotal" total-executors
        "tasksTotal" total-tasks
        "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)
        "totalMem" total-mem
        "totalCpu" total-cpu
        "availMem" avail-mem
        "availCpu" avail-cpu
        "memAssignedPercentUtil" (if (and (not (nil? total-mem)) (> total-mem 0.0)) (format "%.1f" (* (/ (- total-mem avail-mem) total-mem) 100.0)) 0.0)
        "cpuAssignedPercentUtil" (if (and (not (nil? total-cpu)) (> total-cpu 0.0)) (format "%.1f" (* (/ (- total-cpu avail-cpu) total-cpu) 100.0)) 0.0)})))

(defn worker-summary-to-json
  [secure-link? ^WorkerSummary worker-summary]
  (let [host (.get_host worker-summary)
        port (.get_port worker-summary)
        topology-id (.get_topology_id worker-summary)
        uptime-secs (.get_uptime_secs worker-summary)]
    {"supervisorId" (.get_supervisor_id worker-summary)
     "host" host
     "port" port
     "topologyId" topology-id
     "topologyName" (.get_topology_name worker-summary)
     "executorsTotal" (.get_num_executors worker-summary)
     "assignedMemOnHeap" (.get_assigned_memonheap worker-summary)
     "assignedMemOffHeap" (.get_assigned_memoffheap worker-summary)
     "assignedCpu" (.get_assigned_cpu worker-summary)
     "componentNumTasks" (.get_component_to_num_tasks worker-summary)
     "uptime" (pretty-uptime-sec uptime-secs)
     "uptimeSeconds" uptime-secs
     "workerLogLink" (worker-log-link host port topology-id secure-link?)}))

(defn supervisor-summary-to-json 
  [summary scheme]
  (let [slotsTotal (.get_num_workers summary)
        slotsUsed (.get_num_used_workers summary)
        slotsFree (max (- slotsTotal slotsUsed) 0)
        totalMem (get (.get_total_resources summary) Config/SUPERVISOR_MEMORY_CAPACITY_MB)
        totalCpu (get (.get_total_resources summary) Config/SUPERVISOR_CPU_CAPACITY)
        usedMem (.get_used_mem summary)
        usedCpu (.get_used_cpu summary)
        availMem (max (- totalMem usedMem) 0)
        availCpu (max (- totalCpu usedCpu) 0)]
  {"id" (.get_supervisor_id summary)
   "host" (.get_host summary)
   "uptime" (pretty-uptime-sec (.get_uptime_secs summary))
   "uptimeSeconds" (.get_uptime_secs summary)
   "slotsTotal" slotsTotal
   "slotsUsed" slotsUsed
   "slotsFree" slotsFree
   "totalMem" totalMem
   "totalCpu" totalCpu
   "usedMem" usedMem
   "usedCpu" usedCpu
   "availMem" availMem
   "availCpu" availCpu
   "logLink" (supervisor-log-link scheme (.get_host summary))
   "version" (.get_version summary)}))

(defn supervisor-page-info
  ([supervisor-id host include-sys? scheme]
     (with-nimbus nimbus
        (supervisor-page-info (.getSupervisorPageInfo ^Nimbus$Client nimbus
                                                      supervisor-id
                                                      host
                                                      include-sys?) scheme)))
  ([^SupervisorPageInfo supervisor-page-info scheme]
    ;; ask nimbus to return supervisor workers + any details user is allowed
    ;; access on a per-topology basis (i.e. components)
    (let [supervisors-json (map #(supervisor-summary-to-json % scheme) (.get_supervisor_summaries supervisor-page-info))]
      {"supervisors" supervisors-json
       "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)
       "workers" (into [] (for [^WorkerSummary worker-summary (.get_worker_summaries supervisor-page-info)]
                            (worker-summary-to-json (= scheme :https) worker-summary)))})))

(defn supervisor-summary
  ([scheme]
   (with-nimbus nimbus
     (supervisor-summary
       (.get_supervisors (.getClusterInfo ^Nimbus$Client nimbus)) scheme)))
  ([summs scheme]
   (let [bad-supervisors (filter #(= "unknown" (.get_host ^SupervisorSummary %)) summs)
         good-supervisors (filter #(not= "unknown" (.get_host ^SupervisorSummary %)) summs)]
     {"supervisors" (for [^SupervisorSummary s good-supervisors]
                      (supervisor-summary-to-json s scheme))
      "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)
      "has-bad-supervisors" (not (empty? bad-supervisors))
      "bad-supervisors"
      (for [^SupervisorSummary s bad-supervisors]
        {"id" (.get_supervisor_id s)
         "error" (.get_version s)})})))


(defnk get-topologies-map [summs :conditional (fn [t] true) :keys nil]
  (for [^TopologySummary t summs :when (conditional t)]
    (let [data {"id" (.get_id t)
                "encodedId" (url-encode (.get_id t))
                "owner" (.get_owner t)
                "name" (.get_name t)
                "status" (.get_status t)
                "uptime" (pretty-uptime-sec (.get_uptime_secs t))
                "uptimeSeconds" (.get_uptime_secs t)
                "tasksTotal" (.get_num_tasks t)
                "workersTotal" (.get_num_workers t)
                "executorsTotal" (.get_num_executors t)
                "schedulerInfo" (.get_sched_status t)
                "requestedMemOnHeap" (.get_requested_memonheap t)
                "requestedMemOffHeap" (.get_requested_memoffheap t)
                "requestedTotalMem" (+ (.get_requested_memonheap t) (.get_requested_memoffheap t))
                "requestedCpu" (.get_requested_cpu t)
                "assignedMemOnHeap" (.get_assigned_memonheap t)
                "assignedMemOffHeap" (.get_assigned_memoffheap t)
                "assignedTotalMem" (+ (.get_assigned_memonheap t) (.get_assigned_memoffheap t))
                "assignedCpu" (.get_assigned_cpu t)
                "topologyVersion" (.get_topology_version t)}]
      (if (not-nil? keys) (select-keys data keys) data))))

(defn all-topologies-summary
  ([]
   (with-nimbus
     nimbus
     (all-topologies-summary
       (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"topologies" (get-topologies-map summs)
    "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)}))

(defn topology-stats [window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [w (concat times [":all-time"])
          :let [disp ((display-map w) w)]]
      {"windowPretty" disp
       "window" w
       "emitted" (get-in stats [:emitted w])
       "transferred" (get-in stats [:transferred w])
       "completeLatency" (float-str (get-in stats [:complete-latencies w]))
       "acked" (get-in stats [:acked w])
       "failed" (get-in stats [:failed w])})))

(defn build-visualization [id window include-sys?]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          topology-info (->> (doto
                               (GetInfoOptions.)
                               (.set_num_err_choice NumErrorsChoice/ONE))
                             (get-topology-info nimbus id))
          storm-topology (.getTopology ^Nimbus$Client nimbus id)
          spout-executor-summaries (filter (partial spout-summary? storm-topology) (.get_executors topology-info))
          bolt-executor-summaries (filter (partial bolt-summary? storm-topology) (.get_executors topology-info))
          spout-comp-id->executor-summaries (group-by-comp spout-executor-summaries)
          bolt-comp-id->executor-summaries (group-by-comp bolt-executor-summaries)
          bolt-comp-id->executor-summaries (filter-key (mk-include-sys-fn include-sys?) bolt-comp-id->executor-summaries)
          id->spout-spec (.get_spouts storm-topology)
          id->bolt (.get_bolts storm-topology)
          visualizer-data (visualization-data (merge (hashmap-to-persistent id->spout-spec)
                                                     (hashmap-to-persistent id->bolt))
                                              spout-comp-id->executor-summaries
                                              bolt-comp-id->executor-summaries
                                              window
                                              id)]
       {"visualizationTable" (stream-boxes visualizer-data)})))

(defn- common-agg-stats-json
  "Returns a JSON representation of a common aggregated statistics."
  [^CommonAggregateStats common-stats]
  {"executors" (.get_num_executors common-stats)
   "tasks" (.get_num_tasks common-stats)
   "emitted" (.get_emitted common-stats)
   "transferred" (.get_transferred common-stats)
   "acked" (.get_acked common-stats)
   "failed" (.get_failed common-stats)
   "requestedMemOnHeap" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)
   "requestedMemOffHeap" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)
   "requestedCpu" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)})

(defmulti comp-agg-stats-json
  "Returns a JSON representation of aggregated statistics."
  (fn [_ [id ^ComponentAggregateStats s]] (.get_type s)))

(defmethod comp-agg-stats-json ComponentType/SPOUT
  [topo-id [id ^ComponentAggregateStats s]]
  (let [^SpoutAggregateStats ss (.. s get_specific_stats get_spout)
        cs (.get_common_stats s)]
    (merge
      (common-agg-stats-json cs)
      {"spoutId" id
       "encodedSpoutId" (url-encode id)
       "completeLatency" (float-str (.get_complete_latency_ms ss))
       "lastError" (error-subset (if-let [e (.get_last_error s)]
                                              (.get_error e)))})))

(defmethod comp-agg-stats-json ComponentType/BOLT
  [topo-id [id ^ComponentAggregateStats s]]
  (let [^BoltAggregateStats ss (.. s get_specific_stats get_bolt)
        cs (.get_common_stats s)]
    (merge
      (common-agg-stats-json cs)
      {"boltId" id
       "encodedBoltId" (url-encode id)
       "capacity" (float-str (.get_capacity ss))
       "executeLatency" (float-str (.get_execute_latency_ms ss))
       "executed" (.get_executed ss)
       "processLatency" (float-str (.get_process_latency_ms ss))
       "lastError" (error-subset (if-let [e (.get_last_error s)]
                                              (.get_error e)))})))

(defn topology-errors
  [errors-list topology-id]
  (let [errors (->> errors-list
                 (sort-by #(.get_error_time_secs ^ErrorInfo %))
                 reverse)]
    {"topologyErrors"
     (for [^ErrorInfo e errors]
       {"time" (* 1000 (long (.get_error_time_secs e)))
        "errorHost" (.get_host e)
        "errorPort"  (.get_port e)
        "errorLapsedSecs" (get-error-time e)
        "error" (.get_error e)})}))

(defn- unpack-topology-page-info
  "Unpacks the serialized object to data structures"
  [^TopologyPageInfo topo-info window secure?]
  (let [id (.get_id topo-info)
        ^TopologyStats topo-stats (.get_topology_stats topo-info)
        stat->window->number
          {:emitted (.get_window_to_emitted topo-stats)
           :transferred (.get_window_to_transferred topo-stats)
           :complete-latencies (.get_window_to_complete_latencies_ms topo-stats)
           :acked (.get_window_to_acked topo-stats)
           :failed (.get_window_to_failed topo-stats)}
        topo-stats (topology-stats window stat->window->number)]
    (merge
      {"id" id
       "encodedId" (url-encode id)
       "owner" (.get_owner topo-info)
       "name" (.get_name topo-info)
       "status" (.get_status topo-info)
       "uptime" (pretty-uptime-sec (.get_uptime_secs topo-info))
       "uptimeSeconds" (.get_uptime_secs topo-info)
       "tasksTotal" (.get_num_tasks topo-info)
       "workersTotal" (.get_num_workers topo-info)
       "executorsTotal" (.get_num_executors topo-info)
       "schedulerInfo" (.get_sched_status topo-info)
       "requestedMemOnHeap" (.get_requested_memonheap topo-info)
       "requestedMemOffHeap" (.get_requested_memoffheap topo-info)
       "requestedTotalMem" (+ (.get_requested_memonheap topo-info) (.get_requested_memoffheap topo-info))
       "requestedCpu" (.get_requested_cpu topo-info)
       "assignedMemOnHeap" (.get_assigned_memonheap topo-info)
       "assignedMemOffHeap" (.get_assigned_memoffheap topo-info)
       "assignedTotalMem" (+ (.get_assigned_memonheap topo-info) (.get_assigned_memoffheap topo-info))
       "assignedCpu" (.get_assigned_cpu topo-info)
       "requestedRegularOnHeapMem" (.get_requested_regular_on_heap_memory topo-info)
       "requestedSharedOnHeapMem" (.get_requested_shared_on_heap_memory topo-info)
       "requestedRegularOffHeapMem" (.get_requested_regular_off_heap_memory topo-info)
       "requestedSharedOffHeapMem" (.get_requested_shared_off_heap_memory topo-info)
       "assignedRegularOnHeapMem" (.get_assigned_regular_on_heap_memory topo-info)
       "assignedSharedOnHeapMem" (.get_assigned_shared_on_heap_memory topo-info)
       "assignedRegularOffHeapMem" (.get_assigned_regular_off_heap_memory topo-info)
       "assignedSharedOffHeapMem" (.get_assigned_shared_off_heap_memory topo-info)
       "topologyStats" topo-stats
       "workers"  (map (partial worker-summary-to-json secure?)
                       (.get_workers topo-info))
       "spouts" (map (partial comp-agg-stats-json id)
                     (.get_id_to_spout_agg_stats topo-info))
       "bolts" (map (partial comp-agg-stats-json id)
                    (.get_id_to_bolt_agg_stats topo-info))
       "configuration" (.get_topology_conf topo-info)
       "topologyVersion" (.get_topology_version topo-info)}
      (topology-errors (.get_errors topo-info) id))))

(defn exec-host-port
  [executors]
  (for [^ExecutorSummary e executors]
    {"host" (.get_host e)
     "port" (.get_port e)}))

(defn worker-host-port
  "Get the set of all worker host/ports"
  [id]
  (with-nimbus nimbus
    (distinct (exec-host-port (.get_executors (get-topology-info nimbus id))))))

(defn topology-page [id window include-sys? user secure?]
  (with-nimbus nimbus
    (let [window (or window ":all-time")
          window-hint (window-hint window)
          topo-page-info (.getTopologyPageInfo ^Nimbus$Client nimbus
                                               id
                                               window
                                               include-sys?)
          topology-conf (from-json (.get_topology_conf topo-page-info))
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)]
      (merge
       (unpack-topology-page-info topo-page-info window secure?)
       {"user" user
        "window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "configuration" topology-conf
        "visualizationTable" []
        "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)}))))

(defn component-errors
  [errors-list topology-id secure?]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"componentErrors"
     (for [^ErrorInfo e errors]
       {"time" (* 1000 (long (.get_error_time_secs e)))
        "errorHost" (.get_host e)
        "errorPort"  (.get_port e)
        "errorWorkerLogLink"  (worker-log-link (.get_host e) (.get_port e) topology-id secure?)
        "errorLapsedSecs" (get-error-time e)
        "error" (.get_error e)})}))

(defmulti unpack-comp-agg-stat
  (fn [[_ ^ComponentAggregateStats s]] (.get_type s)))

(defmethod unpack-comp-agg-stat ComponentType/BOLT
  [[window ^ComponentAggregateStats s]]
  (let [^CommonAggregateStats comm-s (.get_common_stats s)
        ^SpecificAggregateStats spec-s (.get_specific_stats s)
        ^BoltAggregateStats bolt-s (.get_bolt spec-s)]
    {"window" window
     "windowPretty" (window-hint window)
     "emitted" (.get_emitted comm-s)
     "transferred" (.get_transferred comm-s)
     "acked" (.get_acked comm-s)
     "failed" (.get_failed comm-s)
     "executeLatency" (float-str (.get_execute_latency_ms bolt-s))
     "processLatency"  (float-str (.get_process_latency_ms bolt-s))
     "executed" (.get_executed bolt-s)
     "capacity" (float-str (.get_capacity bolt-s))}))

(defmethod unpack-comp-agg-stat ComponentType/SPOUT
  [[window ^ComponentAggregateStats s]]
  (let [^CommonAggregateStats comm-s (.get_common_stats s)
        ^SpecificAggregateStats spec-s (.get_specific_stats s)
        ^SpoutAggregateStats spout-s (.get_spout spec-s)]
    {"window" window
     "windowPretty" (window-hint window)
     "emitted" (.get_emitted comm-s)
     "transferred" (.get_transferred comm-s)
     "acked" (.get_acked comm-s)
     "failed" (.get_failed comm-s)
     "completeLatency" (float-str (.get_complete_latency_ms spout-s))}))

(defn- unpack-bolt-input-stat
  [[^GlobalStreamId s ^ComponentAggregateStats stats]]
  (let [^SpecificAggregateStats sas (.get_specific_stats stats)
        ^BoltAggregateStats bas (.get_bolt sas)
        ^CommonAggregateStats cas (.get_common_stats stats)
        comp-id (.get_componentId s)]
    {"component" comp-id
     "encodedComponentId" (url-encode comp-id)
     "stream" (.get_streamId s)
     "executeLatency" (float-str (.get_execute_latency_ms bas))
     "processLatency" (float-str (.get_process_latency_ms bas))
     "executed" (nil-to-zero (.get_executed bas))
     "acked" (nil-to-zero (.get_acked cas))
     "failed" (nil-to-zero (.get_failed cas))}))

(defmulti unpack-comp-output-stat
  (fn [[_ ^ComponentAggregateStats s]] (.get_type s)))

(defmethod unpack-comp-output-stat ComponentType/BOLT
  [[stream-id ^ComponentAggregateStats stats]]
  (let [^CommonAggregateStats cas (.get_common_stats stats)]
    {"stream" stream-id
     "emitted" (nil-to-zero (.get_emitted cas))
     "transferred" (nil-to-zero (.get_transferred cas))}))

(defmethod unpack-comp-output-stat ComponentType/SPOUT
  [[stream-id ^ComponentAggregateStats stats]]
  (let [^CommonAggregateStats cas (.get_common_stats stats)
        ^SpecificAggregateStats spec-s (.get_specific_stats stats)
        ^SpoutAggregateStats spout-s (.get_spout spec-s)]
    {"stream" stream-id
     "emitted" (nil-to-zero (.get_emitted cas))
     "transferred" (nil-to-zero (.get_transferred cas))
     "completeLatency" (float-str (.get_complete_latency_ms spout-s))
     "acked" (nil-to-zero (.get_acked cas))
     "failed" (nil-to-zero (.get_failed cas))}))

(defmulti unpack-comp-exec-stat
  (fn [_ secure-link? ^ComponentAggregateStats cas] (.get_type (.get_stats ^ExecutorAggregateStats cas))))

(defmethod unpack-comp-exec-stat ComponentType/BOLT
  [topology-id secure-link? ^ExecutorAggregateStats eas]
  (let [^ExecutorSummary summ (.get_exec_summary eas)
        ^ExecutorInfo info (.get_executor_info summ)
        ^ComponentAggregateStats stats (.get_stats eas)
        ^SpecificAggregateStats ss (.get_specific_stats stats)
        ^BoltAggregateStats bas (.get_bolt ss)
        ^CommonAggregateStats cas (.get_common_stats stats)
        host (.get_host summ)
        port (.get_port summ)
        exec-id (pretty-executor-info info)]
    {"id" exec-id
     "encodedId" (url-encode exec-id)
     "uptime" (pretty-uptime-sec (.get_uptime_secs summ))
     "uptimeSeconds" (.get_uptime_secs summ)
     "host" host
     "port" port
     "emitted" (nil-to-zero (.get_emitted cas))
     "transferred" (nil-to-zero (.get_transferred cas))
     "capacity" (float-str (nil-to-zero (.get_capacity bas)))
     "executeLatency" (float-str (.get_execute_latency_ms bas))
     "executed" (nil-to-zero (.get_executed bas))
     "processLatency" (float-str (.get_process_latency_ms bas))
     "acked" (nil-to-zero (.get_acked cas))
     "failed" (nil-to-zero (.get_failed cas))
     "workerLogLink" (worker-log-link host port topology-id secure-link?)}))

(defmethod unpack-comp-exec-stat ComponentType/SPOUT
  [topology-id secure-link? ^ExecutorAggregateStats eas]
  (let [^ExecutorSummary summ (.get_exec_summary eas)
        ^ExecutorInfo info (.get_executor_info summ)
        ^ComponentAggregateStats stats (.get_stats eas)
        ^SpecificAggregateStats ss (.get_specific_stats stats)
        ^SpoutAggregateStats sas (.get_spout ss)
        ^CommonAggregateStats cas (.get_common_stats stats)
        host (.get_host summ)
        port (.get_port summ)
        exec-id (pretty-executor-info info)]
    {"id" exec-id
     "encodedId" (url-encode exec-id)
     "uptime" (pretty-uptime-sec (.get_uptime_secs summ))
     "uptimeSeconds" (.get_uptime_secs summ)
     "host" host
     "port" port
     "emitted" (nil-to-zero (.get_emitted cas))
     "transferred" (nil-to-zero (.get_transferred cas))
     "completeLatency" (float-str (.get_complete_latency_ms sas))
     "acked" (nil-to-zero (.get_acked cas))
     "failed" (nil-to-zero (.get_failed cas))
     "workerLogLink" (worker-log-link host port topology-id secure-link?)}))

(defmulti unpack-component-page-info
  "Unpacks component-specific info to clojure data structures"
  (fn [^ComponentPageInfo info & _]
    (.get_component_type info)))

(defmethod unpack-component-page-info ComponentType/BOLT
  [^ComponentPageInfo info topology-id window include-sys? secure?]
  (merge
    {"boltStats" (map unpack-comp-agg-stat (.get_window_to_stats info))
     "inputStats" (map unpack-bolt-input-stat (.get_gsid_to_input_stats info))
     "outputStats" (map unpack-comp-output-stat (.get_sid_to_output_stats info))
     "executorStats" (map (partial unpack-comp-exec-stat topology-id secure?)
                          (.get_exec_stats info))}
    (component-errors (.get_errors info) topology-id secure?)))

(defmethod unpack-component-page-info ComponentType/SPOUT
  [^ComponentPageInfo info topology-id window include-sys? secure?]
  (merge
    {"spoutSummary" (map unpack-comp-agg-stat (.get_window_to_stats info))
     "outputStats" (map unpack-comp-output-stat (.get_sid_to_output_stats info))
     "executorStats" (map (partial unpack-comp-exec-stat topology-id secure?)
                          (.get_exec_stats info))}
    (component-errors (.get_errors info) topology-id secure?)))

(defn get-active-profile-actions
  [nimbus topology-id component scheme]
  (let [profile-actions  (.getComponentPendingProfileActions nimbus
                                               topology-id
                                               component
                                 ProfileAction/JPROFILE_STOP)
        latest-profile-actions (map clojurify-profile-request profile-actions)
        active-actions (map (fn [profile-action]
                              {"host" (:host profile-action)
                               "port" (str (:port profile-action))
                               "dumplink" (worker-dump-link scheme (:host profile-action) (str (:port profile-action)) topology-id)
                               "timestamp" (str (- (:timestamp profile-action) (System/currentTimeMillis)))})
                            latest-profile-actions)]
    (log-message "Latest-active actions are: " (pr-str active-actions))
    active-actions))

(defn component-page
  [topology-id component window include-sys? user secure?]
  (with-nimbus nimbus
    (let [topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus
                                                     topology-id))
          window (or window ":all-time")
          window-hint (window-hint window)
          comp-page-info (.getComponentPageInfo ^Nimbus$Client nimbus
                                                topology-id
                                                component
                                                window
                                                include-sys?)
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)]
      (assoc
       (unpack-component-page-info comp-page-info
                                   topology-id
                                   window
                                   include-sys?
                                   secure?)
       "user" user
       "id" component
       "encodedId" (url-encode component)
       "name" (.get_topology_name comp-page-info)
       "executors" (.get_num_executors comp-page-info)
       "tasks" (.get_num_tasks comp-page-info)
       "requestedMemOnHeap" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)
       "requestedMemOffHeap" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)
       "requestedCpu" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)
       "schedulerDisplayResource" (*STORM-CONF* Config/SCHEDULER_DISPLAY_RESOURCE)
       "topologyId" topology-id
       "encodedTopologyId" (url-encode topology-id)
       "window" window
       "componentType" (-> comp-page-info .get_component_type str lower-case)
       "windowHint" window-hint
       "profilerActive" (get-active-profile-actions nimbus topology-id component (if secure? ":https" ":http"))))))

(defn unpack-owner-resource-summary [summary]
  (let [memory-guarantee (if (.is_set_memory_guarantee summary) 
                         (.get_memory_guarantee summary)
                         "N/A")
        cpu-guarantee (if (.is_set_cpu_guarantee summary)
                        (.get_cpu_guarantee summary)
                        "N/A")
        isolated-node-guarantee (if (.is_set_isolated_node_guarantee summary)
                                  (.get_isolated_node_guarantee summary)
                                  "N/A")
        memory-guarantee-remaining (if (.is_set_memory_guarantee_remaining summary)
                                     (.get_memory_guarantee_remaining summary)
                                     "N/A")
        cpu-guarantee-remaining (if (.is_set_cpu_guarantee_remaining summary)
                                  (.get_cpu_guarantee_remaining summary)
                                  "N/A")]
    {"owner" (.get_owner summary)
     "totalTopologies" (.get_total_topologies summary)
     "totalExecutors" (.get_total_executors summary)
     "totalWorkers" (.get_total_workers summary)
     "totalTasks" (.get_total_tasks summary)
     "totalMemoryUsage" (.get_memory_usage summary) 
     "totalCpuUsage" (.get_cpu_usage summary)
     "memoryGuarantee" memory-guarantee
     "cpuGuarantee" cpu-guarantee
     "isolatedNodes" isolated-node-guarantee
     "memoryGuaranteeRemaining" memory-guarantee-remaining
     "cpuGuaranteeRemaining" cpu-guarantee-remaining
     "totalReqOnHeapMem" (.get_requested_on_heap_memory summary) 
     "totalReqOffHeapMem" (.get_requested_off_heap_memory summary)
     "totalReqMem" (.get_requested_total_memory summary)
     "totalReqCpu" (.get_requested_cpu summary)
     "totalAssignedOnHeapMem" (.get_assigned_on_heap_memory summary)
     "totalAssignedOffHeapMem" (.get_assigned_off_heap_memory summary)
     "rasTopologiesAssignedMem" (.get_assigned_ras_total_memory summary)
     "rasTopologiesAssignedCpu" (.get_assigned_ras_cpu summary)
     "totalReqRegularOnHeapMem" (.get_requested_regular_on_heap_memory summary)
     "totalReqSharedOnHeapMem" (.get_requested_shared_on_heap_memory summary)
     "totalReqRegularOffHeapMem" (.get_requested_regular_off_heap_memory summary)
     "totalReqSharedOffHeapMem" (.get_requested_shared_off_heap_memory summary)
     "totalAssignedRegularOnHeapMem" (.get_assigned_regular_on_heap_memory summary)
     "totalAssignedSharedOnHeapMem" (.get_assigned_shared_on_heap_memory summary)
     "totalAssignedRegularOffHeapMem" (.get_assigned_regular_off_heap_memory summary)
     "totalAssignedSharedOffHeapMem" (.get_assigned_shared_off_heap_memory summary)}))

(defn owner-resource-summaries []
  (with-nimbus nimbus
    (let [summaries (.getOwnerResourceSummaries nimbus nil)]
      {"schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
       "owners"
         (for [summary summaries]
           (unpack-owner-resource-summary summary))})))

(defn owner-resource-summary [owner]
  (with-nimbus nimbus
    (let [summaries (.getOwnerResourceSummaries nimbus owner)]
      (merge {"schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
              "resourceGuaranteeWarningEnabled" (*STORM-CONF* UI-RESOURCE-GUARANTEE-WARNING-ENABLED)
              "resourceGuaranteeAllWarningEnabled" (*STORM-CONF* UI-RESOURCE-GUARANTEE-ALL-WARNING-ENABLED)}
             (if (empty? summaries)
               ;; send a default value, we couldn't find topos by that owner
               (unpack-owner-resource-summary (OwnerResourceSummary. owner))
               (let [topologies (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus))
                     data (get-topologies-map topologies :conditional (fn [t] (= (.get_owner t) owner)))]
                 (merge {"topologies" data}
                        (unpack-owner-resource-summary (first summaries)))))))))

(defn- level-to-dict [level]
  (if level
    (let [timeout (.get_reset_log_level_timeout_secs level)
          timeout-epoch (.get_reset_log_level_timeout_epoch level)
          target-level (.get_target_log_level level)
          reset-level (.get_reset_log_level level)]
          {"target_level" (.toString (Level/toLevel target-level))
           "reset_level" (.toString (Level/toLevel reset-level))
           "timeout" timeout
           "timeout_epoch" timeout-epoch})))

(defn log-config [topology-id]
  (with-nimbus nimbus
    (let [log-config (.getLogConfig ^Nimbus$Client nimbus topology-id)
          named-logger-levels (into {}
                                (for [[key val] (.get_named_logger_level log-config)]
                                  [(str key) (level-to-dict val)]))]
      {"namedLoggerLevels" named-logger-levels})))

(defn topology-config [topology-id]
  (with-nimbus nimbus
    (from-json (.getTopologyConf ^Nimbus$Client nimbus topology-id))))

(defn topology-op-response [topology-id op]
  {"topologyOperation" op,
   "topologyId" topology-id,
   "status" "success"
   })

(defn check-include-sys?
  [sys?]
  (if (or (nil? sys?) (= "false" sys?)) false true))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

(defn populate-context!
  "Populate the Storm RequestContext from an servlet-request. This should be called in each handler"
  [servlet-request]
    (when http-creds-handler
      (.populateContext http-creds-handler (ReqContext/context) servlet-request)))

(defn get-user-name
  [servlet-request]
  (.getUserName http-creds-handler servlet-request))

(defroutes main-routes
  (GET "/api/v1/owner-resources" [:as {:keys [cookies servlet-request scheme]} id & m]
    (populate-context! servlet-request)
    (assert-authorized-user "getOwnerResourceSummaries")
    (json-response (owner-resource-summaries) (:callback m)))
  (GET "/api/v1/owner-resources/:id" [:as {:keys [cookies servlet-request scheme]} id & m]
    (populate-context! servlet-request)
    (assert-authorized-user "getOwnerResourceSummaries")
    (json-response (owner-resource-summary id) (:callback m)))
  (GET "/api/v1/cluster/configuration" [& m]
    (mark! ui:num-cluster-configuration-httpRequests)
    (json-response (cluster-configuration)
                   (:callback m) :serialize-fn identity))
  (GET "/api/v1/cluster/schedulerConfiguration" [& m]
    (json-response (scheduler-configuration)
                   (:callback m) :serialize-fn identity))
  (GET "/api/v1/cluster/summary" [:as {:keys [cookies servlet-request]} & m]
    (mark! ui:num-cluster-summary-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (let [user (get-user-name servlet-request)]
      (json-response (assoc (cluster-summary user)
                        "jira-url" (*STORM-CONF* UI-PROJECT-JIRA-URL)
                        "central-log-url" (*STORM-CONF* UI-CENTRAL-LOGGING-URL)) (:callback m))))
  (GET "/api/v1/history/summary" [:as {:keys [cookies servlet-request]} & m]
    (mark! ui:num-nimbus-summary-httpRequests)
    (populate-context! servlet-request)
    (let [user (get-user-name servlet-request)]
      (json-response (topology-history-info user) (:callback m))))
  (GET "/api/v1/supervisor/summary" [:as {:keys [cookies servlet-request scheme]} & m]
    (mark! ui:num-supervisor-summary-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (json-response (assoc (supervisor-summary scheme)
                          "scheme" (name scheme)
                          "logviewerPort" (if (= scheme :https) (*STORM-CONF* LOGVIEWER-HTTPS-PORT) (*STORM-CONF* LOGVIEWER-PORT)))
                   (:callback m)))
  (GET "/api/v1/supervisor" [:as {:keys [cookies servlet-request scheme]} & m]
    (populate-context! servlet-request)
    (assert-authorized-user "getSupervisorPageInfo")
    ;; supervisor takes either an id or a host query parameter, and technically both
    ;; that said, if both the id and host are provided, the id wins
    (let [id (:id m)
          host (:host m)]
      (json-response (assoc (supervisor-page-info id host (check-include-sys? (:sys m)) scheme)
                            "scheme" (name scheme)
                            "logviewerPort" (if (= scheme :https) (*STORM-CONF* LOGVIEWER-HTTPS-PORT) (*STORM-CONF* LOGVIEWER-PORT))
                            "logLink" (supervisor-log-link (name scheme) host)) (:callback m))))
  (GET "/api/v1/topology/summary" [:as {:keys [cookies servlet-request]} & m]
    (mark! ui:num-all-topologies-summary-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (json-response (all-topologies-summary) (:callback m)))
  (GET "/api/v1/topology-workers/:id" [:as {:keys [cookies servlet-request scheme]} id & m]
    (mark! ui:num-topology-workers-page-httpRequests)
    (populate-context! servlet-request)
    (let [id (url-decode id)]
      (json-response {"hostPortList" (worker-host-port id)
                      "scheme" (name scheme)
                      "logviewerPort" (if (= scheme :https) (*STORM-CONF* LOGVIEWER-HTTPS-PORT) (*STORM-CONF* LOGVIEWER-PORT))}
        (:callback m))))
  (GET "/api/v1/topology/:id" [:as {:keys [cookies servlet-request scheme]} id & m]
    (mark! ui:num-topology-page-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (let [user (get-user-name servlet-request)]
      (json-response (topology-page id (:window m) (check-include-sys? (:sys m)) user (= scheme :https)) (:callback m))))
  (GET "/api/v1/topology/:id/visualization-init" [:as {:keys [cookies servlet-request]} id & m]
    (mark! ui:num-build-visualization-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (build-visualization id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/visualization" [:as {:keys [cookies servlet-request]} id & m]
    (mark! ui:num-mk-visualization-data-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (mk-visualization-data id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/component/:component" [:as {:keys [cookies servlet-request scheme]} id component & m]
    (mark! ui:num-component-page-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (let [user (get-user-name servlet-request)]
      (json-response (component-page id component (:window m) (check-include-sys? (:sys m)) user (= scheme :https)) (:callback m))))
  (GET "/api/v1/topology/:id/logconfig" [:as {:keys [cookies servlet-request]} id & m]
    (mark! ui:num-log-config-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopologyConf" (topology-config id))
    (json-response (log-config id) (:callback m)))
  (POST "/api/v1/topology/:id/activate" [:as {:keys [cookies servlet-request]} id & m]
    (mark! ui:num-activate-topology-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "activate" (topology-config id))
    (with-nimbus nimbus
       (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (get-topology-info nimbus id))
            name (.get_name tplg)]
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (json-response (topology-op-response id "activate") (m "callback")))
  (POST "/api/v1/topology/:id/deactivate" [:as {:keys [cookies servlet-request]} id & m]
    (mark! ui:num-deactivate-topology-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "deactivate" (topology-config id))
    (with-nimbus nimbus
        (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (get-topology-info nimbus id))
            name (.get_name tplg)]
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (json-response (topology-op-response id "deactivate") (m "callback")))
  (POST "/api/v1/topology/:id/rebalance/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time & m]
    (mark! ui:num-topology-op-response-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "rebalance" (topology-config id))
    (with-nimbus nimbus
      (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (get-topology-info nimbus id))
            name (.get_name tplg)
            rebalance-options (m "rebalanceOptions")
            options (RebalanceOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (if (and (not-nil? rebalance-options) (contains? rebalance-options "numWorkers"))
          (.set_num_workers options (Integer/parseInt (.toString (rebalance-options "numWorkers")))))
        (if (and (not-nil? rebalance-options) (contains? rebalance-options "executors"))
          (doseq [keyval (rebalance-options "executors")]
            (.put_to_num_executors options (key keyval) (Integer/parseInt (.toString (val keyval))))))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (json-response (topology-op-response id "rebalance") (m "callback")))
  (POST "/api/v1/topology/:id/kill/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time & m]
    (mark! ui:num-topology-op-response-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "killTopology" (topology-config id))
    (with-nimbus nimbus
      (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (get-topology-info nimbus id))
            name (.get_name tplg)
            options (KillOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (json-response (topology-op-response id "kill") (m "callback")))
  (POST "/api/v1/topology/:id/logconfig" [:as {:keys [body cookies servlet-request]} id & m]
    (mark! ui:num-topology-op-response-httpRequests)
    (populate-context! servlet-request)
    (assert-authorized-user "setLogConfig" (topology-config id))
    (with-nimbus nimbus
      (let [log-config-json (from-json (slurp body))
            named-loggers (.get log-config-json "namedLoggerLevels")
            new-log-config (LogConfig.)]
        (doseq [[key level] named-loggers]
            (let [logger-name (str key)
                  target-level (.get level "target_level")
                  timeout (or (.get level "timeout") 0)
                  named-logger-level (LogLevel.)]
              ;; if target-level is nil, do not set it, user wants to clear
              (log-message "The target level for " logger-name " is " target-level)
              (if (nil? target-level)
                (do
                  (.set_action named-logger-level LogLevelAction/REMOVE)
                  (.unset_target_log_level named-logger-level))
                (do
                  (.set_action named-logger-level LogLevelAction/UPDATE)
                  ;; the toLevel here ensures the string we get is valid
                  (.set_target_log_level named-logger-level (.name (Level/toLevel target-level)))
                  (.set_reset_log_level_timeout_secs named-logger-level timeout)))
              (log-message "Adding this " logger-name " " named-logger-level " to " new-log-config)
              (.put_to_named_logger_level new-log-config logger-name named-logger-level)))
        (log-message "Setting topology " id " log config " new-log-config)
        (.setLogConfig nimbus id new-log-config)
        (json-response (log-config id) (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/start/:host-port/:timeout"
       [:as {:keys [servlet-request scheme]} id host-port timeout & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))

         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (+ (System/currentTimeMillis) (* 60000 (Long. timeout)))
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JPROFILE_STOP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port
                           "timeout" timeout
                           "dumplink" (worker-dump-link
                                       scheme
                                       host
                                       port
                                       id)}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/stop/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp 0
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JPROFILE_STOP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))
  
  (GET "/api/v1/topology/:id/profiling/dumpprofile/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JPROFILE_DUMP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/dumpjstack/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JSTACK_DUMP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/restartworker/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JVM_RESTART)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))
       
  (GET "/api/v1/topology/:id/profiling/dumpheap/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (with-nimbus nimbus
         (let [user (get-user-name servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user "setWorkerProfiler" (topology-config id)))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JMAP_DUMP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))
  
  (GET "/" [:as {cookies :cookies}]
    (mark! ui:num-main-page-httpRequests)
    (resp/redirect "/index.html"))
  
  ;; TODO: remove before going to the Apache
  (GET "/user.html" req
    (resp/redirect (str "owner.html?" (:query-string req))))

  (route/resources "/")
  (route/not-found "Page not found"))

(defn catch-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (json-response (exception->json ex) ((:query-params request) "callback") :status 500)))))

(defn app [https-port]
  (let [ns-redirect (non-secure-redirect https-port)]
    (handler/site (-> main-routes
                      (wrap-multipart-params)
                      (wrap-reload '[backtype.storm.ui.core])
                      nocache-middleware
                      (metrics-middleware ui:num-web-requests)
                      catch-errors
                      ns-redirect))))

(defn start-server!
  []
  (try
    (let [conf *STORM-CONF*
          header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filters-confs [{:filter-class (conf UI-FILTER)
                          :filter-params (conf UI-FILTER-PARAMS)}]
          https-port (conf UI-HTTPS-PORT)
          https-ks-path (conf UI-HTTPS-KEYSTORE-PATH)
          https-ks-password (conf UI-HTTPS-KEYSTORE-PASSWORD)
          https-ks-type (conf UI-HTTPS-KEYSTORE-TYPE)
          https-key-password (conf UI-HTTPS-KEY-PASSWORD)
          https-ts-path (conf UI-HTTPS-TRUSTSTORE-PATH)
          https-ts-password (conf UI-HTTPS-TRUSTSTORE-PASSWORD)
          https-ts-type (conf UI-HTTPS-TRUSTSTORE-TYPE)
          https-want-client-auth (conf UI-HTTPS-WANT-CLIENT-AUTH)
          https-need-client-auth (conf UI-HTTPS-NEED-CLIENT-AUTH)]
      (if-not (local-mode? conf)
        (redirect-stdio-to-slf4j!))

      (start-metrics-reporters conf)
      (storm-run-jetty {:port (conf UI-PORT)
                        :host (conf UI-HOST)
                        :https-port https-port
                        :configurator (fn [server]
                                        (config-ssl server
                                                    https-port
                                                    https-ks-path
                                                    https-ks-password
                                                    https-ks-type
                                                    https-key-password
                                                    https-ts-path
                                                    https-ts-password
                                                    https-ts-type
                                                    https-need-client-auth
                                                    https-want-client-auth)
                                        (doseq [connector (.getConnectors server)]
                                          (.setRequestHeaderSize connector header-buffer-size))
                                        (config-filter server (app https-port) filters-confs))}))
   (catch Exception ex
     (log-error ex))))

(defn -main
  []
  (log-message "Starting ui server for storm version '" STORM-VERSION "'")
  (start-server!))
