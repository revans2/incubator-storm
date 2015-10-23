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
        ring.middleware.multipart-params)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log converter stats])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID ACKER-INIT-STREAM-ID ACKER-ACK-STREAM-ID
                                              ACKER-FAIL-STREAM-ID mk-authorization-handler
                                              start-metrics-reporters]]])
  (:use [clojure.string :only [blank? lower-case trim split]])
  (:use [clojure.set :only [intersection]])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions GetInfoOptions NumErrorsChoice TopologyPageInfo
            TopologyStats CommonAggregateStats ComponentAggregateStats
            ComponentType BoltAggregateStats SpoutAggregateStats
            ExecutorAggregateStats SpecificAggregateStats ComponentPageInfo
            LogConfig LogLevel LogLevelAction])
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
(defmeter num-getTopologyInfo-calls)

(defmacro with-nimbus
  [nimbus-sym & body]
  `(let [context# (ReqContext/context)
         user# (if (.principal context#) (.getName (.principal context#)))]
    (thrift/with-nimbus-connection
       [~nimbus-sym (*STORM-CONF* NIMBUS-HOST) (*STORM-CONF* NIMBUS-THRIFT-PORT)]
       ~@body)))

(defn assert-authorized-user
  ([servlet-request op]
    (assert-authorized-user servlet-request op nil))
  ([servlet-request op topology-conf]
    (let [context (ReqContext/context)]
      (if http-creds-handler (.populateContext http-creds-handler context servlet-request))

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
    (if secure?
      (url-format "https://%s:%s/log?file=%s"
                  host
                  (*STORM-CONF* LOGVIEWER-HTTPS-PORT)
                  fname)
      (url-format "http://%s:%s/log?file=%s"
                  host
                  (*STORM-CONF* LOGVIEWER-PORT)
                  fname))))

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

(defn worker-dump-link [host port topology-id]
  (url-format "http://%s:%s/dumps/%s/%s"
              (url-encode host)
              (*STORM-CONF* LOGVIEWER-PORT)
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
   (mark! num-getTopologyInfo-calls)
   (.getTopologyInfo nimbus id))
  ([^Nimbus$Client nimbus id options]
   (mark! num-getTopologyInfo-calls)
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
                                (reduce +))]
       {"user" user
        "stormVersion" (str (VersionInfo/getVersion))
        "nimbusUptime" (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
        "nimbusUptimeSeconds" (.get_nimbus_uptime_secs summ)
        "supervisors" (count sups)
        "topologies" topologies
        "slotsTotal" total-slots
        "slotsUsed"  used-slots
        "slotsFree" free-slots
        "executorsTotal" total-executors
        "tasksTotal" total-tasks })))

(defn supervisor-summary
  ([]
   (with-nimbus nimbus
     (supervisor-summary
       (.get_supervisors (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   (let [bad-supervisors (filter #(= "unknown" (.get_host ^SupervisorSummary %)) summs)
         good-supervisors (filter #(not= "unknown" (.get_host ^SupervisorSummary %)) summs)]
     {"supervisors"
      (for [^SupervisorSummary s good-supervisors]
        {"id" (.get_supervisor_id s)
         "host" (.get_host s)
         "uptime" (pretty-uptime-sec (.get_uptime_secs s))
         "uptimeSeconds" (.get_uptime_secs s)
         "slotsTotal" (.get_num_workers s)
         "slotsUsed" (.get_num_used_workers s)
         "version" (.get_version s)})
      "has-bad-supervisors" (not (empty? bad-supervisors))
      "bad-supervisors"
      (for [^SupervisorSummary s bad-supervisors]
        {"id" (.get_supervisor_id s)
         "error" (.get_version s)})})))

(defn all-topologies-summary
  ([]
   (with-nimbus
     nimbus
     (all-topologies-summary
       (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"topologies"
    (for [^TopologySummary t summs]
      {
       "id" (.get_id t)
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
       "requestedMemOnHeap" (.get_requsted_memonheap t)
       "requestedMemOffHeap" (.get_requsted_memoffheap t)
       "requestedCpu" (.get_requsted_cpu t)
       "assignedMemOnHeap" (.get_assigned_memonheap t)
       "assignedMemOffHeap" (.get_assigned_memoffheap t)
       "assignedCpu" (.get_assigned_cpu t)})}))

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
   "failed" (.get_failed common-stats)})

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

(defn- unpack-topology-page-info
  "Unpacks the serialized object to data structures"
  [^TopologyPageInfo topo-info window]
  (let [id (.get_id topo-info)
        ^TopologyStats topo-stats (.get_topology_stats topo-info)
        stat->window->number
          {:emitted (.get_window_to_emitted topo-stats)
           :transferred (.get_window_to_transferred topo-stats)
           :complete-latencies (.get_window_to_complete_latencies_ms topo-stats)
           :acked (.get_window_to_acked topo-stats)
           :failed (.get_window_to_failed topo-stats)}
        topo-stats (topology-stats window stat->window->number)]
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
     "requestedMemOnHeap" (.get_requsted_memonheap t)
     "requestedMemOffHeap" (.get_requsted_memoffheap t)
     "requestedCpu" (.get_requsted_cpu t)
     "assignedMemOnHeap" (.get_assigned_memonheap t)
     "assignedMemOffHeap" (.get_assigned_memoffheap t)
     "assignedCpu" (.get_assigned_cpu t)
     "topologyStats" topo-stats
     "spouts" (map (partial comp-agg-stats-json id)
                   (.get_id_to_spout_agg_stats topo-info))
     "bolts" (map (partial comp-agg-stats-json id)
                  (.get_id_to_bolt_agg_stats topo-info))
     "configuration" (.get_topology_conf topo-info)}))

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

(defn topology-page [id window include-sys? user]
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
       (unpack-topology-page-info topo-page-info window)
       {"user" user
        "window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "configuration" topology-conf
        "visualizationTable" []}))))

(defn component-errors
  [errors-list topology-id]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"componentErrors"
     (for [^ErrorInfo e errors]
       {"time" (* 1000 (long (.get_error_time_secs e)))
        "errorHost" (.get_host e)
        "errorPort"  (.get_port e)
        "errorWorkerLogLink"  (worker-log-link (.get_host e) (.get_port e) topology-id)
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
    (component-errors (.get_errors info) topology-id)))

(defmethod unpack-component-page-info ComponentType/SPOUT
  [^ComponentPageInfo info topology-id window include-sys? secure?]
  (merge
    {"spoutSummary" (map unpack-comp-agg-stat (.get_window_to_stats info))
     "outputStats" (map unpack-comp-output-stat (.get_sid_to_output_stats info))
     "executorStats" (map (partial unpack-comp-exec-stat topology-id secure?)
                          (.get_exec_stats info))}
    (component-errors (.get_errors info) topology-id)))

(defn get-active-profile-actions
  [nimbus topology-id component]
  (let [profile-actions  (.getComponentPendingProfileActions nimbus
                                               topology-id
                                               component
                                 ProfileAction/JPROFILE_STOP)
        latest-profile-actions (map clojurify-profile-request profile-actions)
        active-actions (map (fn [profile-action]
                              {"host" (:host profile-action)
                               "port" (str (:port profile-action))
                               "dumplink" (worker-dump-link (:host profile-action) (str (:port profile-action)) topology-id)
                               "timestamp" (str (- (:timestamp profile-action) (System/currentTimeMillis)))})
                            latest-profile-actions)]
    (log-message "Latest-active actions are: " (pr active-actions))
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
       "topologyId" topology-id
       "encodedTopologyId" (url-encode topology-id)
       "window" window
       "componentType" (-> comp-page-info .get_component_type str lower-case)
       "windowHint" window-hint
       "profilerActive" (get-active-profile-actions nimbus topology-id component)))))
    
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

(defroutes main-routes
  (GET "/api/v1/cluster/configuration" [& m]
       (json-response (cluster-configuration)
                      (:callback m) :serialize-fn identity))
  (GET "/api/v1/cluster/schedulerConfiguration" [& m]
       (json-response (scheduler-configuration)
                      (:callback m) :serialize-fn identity))
  (GET "/api/v1/cluster/summary" [:as {:keys [cookies servlet-request]} & m]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (assert-authorized-user servlet-request "getClusterInfo")
         (json-response (assoc (cluster-summary user)
                          "jira-url" (*STORM-CONF* UI-PROJECT-JIRA-URL)
                          "central-log-url" (*STORM-CONF* UI-CENTRAL-LOGGING-URL)) (:callback m))))
  (GET "/api/v1/history/summary" [:as {:keys [cookies servlet-request]} & m]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (json-response (topology-history-info user) (:callback m))))
  (GET "/api/v1/supervisor/summary" [:as {:keys [cookies servlet-request]} & m]
       (assert-authorized-user servlet-request "getClusterInfo")
       (json-response (assoc (supervisor-summary)
                             "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)) (:callback m)))
  (GET "/api/v1/topology/summary" [:as {:keys [cookies servlet-request]} & m]
       (assert-authorized-user servlet-request "getClusterInfo")
       (json-response (all-topologies-summary) (:callback m)))
  (GET  "/api/v1/topology-workers/:id" [:as {:keys [cookies servlet-request]} id & m]
        (let [id (url-decode id)]
          (json-response {"hostPortList" (worker-host-port id)
                          "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)} (:callback m))))
  (GET  "/api/v1/topology/:id" [:as {:keys [cookies servlet-request]} id & m]
        (let [user (.getUserName http-creds-handler servlet-request)]
          (assert-authorized-user servlet-request "getTopology" (topology-config id))
          (json-response (topology-page id (:window m) (check-include-sys? (:sys m)) user) (:callback m))))
  (GET "/api/v1/topology/:id/visualization-init" [:as {:keys [cookies servlet-request]} id & m]
        (assert-authorized-user servlet-request "getTopology" (topology-config id))
        (json-response (build-visualization id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/visualization" [:as {:keys [cookies servlet-request]} id & m]
        (assert-authorized-user servlet-request "getTopology" (topology-config id))
        (json-response (mk-visualization-data id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/component/:component" [:as {:keys [cookies servlet-request scheme]} id component & m]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (assert-authorized-user servlet-request "getTopology" (topology-config id))
         (json-response (component-page id component (:window m) (check-include-sys? (:sys m)) user (= scheme :https)) (:callback m))))
(GET "/api/v1/topology/:id/logconfig" [:as {:keys [cookies servlet-request]} id & m]
       (assert-authorized-user servlet-request "getTopologyConf" (topology-config id))
       (json-response (log-config id) (:callback m)))
  (POST "/api/v1/topology/:id/activate" [:as {:keys [cookies servlet-request]} id & m]
    (assert-authorized-user servlet-request "activate" (topology-config id))
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
    (assert-authorized-user servlet-request "deactivate" (topology-config id))
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
    (assert-authorized-user servlet-request "rebalance" (topology-config id))
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
    (assert-authorized-user servlet-request "killTopology" (topology-config id))
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
    (assert-authorized-user servlet-request "setLogConfig" (topology-config id))
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
       [:as {:keys [servlet-request]} id host-port timeout & m]
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))

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
                                       host
                                       port
                                       id)}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/stop/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))
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
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))
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
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))
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
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))
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
       (with-nimbus nimbus
         (let [user (.getUserName http-creds-handler servlet-request)
               topology-conf (from-json
                              (.getTopologyConf ^Nimbus$Client nimbus id))]
           (assert-authorized-user servlet-request "setWorkerProfiler" (topology-config id)))
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
       (resp/redirect "/index.html"))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn catch-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (json-response (exception->json ex) ((:query-params request) "callback") :status 500)))))


(def app
  (handler/site (-> main-routes
                    (wrap-multipart-params)
                    (wrap-reload '[backtype.storm.ui.core])
                    requests-middleware
                    catch-errors)))

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

      (start-metrics-reporters)
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
                                        (config-filter server app filters-confs))}))
   (catch Exception ex
     (log-error ex))))

(defn -main [] (start-server!))
