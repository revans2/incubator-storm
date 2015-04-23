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
  (:use ring.middleware.reload)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log stats])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID ACKER-INIT-STREAM-ID
                                              ACKER-ACK-STREAM-ID ACKER-FAIL-STREAM-ID system-id?]]])
  (:use [clojure.string :only [blank? lower-case trim]])
  (:use [clojure.set :only [intersection]])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions GetInfoOptions NumErrorsChoice TopologyPageInfo
            TopologyStats BoltAggregateStats SpoutAggregateStats
            LogConfig LogLevel LogLevelAction])
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [java.io File])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [backtype.storm [thrift :as thrift]])
  (:import [org.apache.commons.lang StringEscapeUtils])
  (:import [org.apache.logging.log4j Level])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))
(def igroup-mapper (AuthUtils/GetGroupMappingServiceProviderPlugin *STORM-CONF*))

(defmacro with-nimbus
  [nimbus-sym & body]
  `(thrift/with-nimbus-connection
     [~nimbus-sym (*STORM-CONF* NIMBUS-HOST) (*STORM-CONF* NIMBUS-THRIFT-PORT)]
     ~@body))

(defn user-groups
  [user]
  (if (blank? user) [] (.getGroups igroup-mapper user)))

(defn authorized-ui-user?
  [user conf topology-conf]
  (let [groups (user-groups user)
        ui-users (concat (conf UI-USERS)
                         (conf NIMBUS-ADMINS)
                         (topology-conf UI-USERS)
                         (topology-conf TOPOLOGY-USERS))
        ui-groups (concat (conf UI-GROUPS)
                         (topology-conf UI-GROUPS)
                         (topology-conf TOPOLOGY-GROUPS))]
    (or (blank? (conf UI-FILTER))
        (and (not (blank? user))
          (or (some #(= % user) ui-users)
            (< 0 (.size (intersection (set groups) (set ui-groups)))))))))

(defn assert-authorized-ui-user
  [user conf topology-conf]
  (if (not (authorized-ui-user? user conf topology-conf))
    ;;TODO need a better exception here so the UI can appear better
    (throw (RuntimeException. (str "User " user " is not authorized.")))))

(defn- ui-actions-enabled?
  []
  (= "true" (lower-case (*STORM-CONF* UI-ACTIONS-ENABLED))))

(defn assert-authorized-topology-user
  [user]
  ;;TODO eventually we will want to use the Authorizatin handler from nimbus, but for now
  ;; Disable the calls conditionally
  (if (not (ui-actions-enabled?))
    ;;TODO need a better exception here so the UI can appear better
    (throw (RuntimeException. (str "Topology actions for the UI have been disabled")))))

(defn read-storm-version
  "Returns a string containing the Storm version or 'Unknown'."
  []
  (let [storm-home (System/getProperty "storm.home")
        release-path (format "%s/RELEASE" storm-home)
        release-file (File. release-path)]
    (if (and (.exists release-file) (.isFile release-file))
      (trim (slurp release-path))
      "Unknown")))

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

(defn worker-log-link [host port topology-id]
  (let [fname (logs-filename topology-id port)]
    (url-format (str "http://%s:%s/log?file=%s")
          host (*STORM-CONF* LOGVIEWER-PORT) fname)))

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

(defn topology-action-button
  [id name action command is-wait default-wait enabled]
  [:input {:type "button"
           :value action
           (if enabled :enabled :disabled) ""
           :onclick (str "confirmAction('"
                         (StringEscapeUtils/escapeJavaScript id) "', '"
                         (StringEscapeUtils/escapeJavaScript name) "', '"
                         command "', " is-wait ", " default-wait ")")}])

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

(defn mk-visualization-data
  [id window include-sys? user]
  (with-nimbus
    nimbus
    (let [topology-conf (from-json 
                          (.getTopologyConf ^Nimbus$Client nimbus id))
          _ (assert-authorized-ui-user user *STORM-CONF* topology-conf)
          window (if window window ":all-time")
          topology (.getTopology ^Nimbus$Client nimbus id)
          spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          summ (->> (doto
                      (GetInfoOptions.)
                      (.set_num_err_choice NumErrorsChoice/NONE))
                    (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
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
        total-tasks (->> (.get_topologies summ)
                         (map #(.get_num_tasks ^TopologySummary %))
                         (reduce +))
        total-executors (->> (.get_topologies summ)
                             (map #(.get_num_executors ^TopologySummary %))
                             (reduce +))]
       {"user" user
        "stormVersion" (read-storm-version)
        "nimbusUptime" (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
        "supervisors" (count sups)
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
   {"supervisors"
    (for [^SupervisorSummary s summs]
      {"id" (.get_supervisor_id s)
       "host" (.get_host s)
       "uptime" (pretty-uptime-sec (.get_uptime_secs s))
       "slotsTotal" (.get_num_workers s)
       "slotsUsed" (.get_num_used_workers s)})}))

(defn all-topologies-summary
  ([]
   (with-nimbus
     nimbus
     (all-topologies-summary
       (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"topologies"
    (for [^TopologySummary t summs]
      {"id" (.get_id t)
       "owner" (.get_owner t)
       "name" (.get_name t)
       "status" (.get_status t)
       "uptime" (pretty-uptime-sec (.get_uptime_secs t))
       "tasksTotal" (.get_num_tasks t)
       "workersTotal" (.get_num_workers t)
       "executorsTotal" (.get_num_executors t)
       "schedulerInfo" (.get_sched_status t)})}))

(defn topology-stats [window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [k (concat times [":all-time"])
          :let [disp ((display-map k) k)]]
      {"windowPretty" disp
       "window" k
       "emitted" (get-in stats [:emitted k])
       "transferred" (get-in stats [:transferred k])
       "completeLatency" (float-str (get-in stats [:complete-latencies k]))
       "acked" (get-in stats [:acked k])
       "failed" (get-in stats [:failed k])})))

(defn topology-summary [^TopologyInfo summ]
  (let [executors (.get_executors summ)
        workers (set (for [^ExecutorSummary e executors]
                       [(.get_host e) (.get_port e)]))]
      {"id" (.get_id summ)
       "owner" (.get_owner summ)
       "name" (.get_name summ)
       "status" (.get_status summ)
       "uptime" (pretty-uptime-sec (.get_uptime_secs summ))
       "tasksTotal" (sum-tasks executors)
       "workersTotal" (count workers)
       "executorsTotal" (count executors)
       "schedulerInfo" (.get_sched_status summ)}))

(defn spout-summary-json [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       {"windowPretty" disp
        "window" k
        "emitted" (get-in stats [:emitted k])
        "transferred" (get-in stats [:transferred k])
        "completeLatency" (float-str (get-in stats [:complete-latencies k]))
        "acked" (get-in stats [:acked k])
        "failed" (get-in stats [:failed k])})))

(defn exec-host-port
  [executors]
  (for [^ExecutorSummary e executors]
    {"host" (.get_host e)
     "port" (.get_port e)}))

(defn worker-host-port
  "Get the set of all worker host/ports"
  [id]
  (with-nimbus nimbus
    (distinct (exec-host-port (.get_executors (.getTopologyInfo nimbus id))))))

(defn build-visualization [id window include-sys? user]
  (with-nimbus nimbus
    (let [topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus id))
          _ (assert-authorized-ui-user user *STORM-CONF* topology-conf)
          window (if window window ":all-time")
          topology-info (->> (doto
                               (GetInfoOptions.)
                               (.set_num_err_choice NumErrorsChoice/ONE))
                             (.getTopologyInfoWithOpts ^Nimbus$Client nimbus
                                                       id))
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

(defn topology-page-old [id window include-sys? user]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          topology-info (->> (doto
                               (GetInfoOptions.)
                               (.set_num_err_choice NumErrorsChoice/ONE))
                             (.getTopologyInfoWithOpts ^Nimbus$Client nimbus
                                                       id))
          storm-topology (.getTopology ^Nimbus$Client nimbus id)
          topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus id))
          spout-executor-summaries (filter (partial spout-summary? storm-topology) (.get_executors topology-info))
          bolt-executor-summaries (filter (partial bolt-summary? storm-topology) (.get_executors topology-info))
          spout-comp-id->executor-summaries (group-by-comp spout-executor-summaries)
          bolt-comp-id->executor-summaries (group-by-comp bolt-executor-summaries)
          bolt-comp-id->executor-summaries (filter-key (mk-include-sys-fn include-sys?) bolt-comp-id->executor-summaries)
          name (.get_name topology-info)
          status (.get_status topology-info)
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)
          id->spout-spec (.get_spouts storm-topology)
          id->bolt (.get_bolts storm-topology)
          visualizer-data (visualization-data (merge (hashmap-to-persistent id->spout-spec)
                                                     (hashmap-to-persistent id->bolt))
                                              spout-comp-id->executor-summaries
                                              bolt-comp-id->executor-summaries
                                              window
                                              id)]
      (assert-authorized-ui-user user *STORM-CONF* topology-conf)
      (merge
       (topology-summary topology-info)
       {"user" user
        "window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "topologyStats" (topology-stats window (total-aggregate-stats spout-executor-summaries bolt-executor-summaries include-sys?))
        "spouts" (spout-comp id spout-comp-id->executor-summaries (.get_errors topology-info) window include-sys?)
        "bolts" (bolt-comp id bolt-comp-id->executor-summaries (.get_errors topology-info) window include-sys?)
        "visualizationTable" (stream-boxes visualizer-data)
        "uiActionsEnabled" (ui-actions-enabled?)}))))

(defn- spout-agg-stats-json
  "Returns a JSON representation of a sequence of spout aggregated statistics."
  [spout-agg-stats]
  (for [^SpoutAggregateStats s spout-agg-stats]
    {"spoutId" (.get_id s)
     "executors" (.get_num_executors s)
     "tasks" (.get_num_tasks s)
     "emitted" (.get_num_emitted s)
     "transferred" (.get_num_transferred s)
     "completeLatency" (float-str (.get_complete_latency s))
     "acked" (.get_num_acked s)
     "failed" (.get_num_failed s)
     "lastError" (error-subset (if-let [e (.get_last_error s)]
                                       (.get_error e)))}))

(defn- bolt-agg-stats-json
  "Returns a JSON representation of a sequence of bolt aggregated statistics."
  [bolt-agg-stats]
  (for [^BoltAggregateStats s bolt-agg-stats]
    {"boltId" (.get_id s)
     "executors" (.get_num_executors s)
     "tasks" (.get_num_tasks s)
     "emitted" (.get_num_emitted s)
     "transferred" (.get_num_transferred s)
     "capacity" (float-str (.get_capacity s))
     "executeLatency" (float-str (.get_execute_latency s))
     "executed" (.get_num_executed s)
     "processLatency" (float-str (.get_process_latency s))
     "acked" (.get_num_acked s)
     "failed" (.get_num_failed s)
     "lastError" (error-subset (if-let [e (.get_last_error s)]
                                       (.get_error e)))}))

(defn- unpack-topology-page-info
  "Unpacks the serialized object to data structures"
  [^TopologyPageInfo topo-info window]
  (let [^TopologyStats topo-stats (.get_topology_stats topo-info)
        spouts-stat->window->number
          {:emitted (.get_emitted topo-stats)
           :transferred (.get_transferred topo-stats)
           :complete-latencies (.get_complete_latencies topo-stats)
           :acked (.get_acked topo-stats)
           :failed (.get_failed topo-stats)}
        topo-stats (topology-stats window spouts-stat->window->number)]
    {"id" (.get_id topo-info)
     "owner" (.get_owner topo-info)
     "name" (.get_name topo-info)
     "status" (.get_status topo-info)
     "uptime" (pretty-uptime-sec (.get_uptime_secs topo-info))
     "tasksTotal" (.get_num_tasks topo-info)
     "workersTotal" (.get_num_workers topo-info)
     "executorsTotal" (.get_num_executors topo-info)
     "schedulerInfo" (.get_sched_status topo-info)
     "topologyStats" topo-stats
     "spouts" (spout-agg-stats-json (.get_spout_agg_stats topo-info))
     "bolts" (bolt-agg-stats-json (.get_bolt_agg_stats topo-info))
     "configuration" (.get_topology_conf topo-info)}))

(defn topology-page [id window include-sys? user]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          topo-page-info (.getTopologyPageInfo ^Nimbus$Client nimbus
                                               id
                                               window
                                               include-sys?)
          topology-conf (from-json (.get_topology_conf topo-page-info))
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)]
      (assert-authorized-ui-user user *STORM-CONF* topology-conf)
      (merge
       (unpack-topology-page-info topo-page-info window)
       {"user" user
        "window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "configuration" topology-conf
        "visualizationTable" []
        "uiActionsEnabled" (ui-actions-enabled?)}))))

(defn spout-output-stats
  [stream-summary window]
  (let [stream-summary (map-val swap-map-order
                                (swap-map-order stream-summary))]
    (for [[s stats] (stream-summary window)]
      {"stream" s
       "emitted" (nil-to-zero (:emitted stats))
       "transferred" (nil-to-zero (:transferred stats))
       "completeLatency" (float-str (:complete-latencies stats))
       "acked" (nil-to-zero (:acked stats))
       "failed" (nil-to-zero (:failed stats))})))

(defn spout-executor-stats
  [topology-id executors window include-sys?]
  (for [^ExecutorSummary e executors
        :let [stats (.get_stats e)
              stats (if stats
                      (-> stats
                          (aggregate-spout-stats include-sys?)
                          aggregate-spout-streams
                          swap-map-order
                          (get window)))]]
    {"id" (pretty-executor-info (.get_executor_info e))
     "uptime" (pretty-uptime-sec (.get_uptime_secs e))
     "host" (.get_host e)
     "port" (.get_port e)
     "emitted" (nil-to-zero (:emitted stats))
     "transferred" (nil-to-zero (:transferred stats))
     "completeLatency" (float-str (:complete-latencies stats))
     "acked" (nil-to-zero (:acked stats))
     "failed" (nil-to-zero (:failed stats))
     "workerLogLink" (worker-log-link (.get_host e) (.get_port e) topology-id)}))

(defn component-errors
  [errors-list]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"componentErrors"
     (for [^ErrorInfo e errors]
       {"time" (date-str (.get_error_time_secs e))
        "error" (.get_error e)})}))

(defn spout-stats
  [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-spout-stats include-sys?))
        summary (-> stream-summary aggregate-spout-streams)]
    {"spoutSummary" (spout-summary-json
                      (.get_id topology-info) component summary window)
     "outputStats" (spout-output-stats stream-summary window)
     "executorStats" (spout-executor-stats (.get_id topology-info)
                                           executors window include-sys?)}))

(defn bolt-summary
  [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [k (concat times [":all-time"])
          :let [disp ((display-map k) k)]]
      {"window" k
       "windowPretty" disp
       "emitted" (get-in stats [:emitted k])
       "transferred" (get-in stats [:transferred k])
       "executeLatency" (float-str (get-in stats [:execute-latencies k]))
       "executed" (get-in stats [:executed k])
       "processLatency" (float-str (get-in stats [:process-latencies k]))
       "acked" (get-in stats [:acked k])
       "failed" (get-in stats [:failed k])})))

(defn bolt-output-stats
  [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:emitted :transferred])
                           swap-map-order)]
    (for [[s stats] stream-summary]
      {"stream" s
        "emitted" (nil-to-zero (:emitted stats))
        "transferred" (nil-to-zero (:transferred stats))})))

(defn bolt-input-stats
  [stream-summary window]
  (let [stream-summary
        (-> stream-summary
            swap-map-order
            (get window)
            (select-keys [:acked :failed :process-latencies
                          :executed :execute-latencies])
            swap-map-order)]
    (for [[^GlobalStreamId s stats] stream-summary]
      {"component" (.get_componentId s)
       "stream" (.get_streamId s)
       "executeLatency" (float-str (:execute-latencies stats))
       "processLatency" (float-str (:execute-latencies stats))
       "executed" (nil-to-zero (:executed stats))
       "acked" (nil-to-zero (:acked stats))
       "failed" (nil-to-zero (:failed stats))})))

(defn bolt-executor-stats
  [topology-id executors window include-sys?]
  (for [^ExecutorSummary e executors
        :let [stats (.get_stats e)
              stats (if stats
                      (-> stats
                          (aggregate-bolt-stats include-sys?)
                          (aggregate-bolt-streams)
                          swap-map-order
                          (get window)))]]
    {"id" (pretty-executor-info (.get_executor_info e))
     "uptime" (pretty-uptime-sec (.get_uptime_secs e))
     "host" (.get_host e)
     "port" (.get_port e)
     "emitted" (nil-to-zero (:emitted stats))
     "transferred" (nil-to-zero (:transferred stats))
     "capacity" (float-str (nil-to-zero (compute-executor-capacity e)))
     "executeLatency" (float-str (:execute-latencies stats))
     "executed" (nil-to-zero (:executed stats))
     "processLatency" (float-str (:process-latencies stats))
     "acked" (nil-to-zero (:acked stats))
     "failed" (nil-to-zero (:failed stats))
     "workerLogLink" (worker-log-link (.get_host e) (.get_port e) topology-id)}))

(defn bolt-stats
  [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-bolt-stats include-sys?))
        summary (-> stream-summary aggregate-bolt-streams)]
    {"boltStats" (bolt-summary (.get_id topology-info) component summary window)
     "inputStats" (bolt-input-stats stream-summary window)
     "outputStats" (bolt-output-stats stream-summary window)
     "executorStats" (bolt-executor-stats
                       (.get_id topology-info) executors window include-sys?)}))

(defn component-page
  [topology-id component window include-sys? user]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus topology-id))
          type (component-type topology component)
          summs (component-task-summs summ topology component)
          spec (cond (= type :spout) (spout-stats window summ component summs include-sys?)
                     (= type :bolt) (bolt-stats window summ component summs include-sys?))
          errors (component-errors (get (.get_errors summ) component))]
      (assert-authorized-ui-user user *STORM-CONF* topology-conf)
      (merge
        {"user" user
         "id" component
         "name" (.get_name summ)
         "executors" (count summs)
         "tasks" (sum-tasks summs)
         "topologyId" topology-id
         "window" window
         "componentType" (name type)
         "windowHint" (window-hint window)}
       spec errors))))

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

(defn check-include-sys?
  [sys?]
  (if (or (nil? sys?) (= "false" sys?)) false true))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

(defroutes main-routes
  (GET "/api/v1/cluster/configuration" []
       (cluster-configuration))
  (GET "/api/v1/cluster/schedulerConfiguration" []
       (scheduler-configuration))
  (GET "/api/v1/cluster/summary" [:as {:keys [cookies servlet-request]}]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (json-response (cluster-summary user))))
  (GET "/api/v1/history/summary" [:as {:keys [cookies servlet-request]}]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (json-response (topology-history-info user))))
  (GET "/api/v1/supervisor/summary" []
       (json-response (assoc (supervisor-summary)
                             "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT))))
  (GET "/api/v1/topology/summary" []
       (json-response (all-topologies-summary)))
  (GET  "/api/v1/topology-workers/:id" [:as {:keys [cookies servlet-request]} id & m]
        (let [id (url-decode id)
              user (.getUserName http-creds-handler servlet-request)]
          (json-response {"hostPortList" (worker-host-port id)
                          "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)})))
  (GET  "/api/v1/topology/:id" [:as {:keys [cookies servlet-request]} id & m]
        (let [id (url-decode id)
              user (.getUserName http-creds-handler servlet-request)]
          (json-response (topology-page id (:window m) (check-include-sys? (:sys m)) user))))
  (GET "/api/v1/topology/:id/visualization-init" [:as {:keys [cookies servlet-request]} id & m]
        (let [id (url-decode id)
              user (.getUserName http-creds-handler servlet-request)]
          (json-response (build-visualization id (:window m) (check-include-sys? (:sys m)) user))))
  (GET "/api/v1/topology/:id/visualization" [:as {:keys [cookies servlet-request]} id & m]
        (let [id (url-decode id)
              user (.getUserName http-creds-handler servlet-request)]
          (json-response (mk-visualization-data id (:window m) (check-include-sys? (:sys m)) user))))
  (GET "/api/v1/topology/:id/component/:component" [:as {:keys [cookies servlet-request]} id component & m]
       (let [id (url-decode id)
             component (url-decode component)
             user (.getUserName http-creds-handler servlet-request)]
         (json-response (component-page id component (:window m) (check-include-sys? (:sys m)) user))))
  (GET "/api/v1/topology/:id/logconfig" [:as {:keys [cookies servlet-request]} id & m]
         (let [user (.getUserName http-creds-handler servlet-request)]
            (assert-authorized-topology-user user))
         (json-response (log-config id)))
  (POST "/api/v1/topology/:id/activate" [:as {:keys [cookies servlet-request]} id]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            user (.getUserName http-creds-handler servlet-request)]
        (assert-authorized-topology-user user)
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (resp/redirect (str "/api/v1/topology/" id)))

  (POST "/api/v1/topology/:id/deactivate" [:as {:keys [cookies servlet-request]} id]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            user (.getUserName http-creds-handler servlet-request)]
        (assert-authorized-topology-user user)
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (resp/redirect (str "/api/v1/topology/" id)))
  (POST "/api/v1/topology/:id/rebalance/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            options (RebalanceOptions.)
            user (.getUserName http-creds-handler servlet-request)]
        (assert-authorized-topology-user user)
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/v1/topology/" id)))
  (POST "/api/v1/topology/:id/kill/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            options (KillOptions.)
            user (.getUserName http-creds-handler servlet-request)]
        (assert-authorized-topology-user user)
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/v1/topology/" id)))

  (POST "/api/v1/topology/:id/logconfig" [:as {:keys [body cookies servlet-request]} id & m]
    (with-nimbus nimbus
      (let [user (.getUserName http-creds-handler servlet-request)
            _ (assert-authorized-topology-user user)
            log-config-json (from-json (slurp body))
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
        (json-response (log-config id)))))

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
        (json-response (exception->json ex) :status 500)))))

(def app
  (handler/site (-> main-routes
                    (wrap-reload '[backtype.storm.ui.core])
                    catch-errors)))

(defn start-server!
  []
  (try
    (let [conf *STORM-CONF*
          header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filters-confs [{:filter-class (conf UI-FILTER)
                          :filter-params (conf UI-FILTER-PARAMS)}]]
      (storm-run-jetty {:port (conf UI-PORT)
                        :configurator (fn [server]
                                        (doseq [connector (.getConnectors server)]
                                          (.setHeaderBufferSize connector header-buffer-size))
                                        (config-filter server app filters-confs))}))
   (catch Exception ex
     (log-error ex))))

(defn -main [] (start-server!))
