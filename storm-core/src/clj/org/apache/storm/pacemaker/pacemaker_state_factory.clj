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

(ns org.apache.storm.pacemaker.pacemaker-state-factory
  (:require [org.apache.storm.pacemaker pacemaker]
            [backtype.storm.cluster-state [zookeeper-state-factory :as zk-factory]]
            [backtype.storm
             [config :refer :all]
             [cluster :refer :all]
             [log :refer :all]
             [timer :refer :all]
             [converter :refer :all]
             [util :as util]])
  (:import [backtype.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType HBMessage HBMessageData HBPulse
            ClusterWorkerHeartbeat]
           [backtype.storm.cluster_state zookeeper_state_factory]
           [backtype.storm.cluster ClusterState]
           [backtype.storm.utils Utils]
           [org.apache.storm.pacemaker PacemakerClient])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

(def pacemaker-client-pool (atom []))

(defn- maybe-deserialize
  [ser clazz]
  (when ser
    (Utils/deserialize ser clazz)))

(defn clojurify-details [details]
  (if details
    (clojurify-zk-worker-hb (maybe-deserialize details ClusterWorkerHeartbeat))))

(defn get-wk-hb-time-secs-pair
  [details-set]
  (into []
    (for [details details-set]
      (let [_ (log-debug "details" details)
            wk-hb (if details
                    (clojurify-details details))
            time-secs (:time-secs wk-hb)]
        [time-secs details]))))

;; So we can mock the client for testing
(defn makeClientPool [conf]
  (dosync
    (let [servers (conf PACEMAKER-SERVERS)]
      (reset! pacemaker-client-pool (for [host servers]
                                      (try
                                        (PacemakerClient. conf host)
                                        (catch Exception exp
                                          (log-message "Error attempting connection to host" host))))))))

(defn makeZKState [conf auth-conf acls context]
  (.mkState (zookeeper_state_factory.) conf auth-conf acls context))

(def max-retries 10)

(defn- delete-worker-hb  [path pacemaker-client]
  (util/retry-on-exception
    max-retries
    "delete-worker-hb"
    #(let [response
           (.send pacemaker-client
                  (HBMessage. HBServerMessageType/DELETE_PATH
                              (HBMessageData/path path)))]
       (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
         :ok
         (throw (HBExecutionException. "Invalid Response Type"))))))

(defn- get-worker-hb [path pacemaker-client]
  (util/retry-on-exception
    max-retries
    "get-worker-hb"
    #(let [response (.send pacemaker-client
                           (HBMessage. HBServerMessageType/GET_PULSE
                                       (HBMessageData/path path)))]
       (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
         (try
           (.get_details (.get_pulse (.get_data response)))
           (catch Exception e
             (throw (HBExecutionException. (.toString e)))))
         (throw (HBExecutionException. "Invalid Response Type"))))))

(defn- get-worker-hb-children [path pacemaker-client]
  (util/retry-on-exception
    max-retries
    "get_worker_hb_children"
    #(let [response
           (.send pacemaker-client
                  (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                              (HBMessageData/path path)))]
       (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
         (try
           (into [] (.get_pulseIds (.get_nodes (.get_data response))))
           (catch Exception e
             (throw (HBExecutionException. (.toString e)))))
         (throw (HBExecutionException. "Invalid Response Type"))))))

(defn delete-stale-heartbeats []
  (doseq [pacemaker-client @pacemaker-client-pool]
    (if (not (nil? pacemaker-client))
      (let [storm-ids (get-worker-hb-children WORKERBEATS-SUBTREE pacemaker-client)
          _ (log-debug "storm ids" storm-ids)]
        (doseq [id storm-ids]
          (let[hb-paths (get-worker-hb-children
                          (str WORKERBEATS-SUBTREE "/" id) pacemaker-client)
               _ (log-debug "hb-paths" hb-paths)]
            (doseq [path hb-paths]
              (let [wk-hb (clojurify-details (get-worker-hb path pacemaker-client))
                    _ (log-debug "wk-hb" wk-hb)]
                (when (and wk-hb (> 60 (- (util/current-time-secs) (:time-secs wk-hb))))
                  (log-message "deleting" wk-hb "path" path)
                  (delete-worker-hb path pacemaker-client))))))))))

(defn launch-client-pool-refresh-thread [conf]
  (let [timer (mk-timer :kill-fn (fn [t]
                                   (log-error t "Error when processing event")
                                   (util/exit-process! 20 "Error when processing an event")
                                   ))]
    (schedule-recurring timer
                        0
                        120 ;; do we want to change this into a config
                        (fn []
                          (makeClientPool conf)))))

(defn launch-cleanup-hb-thread [conf]
  (let [timer (mk-timer :kill-fn (fn [t]
                                   (log-error t "Error when processing event")
                                   (util/exit-process! 20 "Error when processing an event")
                                   ))]
    (schedule-recurring timer
                        0
                        120 ;; do we want to change this into a config
                        (fn []
                          (delete-stale-heartbeats)))))

(defn -mkState [this conf auth-conf acls context]
  (let [zk-state (makeZKState conf auth-conf acls context)
        _ (launch-client-pool-refresh-thread conf)
        _ (launch-cleanup-hb-thread conf)
        servers (conf PACEMAKER-SERVERS)
        num-servers (.length servers)]

    (reify
      ClusterState
      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set_ephemeral_node [this path data acls] (.set_ephemeral_node zk-state path data acls))
      (create_sequential [this path data acls] (.create_sequential zk-state path data acls))
      (set_data [this path data acls] (.set_data zk-state path data acls))
      (delete_node [this path] (.delete_node zk-state path))
      (get_data [this path watch?] (.get_data zk-state path watch?))
      (get_data_with_version [this path watch?] (.get_data_with_version zk-state path watch?))
      (get_version [this path watch?] (.get_version zk-state path watch?))
      (get_children [this path watch?] (.get_children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (node_exists [this path watch?] (.node_exists zk-state path watch?))

      (set_worker_hb [this path data acls]
        (util/retry-on-exception
         max-retries
         "set_worker_hb"
         #(let [response
                ;; connecting to a single client, might want to
                ;; remove once reimplemented
                (.send (nth @pacemaker-client-pool 0)
                       (HBMessage. HBServerMessageType/SEND_PULSE
                                   (HBMessageData/pulse
                                    (doto (HBPulse.)
                                      (.set_id path)
                                      (.set_details data)))))]
            (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
              :ok
              (throw (HBExecutionException. "Invalid Response Type"))))))

      (delete_worker_hb [this path]
        ;; connecting to a single client, might want to
        ;; remove once reimplemented
        (delete-worker-hb path (nth @pacemaker-client-pool 0)))

      ;; aggregating worker heartbeat details
      (get_worker_hb [this path watch?]
        (let [count 0
              details-set (try
                            (into #{}
                              (for [pacemaker-client @pacemaker-client-pool]
                                (get-worker-hb path pacemaker-client)))
                                (catch Exception e
                                  ;; try for other servers, if it does not connect to any
                                  ;; of the servers throw exception
                                  (if (= (inc count) num-servers)
                                    (throw e))))
              _ (log-debug "details set" details-set)
              wk-hb-time-secs-pair (get-wk-hb-time-secs-pair details-set)
              sorted-map (into {} (sort-by first wk-hb-time-secs-pair))]
          (last (vals sorted-map))))

      ;; aggregating worker heartbeat children across all servers
      (get_worker_hb_children [this path watch?]
        (let [count 0
              hb-paths (try
                         (into []
                           (for [pacemaker-client @pacemaker-client-pool]
                             (get-worker-hb-children path pacemaker-client)))
                         (catch Exception e
                           ;; try for other servers, if it does not connect to any
                           ;; of the servers throw exception
                           (if (= (inc count) num-servers)
                             (throw e))))]
            (into[]
              (set (flatten hb-paths)))))

      (close [this]
        (.close zk-state)
        (for [pacemaker-client @pacemaker-client-pool]
          (.close pacemaker-client))))))