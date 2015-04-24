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

(ns org.apache.storm.pacemaker.pacemaker
  (:import [com.twitter.finagle Service]
           [org.apache.storm.pacemaker PacemakerServerFactory]
           [java.util.concurrent ConcurrentHashMap ThreadPoolExecutor TimeUnit LinkedBlockingDeque]
           [backtype.storm.generated
            HBAuthorizationException HBExecutionException HBNodes HBRecords
            HBServerMessageType Message MessageData Pulse])
  (:require [finagle-clojure.future-pool :as future-pool]
            [finagle-clojure.futures :as futures])
  (:use [clojure.string :only [replace-first split]]
        [backtype.storm log config])
  (:gen-class))

;; This is the old Thrift service that this server is emulating.
;  void createPath(1: string path) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  bool exists(1: string path) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  void sendPulse(1: Pulse pulse) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  HBRecords getAllPulseForPath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  HBNodes getAllNodesForPath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  Pulse getPulse(1: string id) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  void deletePath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
;  void deletePulseId(1: string id) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);

(defn hb-data [conf]
  (ConcurrentHashMap.))

(defn create-path [^String path heartbeats]
  (Message. HBServerMessageType/CREATE_PATH_RESPONSE nil))

(defn exists [^String path heartbeats]
  (let [it-does (.containsKey heartbeats path)]
    (log-debug (str "Checking if path [" path "] exists..." it-does "."))
    (Message. HBServerMessageType/EXISTS_RESPONSE
              (MessageData/boolval it-does))))

(defn send-pulse [^Pulse pulse heartbeats]
  (let [id (.get_id pulse)
        details (.get_details pulse)]
    (log-debug (str "Saving Pulse for id [" id "] data [" + (str details) "]."))
    (.put heartbeats id details)
    (Message. HBServerMessageType/SEND_PULSE_RESPONSE nil)))

(defn get-all-pulse-for-path [^String path heartbeats]
  (Message. HBServerMessageType/GET_ALL_PULSE_FOR_PATH_RESPONSE nil))

(defn get-all-nodes-for-path [^String path ^ConcurrentHashMap heartbeats]
    (log-debug "List all nodes for path " path)
    (Message. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE
              (MessageData/nodes
               (HBNodes. (distinct (for [k (.keySet heartbeats)
                                         :let [trimmed-k (second (split (replace-first k path "") #"/"))]
                                         :when (and
                                                (not (nil? trimmed-k))
                                                (= (.indexOf k path) 0))]
                                     trimmed-k))))))

(defn get-pulse [^String path heartbeats]
  (let [details (.get heartbeats path)]
    (log-debug (str "Getting Pulse for path [" path "]...data " (str details) "]."))
    (Message. HBServerMessageType/GET_PULSE_RESPONSE
              (MessageData/pulse
               (doto (Pulse. ) (.set_id path) (.set_details details))))))

(defn delete-pulse-id [^String path heartbeats]
  (log-debug (str "Deleting Pulse for id [" path "]."))
  (.remove heartbeats path)
  (Message. HBServerMessageType/DELETE_PULSE_ID_RESPONSE nil))

(defn delete-path [^String path heartbeats]
  (let [prefix (if (= \/ (last path)) path (str path "/"))]
    (doseq [k (.keySet heartbeats)
            :when (= (.indexOf k prefix) 0)]
      (delete-pulse-id k heartbeats)))
  (Message. HBServerMessageType/DELETE_PATH_RESPONSE nil))

(defn mk-service [conf]
  (let [min (conf PACEMAKER-BASE-THREADS)
        max (conf PACEMAKER-MAX-THREADS)
        mins-wait (conf PACEMAKER-THREAD-TIMEOUT)
        _ (log-message "Starting with " min " base threads.")
        _ (log-message "Will spawn up to " max " worker threads.")
        _ (log-message "Keep-alive for idle workers (Minutes): " mins-wait)
        heartbeats ^ConcurrentHashMap (hb-data conf)
        pool (future-pool/future-pool
              (ThreadPoolExecutor. min max
                                   mins-wait TimeUnit/MINUTES
                                   (LinkedBlockingDeque.)))]
    (proxy [Service] []
      (apply [^Message request]
        (future-pool/run pool
          (condp = (.get_type request)
            HBServerMessageType/CREATE_PATH            (create-path (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/EXISTS                 (exists (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/SEND_PULSE             (send-pulse (.get_pulse (.get_data request)) heartbeats)
            HBServerMessageType/GET_ALL_PULSE_FOR_PATH (get-all-pulse-for-path (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/GET_ALL_NODES_FOR_PATH (get-all-nodes-for-path (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/GET_PULSE              (get-pulse (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/DELETE_PATH            (delete-path (.get_path (.get_data request)) heartbeats)
            HBServerMessageType/DELETE_PULSE_ID        (delete-pulse-id (.get_path (.get_data request)) heartbeats)
            (.get_type request)                        (log-message "Got Type: " (.get_type request))))))))
    
(defn launch-server! []
  (log-message "Starting Server.")
  (let [conf (read-storm-config)]
    (PacemakerServerFactory/makeServer
     "HeartBeat Server"
     (conf PACEMAKER-PORT)
     (mk-service conf))))

(defn -main []
  (launch-server!))
