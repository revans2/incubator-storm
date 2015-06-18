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
  (:import [org.apache.storm.pacemaker PacemakerServer IServerMessageHandler]
           [java.util.concurrent ConcurrentHashMap ThreadPoolExecutor TimeUnit LinkedBlockingDeque]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util Date]
           [backtype.storm.generated
            HBAuthorizationException HBExecutionException HBNodes HBRecords
            HBServerMessageType HBMessage HBMessageData HBPulse])
  (:use [clojure.string :only [replace-first split]]
        [backtype.storm log config util])
  (:require [clojure.java.jmx :as jmx])
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


;; Stats Functions

(def sleep-seconds 5)

(defn- check-largest [stats size]
  (loop []
    (let [largest (.get (:largest stats))]
      (if (> size largest)
        (if (.compareAndSet (:largest stats) largest size)
          nil
          (recur))
        nil))))

(defn- report-stats [stats last-five]
  (loop []
      (let [count (.getAndSet (:count stats) 0)
            largest (.getAndSet (:largest stats) 0)]
        (log-message "Received " count " heartbeats in the last " sleep-seconds " second(s) with maximum size of " largest " bytes.")
        (dosync (ref-set last-five {:count count :largest largest})))
      (Thread/sleep (* 1000 sleep-seconds))
      (recur)))

;; JMX stuff

;(def last-five (ref {:count 0 :largest 0}))
(defn- register [last-five]
  (jmx/register-mbean
   (jmx/create-bean
    last-five)
   "org.apache.storm.pacemaker.pacemaker:stats=Stats_Last_5_Seconds"))
  
;; Pacemaker Functions

(defn hb-data [conf]
  (ConcurrentHashMap.))

(defn create-path [^String path heartbeats]
  (HBMessage. HBServerMessageType/CREATE_PATH_RESPONSE nil))

(defn exists [^String path heartbeats]
  (let [it-does (.containsKey heartbeats path)]
    (log-debug (str "Checking if path [" path "] exists..." it-does "."))
    (HBMessage. HBServerMessageType/EXISTS_RESPONSE
                (HBMessageData/boolval it-does))))

(defn send-pulse [^HBPulse pulse heartbeats pacemaker-stats]
  (let [id (.get_id pulse)
        details (.get_details pulse)]
    (log-debug (str "Saving Pulse for id [" id "] data [" + (str details) "]."))
    (.incrementAndGet (:count pacemaker-stats))
    (check-largest pacemaker-stats (alength details))
    (.put heartbeats id details)
    (HBMessage. HBServerMessageType/SEND_PULSE_RESPONSE nil)))

(defn get-all-pulse-for-path [^String path heartbeats]
  (HBMessage. HBServerMessageType/GET_ALL_PULSE_FOR_PATH_RESPONSE nil))

(defn get-all-nodes-for-path [^String path ^ConcurrentHashMap heartbeats]
  (log-debug "List all nodes for path " path)
  (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE
              (HBMessageData/nodes
               (HBNodes. (distinct (for [k (.keySet heartbeats)
                                         :let [trimmed-k (second (split (replace-first k path "") #"/"))]
                                         :when (and
                                                (not (nil? trimmed-k))
                                                (= (.indexOf k path) 0))]
                                     trimmed-k))))))

(defn get-pulse [^String path heartbeats]
  (let [details (.get heartbeats path)]
    (log-debug (str "Getting Pulse for path [" path "]...data " (str details) "]."))
    (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE
                (HBMessageData/pulse
                 (doto (HBPulse. ) (.set_id path) (.set_details details))))))

(defn delete-pulse-id [^String path heartbeats]
  (log-debug (str "Deleting Pulse for id [" path "]."))
  (.remove heartbeats path)
  (HBMessage. HBServerMessageType/DELETE_PULSE_ID_RESPONSE nil))

(defn delete-path [^String path heartbeats]
  (let [prefix (if (= \/ (last path)) path (str path "/"))]
    (doseq [k (.keySet heartbeats)
            :when (= (.indexOf k prefix) 0)]
      (delete-pulse-id k heartbeats)))
  (HBMessage. HBServerMessageType/DELETE_PATH_RESPONSE nil))

(defn not-authorized []
  (HBMessage. HBServerMessageType/NOT_AUTHORIZED nil))

(defn mk-handler [conf]
  (let [heartbeats ^ConcurrentHashMap (hb-data conf)
        pacemaker-stats {:count (AtomicInteger.)
                         :largest (AtomicInteger.)}
        last-five (ref {:count 0 :largest 0})
        stats-thread (Thread. (fn [] (report-stats pacemaker-stats last-five)))]
    (.setDaemon stats-thread true)
    (.start stats-thread)
    (register last-five)
    (reify
      IServerMessageHandler
      (^HBMessage handleMessage [this ^HBMessage request ^boolean authenticated]
        (let [response
              (condp = (.get_type request)
                HBServerMessageType/CREATE_PATH
                (create-path (.get_path (.get_data request)) heartbeats)

                HBServerMessageType/EXISTS
                (if authenticated
                  (exists (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/SEND_PULSE
                (send-pulse (.get_pulse (.get_data request)) heartbeats pacemaker-stats)

                HBServerMessageType/GET_ALL_PULSE_FOR_PATH
                (if authenticated
                  (get-all-pulse-for-path (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/GET_ALL_NODES_FOR_PATH
                (if authenticated
                  (get-all-nodes-for-path (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/GET_PULSE
                (if authenticated
                  (get-pulse (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/DELETE_PATH
                (delete-path (.get_path (.get_data request)) heartbeats)

                HBServerMessageType/DELETE_PULSE_ID
                (delete-pulse-id (.get_path (.get_data request)) heartbeats)

                ; Otherwise
                (log-message "Got Unexpected Type: " (.get_type request)))]

          (.set_message_id response (.get_message_id request))
          response)))))

(defn launch-server! []
  (log-message "Starting Server.")
  (let [conf (override-login-config-with-system-property (read-storm-config))]
    (PacemakerServer. (mk-handler conf) conf)))

(defn -main []
  (redirect-stdio-to-slf4j!)
  (launch-server!))
