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

(ns org.apache.storm.ecg
;  (:require [org.apache.storm hbserver])
  (:import [java.nio ByteBuffer]
           [org.apache.storm.generated HBServer HBServer$Processor HBServer$ServiceIface
            HBAuthorizationException HBExecutionException HBRecords Pulse HBNodes]
           [java.util.concurrent ConcurrentHashMap ThreadPoolExecutor TimeUnit LinkedBlockingDeque]
           [java.io FileNotFoundException]
           [java.nio.channels Channels WritableByteChannel]
           [backtype.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils]
           [backtype.storm.daemon Shutdownable]
           [com.twitter.util Future])
           ;[org.apache.storm hbserver])
  (:use [clojure.string :only [replace-first split]]
        [backtype.storm bootstrap util log]
        [backtype.storm.config :only [validate-configs-with-schemas read-storm-config]]
        [backtype.storm.daemon common])
  (:require [finagle-clojure.future-pool :as future-pool]
            [finagle-clojure.futures :as futures]
            [finagle-clojure.thrift :as thrift])
  (:gen-class))

(defn hb-data [conf]
  (ConcurrentHashMap.))

(defserverfn service-handler
  [conf]
  (log-message "Starting HBServer with conf " conf)
  (let [heartbeats ^ConcurrentHashMap (hb-data conf)
        pool (future-pool/future-pool
              (ThreadPoolExecutor. 5 10
                                   1 TimeUnit/HOURS
                                   (LinkedBlockingDeque.)))]
    (thrift/service HBServer
      (^Future createPath [this ^String path] (futures/value nil))
      
      (^Future exists [this ^String path]
        (future-pool/run pool 
          (let [it-does (.containsKey heartbeats path)]
            (log-debug (str "Checking if path [" path "] exists..." it-does "."))
            it-does)))
      
      (^Future sendPulse [this ^Pulse pulse]
        (future-pool/run pool
          (let [id (.getId pulse)
                details (.getDetails pulse)]
            (log-debug (str "Saving Pulse for id [" id "] data [" + (str details) "]."))
            (.put heartbeats id details))))
      
      (^Future getAllPulseForPath [this ^String idPrefix] (futures/value nil))
      
      (^Future getAllNodesForPath [this ^String idPrefix]
        (future-pool/run pool
          (log-debug "List all nodes for path " idPrefix)  
          (HBNodes. (distinct (for [k (.keySet heartbeats)
                                    :let [trimmed-k (second (split (replace-first k idPrefix "") #"/"))]
                                    :when (and
                                           (not (nil? trimmed-k))
                                           (= (.indexOf k idPrefix) 0))]
                                trimmed-k)))))
      
      (^Future getPulse [this ^String id]
        (future-pool/run pool
          (let [details (.get heartbeats id)]
            (log-debug (str "Getting Pulse for id [" id "]...data " (str details) "]."))
            (doto (Pulse. ) (.setId id) (.setDetails details)))))

      (^Future deletePath [this ^String idPrefix]
        (future-pool/run pool
          (let [prefix (if (= \/ (last idPrefix)) idPrefix (str idPrefix "/"))]
            (doseq [k (.keySet heartbeats)
                    :when (= (.indexOf k prefix) 0)]
              (.deletePulseId this k)))))

      (^Future deletePulseId [this ^String id]
        (future-pool/run pool
          (log-message (str "Deleting Pulse for id [" id "]."))
          (.remove heartbeats id)))
      
      (shutdown [this]
        (log-message "Shutting down hbserver"))
      
      
      (waiting? [this] ()))))
      

(defn launch-server! []
  (log-message "Beginning.")
  (let [conf (read-storm-config)
        service-handler (service-handler conf)
        _ (log-message "Service Handler: " (type service-handler))]
    ;(.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) (.close server))))
    (log-message "Starting hbserver...")
    (thrift/serve ":8089" service-handler)))


(defn -main []
  (launch-server!))
