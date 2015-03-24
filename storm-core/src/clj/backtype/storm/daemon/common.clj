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
(ns backtype.storm.daemon.common
  (:use [backtype.storm log config util])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.task WorkerTopologyContext])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.security.auth IAuthorizer]) 
  (:require [clojure.set :as set])  
  (:require [backtype.storm.daemon.acker :as acker]))

(defn system-id? [id]
  (Utils/isSystemId id))

(def ACKER-COMPONENT-ID acker/ACKER-COMPONENT-ID)
(def ACKER-INIT-STREAM-ID acker/ACKER-INIT-STREAM-ID)
(def ACKER-ACK-STREAM-ID acker/ACKER-ACK-STREAM-ID)
(def ACKER-FAIL-STREAM-ID acker/ACKER-FAIL-STREAM-ID)

(def SYSTEM-STREAM-ID "__system")

(def SYSTEM-COMPONENT-ID Constants/SYSTEM_COMPONENT_ID)
(def SYSTEM-TICK-STREAM-ID Constants/SYSTEM_TICK_STREAM_ID)
(def METRICS-STREAM-ID Constants/METRICS_STREAM_ID)
(def METRICS-TICK-STREAM-ID Constants/METRICS_TICK_STREAM_ID)
(def CREDENTIALS-CHANGED-STREAM-ID Constants/CREDENTIALS_CHANGED_STREAM_ID)

;; the task id is the virtual port
;; node->host is here so that tasks know who to talk to just from assignment
;; this avoid situation where node goes down and task doesn't know what to do information-wise
(defrecord Assignment [master-local-dir node->host executor->node+port executor->start-time-secs])


;; component->executors is a map from spout/bolt id to number of executors for that component
(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors owner])

(defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs])

(defprotocol DaemonCommon
  (waiting? [this]))

(def LS-WORKER-HEARTBEAT "worker-heartbeat")

;; LocalState constants
(def LS-ID "supervisor-id")
(def LS-LOCAL-ASSIGNMENTS "local-assignments")
(def LS-APPROVED-WORKERS "approved-workers")

(def NIMBUS-LS-TOPO-HISTORY "topo-hist")

(defrecord WorkerHeartbeat [time-secs storm-id executors port])

(defrecord ExecutorStats [^long processed
                          ^long acked
                          ^long emitted
                          ^long transferred
                          ^long failed])

(defn new-executor-stats []
  (ExecutorStats. 0 0 0 0 0))

(defn get-storm-id [storm-cluster-state storm-name]
  (let [active-storms (.active-storms storm-cluster-state)]
    (find-first
      #(= storm-name (:storm-name (.storm-base storm-cluster-state % nil)))
      active-storms)
    ))

(defn topology-bases [storm-cluster-state]
  (let [active-topologies (.active-storms storm-cluster-state)]
    (into {} 
          (dofor [id active-topologies]
                 [id (.storm-base storm-cluster-state id nil)]
                 ))
    ))

(defn validate-distributed-mode! [conf]
  (if (local-mode? conf)
      (throw
        (IllegalArgumentException. "Cannot start server in local mode!"))))

(defmacro defserverfn [name & body]
  `(let [exec-fn# (fn ~@body)]
    (defn ~name [& args#]
      (try-cause
        (apply exec-fn# args#)
      (catch InterruptedException e#
        (throw e#))
      (catch Throwable t#
        (log-error t# "Error on initialization of server " ~(str name))
        (halt-process! 13 "Error on initialization")
        )))))

(defn component-conf [component]
  (->> component
      .get_common
      .get_json_conf
      from-json))

(defn map-occurrences [afn coll]
  (->> coll
       (reduce (fn [[counts new-coll] x]
                 (let [occurs (inc (get counts x 0))]
                   [(assoc counts x occurs) (cons (afn x occurs) new-coll)]))
               [{} []])
       (second)
       (reverse)))

(defn number-duplicates
  "(number-duplicates [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]"
  [coll]
  (map-occurrences (fn [x occurences] (if (>= occurences 2) (str x "#" occurences) x)) coll))

(defn metrics-consumer-register-ids
  "Generates a list of component ids for each metrics consumer
   e.g. [\"__metrics_org.mycompany.MyMetricsConsumer\", ..] "
  [storm-conf]
  (->> (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER)         
       (map #(get % "class"))
       (number-duplicates)
       (map #(str Constants/METRICS_COMPONENT_ID_PREFIX %))))

(defn has-ackers? [storm-conf]
  (or (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0)))

(defn executor-id->tasks [[first-task-id last-task-id]]
  (->> (range first-task-id (inc last-task-id))
       (map int)))

(defn worker-context [worker]
  (WorkerTopologyContext. (:system-topology worker)
                          (:storm-conf worker)
                          (:task->component worker)
                          (:component->sorted-tasks worker)
                          (:component->stream->fields worker)
                          (:storm-id worker)
                          (supervisor-storm-resources-path
                            (supervisor-stormdist-root (:conf worker) (:storm-id worker)))
                          (worker-pids-root (:conf worker) (:worker-id worker))
                          (:port worker)
                          (:task-ids worker)
                          (:default-shared-resources worker)
                          (:user-shared-resources worker)
                          ))


(defn to-task->node+port [executor->node+port]
  (->> executor->node+port
       (mapcat (fn [[e node+port]] (for [t (executor-id->tasks e)] [t node+port])))
       (into {})))

(defn mk-authorization-handler [klassname conf]
  (let [aznClass (if klassname (Class/forName klassname))
        aznHandler (if aznClass (.newInstance aznClass))] 
    (if aznHandler (.prepare ^IAuthorizer aznHandler conf))
    (log-debug "authorization class name:" klassname
                 " class:" aznClass
                 " handler:" aznHandler)
    aznHandler
  )) 

