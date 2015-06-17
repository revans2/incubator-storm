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
(ns clj.com.yahoo.storm.networktopography.yahoo-network-topography-test
  (:use [clojure test])
  (:use [backtype.storm bootstrap config testing thrift])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [backtype.storm.generated StormTopology]
           [backtype.storm.testing TestWordSpout TestWordCounter]
           [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails])
  (:import [backtype.storm.scheduler.resource Node ResourceAwareScheduler]))

(bootstrap)

(defn gen-supervisors [count]
  (into {} (for [id (range count)
                :let [supervisor (SupervisorDetails. (str "id" id)
                                       ; At Yahoo, gsta411nXYZ are in one rack and gsta408nXYZ are in another
                                       (str "gsta" (if (even? id) 411 108) "n" (+ 10 id) ".tan.ygrid.yahoo.com")
                                       (list ) (map int (list 1 2 3 4))
                                   {RAS_TYPES/TYPE_MEMORY 2000.0
                                    RAS_TYPES/TYPE_CPU 400.0})]]
            {(.getId supervisor) supervisor})))

(defn to-top-map [topologies]
  (into {} (for [top topologies] {(.getId top) top})))

(defn ed [id] (ExecutorDetails. (int id) (int id)))

(defn mk-ed-map [arg]
  (into {}
    (for [[name start end] arg]
      (into {}
        (for [at (range start end)]
          {(ed at) name})))))


(deftest test-network-topography
  (let [supers (gen-supervisors 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "com.yahoo.storm.networkTopography.YahooDNSToSwitchMapping"})
        network-topography (.getNetworkTopography cluster)
        ;the mocked cluster should have only two racks, each with two hosts
        rack1-info (first network-topography)
        rack1 (key rack1-info)
        rack1-nodes (val rack1-info)
        rack1-node-1 (first rack1-nodes)
        rack1-node-2 (second rack1-nodes)
        rack2-info (second network-topography)
        rack2 (key rack2-info)
        rack1-nodes (val rack1-info)
        rack2-node-1 (first rack2-nodes)
        rack2-node-2 (second rack2-nodes)]
    (is (= 2 (.size network-topography)))
    (is (= 2 (.size rack1-nodes)))
    (is (= 2 (.size rack2-nodes)))
    (is (= "/107.90.138.0" rack1))
    (is (= "host1" rack1-node-1))
    (is (= "host3" rack1-node-2))
    (is (= "/107.90.114.0" rack2))
    (is (= "host0" rack2-node-1))
    (is (= "host2" rack2-node-2))))
