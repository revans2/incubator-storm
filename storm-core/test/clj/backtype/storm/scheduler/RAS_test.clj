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
(ns backtype.storm.scheduler.RAS-test
  (:use [clojure test])
  (:use [backtype.storm bootstrap config testing thrift])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [backtype.storm.generated StormTopology]
           [backtype.storm.testing TestWordSpout TestWordCounter]
           [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails])
  (:import [backtype.storm.scheduler.resource Node ResourceAwareScheduler RAS_TYPES]))

(bootstrap)

(defn gen-supervisors [count]
  (into {} (for [id (range count)
                :let [supervisor (SupervisorDetails. (str "super" id) (str "host" id) (list ) (map int (list 1 2 3 4))
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



;; testing resource/Node class
(deftest test-node
  (let [supers (gen-supervisors 5)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {})
        topologies (Topologies. (to-top-map []))
        node-map (Node/getAllNodesFrom cluster topologies)]
    (is (= 5 (.size node-map)))
    (let [node (.get node-map "super0")]
      (is (= "super0" (.getId node)))
      (is (= true (.isAlive node)))
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology1" (list (ExecutorDetails. 1 1)) cluster)
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 3 (.totalSlotsFree node)))
      (is (= 1 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology1" (list (ExecutorDetails. 2 2)) cluster)
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 2 (.totalSlotsFree node)))
      (is (= 2 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology2" (list (ExecutorDetails. 1 1)) cluster)
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 1 (.totalSlotsFree node)))
      (is (= 3 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology2" (list (ExecutorDetails. 2 2)) cluster)
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 0 (.totalSlotsFree node)))
      (is (= 4 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.freeAllSlots node cluster)
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
    )))

(deftest test-sanity-RAS-scheduler
  (let [builder (TopologyBuilder.)
        _ (.setSpout builder "wordSpout" (TestWordSpout.) 1)
        _ (.shuffleGrouping (.setBolt builder "wordCountBolt" (TestWordCounter.) 1) "wordSpout")
        supers (gen-supervisors 3)
        storm-topology (.createTopology builder)
        topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userC"
                       TOPOLOGY-RESOURCES-ONHEAP-MEMORY-MB 100.0
                       TOPOLOGY-RESOURCES-OFFHEAP-MEMORY-MB 50.0
                       TOPOLOGY-DEFAULT-CPU-REQUIREMENT 10.0
                       TOPOLOGY-TYPE-CPU "cpu"
                       TOPOLOGY-TYPE-CPU-TOTAL "total"
                       TOPOLOGY-TYPE-MEMORY "memory"
                       }
                       storm-topology
                       4
                       (mk-ed-map [["wordSpout" 0 1]
                                   ["wordCountBolt" 1 2]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {})
        topologies (Topologies. (to-top-map [topology1]))
        node-map (Node/getAllNodesFrom cluster topologies)
        scheduler (ResourceAwareScheduler.)]
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      ;; 4 slots on 1 machine, all executors assigned
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 2 (.size executors))))
    (is (= "topology1 Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))))

(deftest test-topology-set-memory-and-cpu-load
  (let [builder (TopologyBuilder.)
        _ (.setSpout builder "wordSpout" (TestWordSpout.) 1)
        bolt (.setBolt builder "wordCountBolt" (TestWordCounter.) 1)
        _ (.setMemoryLoad bolt 110.0)
        _ (.setCPULoad bolt 20.0)
        _ (.shuffleGrouping bolt "wordSpout")
        supers (gen-supervisors 3)
        storm-topology (.createTopology builder)
        topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-RESOURCES-ONHEAP-MEMORY-MB 100.0
                     TOPOLOGY-RESOURCES-OFFHEAP-MEMORY-MB 50.0
                     TOPOLOGY-DEFAULT-CPU-REQUIREMENT 10.0
                     TOPOLOGY-TYPE-CPU "cpu"
                     TOPOLOGY-TYPE-CPU-TOTAL "total"
                     TOPOLOGY-TYPE-MEMORY "memory"
                     }
                    storm-topology
                    4
                    (mk-ed-map [["wordSpout" 0 1]
                                ["wordCountBolt" 1 2]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {})
        topologies (Topologies. (to-top-map [topology2]))
        node-map (Node/getAllNodesFrom cluster topologies)
        scheduler (ResourceAwareScheduler.)]
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology2")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      ;; 4 slots on 1 machine, all executors assigned
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 2 (.size executors))))
    (is (= "topology2 Fully Scheduled" (.get (.getStatusMap cluster) "topology2")))))