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
                       TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                       TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                       TOPOLOGY-COMPONENT-CPU-REQUIREMENT 10.0
                       TOPOLOGY-COMPONENT-TYPE-CPU "cpu"
                       TOPOLOGY-COMPONENT-TYPE-CPU-TOTAL "total"
                       TOPOLOGY-COMPONENT-TYPE-MEMORY "memory"
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
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-REQUIREMENT 10.0
                     TOPOLOGY-COMPONENT-TYPE-CPU "cpu"
                     TOPOLOGY-COMPONENT-TYPE-CPU-TOTAL "total"
                     TOPOLOGY-COMPONENT-TYPE-MEMORY "memory"
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

(deftest test-resource-limitation
  (let [builder (TopologyBuilder.)
        _ (doto (.setSpout builder "wordSpout" (TestWordSpout.) 2)
            (.setMemoryLoad 1000.0 200.0)
            (.setCPULoad 250.0))
        _ (doto (.setBolt builder "wordCountBolt" (TestWordCounter.) 1)
            (.shuffleGrouping  "wordSpout")
            (.setMemoryLoad 500.0 100.0)
            (.setCPULoad 100.0))
        supers (gen-supervisors 3)
        storm-topology (.createTopology builder)
        topology1 (TopologyDetails. "topology1"
                                    {TOPOLOGY-NAME "topology-name-1"
                                     TOPOLOGY-SUBMITTER-USER "userC"}
                                    storm-topology
                                    4
                                    (mk-ed-map [["wordSpout" 0 2]
                                                ["wordCountBolt" 2 3]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {})
        topologies (Topologies. (to-top-map [topology1]))
        scheduler (ResourceAwareScheduler.)]
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          node-ids (map #(.getNodeId %) assigned-slots)
          executors (.getExecutors assignment)
          epsilon 0.000001
          ed-to-super (into {}
                            (for [[ed slot] (.getExecutorToSlot assignment)]
                              {ed (.getSupervisorById cluster (.getNodeId slot))}))
          super-to-eds (reverse-map ed-to-super)
          mem-avail-to-used (into []
                                 (for [[super eds] super-to-eds]
                                   [(.getTotalMemory super) (sum (map #(.getTotalMemReqTask topology1 %) eds))]))
          cpu-avail-to-used (into []
                                 (for [[super eds] super-to-eds]
                                   [(.getTotalCPU super) (sum (map #(.getTotalCpuReqTask topology1 %) eds))]))]
    ;; 4 slots on 1 machine, all executors assigned
    (is (= 2 (.size assigned-slots)))
    (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    (is (= 3 (.size executors)))
    ;; make sure resource (mem/cpu) assigned equals to resource specified`
    (is (< (Math/abs (- 1200.0 (apply max (map #(.getTotalMemReqTask topology1 %) executors)))) epsilon))
    (is (< (Math/abs (- 250.0 (apply max (map #(.getTotalCpuReqTask topology1 %) executors)))) epsilon))
    (doseq [[avail used] mem-avail-to-used] ;; for each node, assigned mem smaller than total 
      (is (>= avail used)))
    (doseq [[avail used] cpu-avail-to-used] ;; for each node, assigned cpu smaller than total
      (is (>= avail used))))
  (is (= "topology1 Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))))
