(ns backtype.storm.scheduler.multitenant-ras-bridge-scheduler-test
  (:use [clojure test])
  (:use [backtype.storm config testing thrift])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [backtype.storm.generated StormTopology]
           [backtype.storm Config]
           [backtype.storm.testing TestWordSpout TestWordCounter]
           [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails])
  (:import [backtype.storm.scheduler.resource RAS_Node ResourceAwareScheduler])
  (:import [backtype.storm.scheduler.multitenant MultitenantScheduler])
  (:import [backtype.storm.scheduler.bridge MultitenantResourceAwareBridgeScheduler]))

(def MULTITENANT-SCHEDULER (Class/forName "backtype.storm.scheduler.resource.strategies.MultitenantStrategy"))
(def RESOURCE-AWARE-SCHEDULER (Class/forName "backtype.storm.scheduler.resource.strategies.ResourceAwareStrategy"))

(defn gen-supervisors [count ports]
  (into {} (for [id (range count)
                :let [supervisor (SupervisorDetails. (str "id" id)
                                       (str "host" id)
                                       (list ) (map int (range ports))
                                   {Config/SUPERVISOR_MEMORY_CAPACITY_MB 2000.0
                                    Config/SUPERVISOR_CPU_CAPACITY 400.0})]]
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

;; test scheduling a topology with only using multitenant scheduler
;; check if scheduling is correct
(deftest test-schedule-multitenant
  (let [supers (gen-supervisors 5 4)
         cluster (Cluster. (nimbus/standalone-nimbus) supers {} {})
         builder (TopologyBuilder.)
         _ (doto (.setSpout builder "spout1" (TestWordSpout.) 5))
         conf (Config.)
         _ (.setTopologyStrategy conf MULTITENANT-SCHEDULER)
         _ (.put conf Config/TOPOLOGY_NAME "topology-name-1")
         _ (.put conf Config/TOPOLOGY_SUBMITTER_USER "userPeng")
         _ (.put conf Config/TOPOLOGY_WORKERS 5)
         conf_scheduler {MULTITENANT-SCHEDULER-USER-POOLS {"userJerry" 1}}
         storm-topology (.createTopology builder)
         topology1 (TopologyDetails. "topology1"
                     conf
                     storm-topology
                     5
                     (mk-ed-map [["spout1" 0 5]]))
         topologies (Topologies. (to-top-map [topology1]))
         scheduler (MultitenantResourceAwareBridgeScheduler.)]
    (.prepare scheduler conf_scheduler)
    (.schedule scheduler topologies cluster)
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))))

;; test scheduling a topology with only using resource aware scheduler
(deftest test-schedule-resource-aware
  (let [supers (gen-supervisors 5 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {} 
                   {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "backtype.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        builder (TopologyBuilder.)
        _ (doto (.setSpout builder "spout1" (TestWordSpout.) 5) 
            (.setMemoryLoad 500.0 12.0)
            (.setCPULoad 10.0))
        conf (Config.)
        _ (.setTopologyStrategy conf RESOURCE-AWARE-SCHEDULER)
        _ (.put conf Config/TOPOLOGY_NAME "topology-name-1")
        _ (.put conf Config/TOPOLOGY_SUBMITTER_USER "userPeng")
        _ (.put conf Config/TOPOLOGY_WORKERS 5)
        storm-topology (.createTopology builder)
        topology1 (TopologyDetails. "topology1"
                    conf
                    storm-topology
                    5
                    (mk-ed-map [["spout1" 0 5]]))
        
        topologies (Topologies. (to-top-map [topology1]))
        scheduler (MultitenantResourceAwareBridgeScheduler.)]
    (.prepare scheduler {})
    (.schedule scheduler topologies cluster)
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))))

;; test multiple schedulings with scheduling multitenant topologies first 
(deftest test-consecutive-scheduling-multitenant-first
  (let [supers (gen-supervisors 5 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {} 
                   {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "backtype.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        builder1 (TopologyBuilder.)
        _ (doto (.setSpout builder1 "spout1" (TestWordSpout.) 1))
        conf_scheduler {MULTITENANT-SCHEDULER-USER-POOLS {"userJerry" 1}}
        conf1 (Config.)
       _ (.setTopologyStrategy conf1 MULTITENANT-SCHEDULER)
       _ (.put conf1 Config/TOPOLOGY_NAME "topology-name-1")
       _ (.put conf1 Config/TOPOLOGY_SUBMITTER_USER "userJerry")
       _ (.put conf1 Config/TOPOLOGY_WORKERS 1)
                
        storm-topology1 (.createTopology builder1)
        topology1 (TopologyDetails. "topology1"
                    conf1
                    storm-topology1
                    1
                    (mk-ed-map [["spout1" 0 1]]))
        
         builder2 (TopologyBuilder.)
        _ (doto (.setSpout builder2 "spout1" (TestWordSpout.) 5))
        conf2 (Config.)
       _ (.setTopologyStrategy conf2 MULTITENANT-SCHEDULER)
       _ (.put conf2 Config/TOPOLOGY_NAME "topology-name-2")
       _ (.put conf2 Config/TOPOLOGY_SUBMITTER_USER "userPeng")
       _ (.put conf2 Config/TOPOLOGY_WORKERS 5)
       _ (.put conf2 Config/TOPOLOGY_WORKER_CHILDOPTS "-Xmx128m")
                
        storm-topology2 (.createTopology builder2)
        topology2 (TopologyDetails. "topology2"
                    conf2
                    storm-topology2
                    5
                    (mk-ed-map [["spout1" 0 5]]))
        
         builder3 (TopologyBuilder.)
        _ (doto (.setSpout builder3 "spout1" (TestWordSpout.) 5) 
            (.setMemoryLoad 500.0 12.0)
            (.setCPULoad 10.0))
        conf3 (Config.)
       _ (.setTopologyStrategy conf3 MULTITENANT-SCHEDULER)
       _ (.put conf3 Config/TOPOLOGY_NAME "topology-name-3")
       _ (.put conf3 Config/TOPOLOGY_SUBMITTER_USER "userPeng")
       _ (.put conf3 Config/TOPOLOGY_WORKERS 5)
       _ (.put conf3 Config/TOPOLOGY_WORKER_CHILDOPTS "-Xmx128m")
                
        storm-topology3 (.createTopology builder3)
        topology3 (TopologyDetails. "topology3"
                    conf3
                    storm-topology3
                    5
                    (mk-ed-map [["spout1" 0 5]]))
        
        topologies (Topologies. (to-top-map [topology1 topology2 topology3]))
        scheduler (MultitenantResourceAwareBridgeScheduler.)]
    (.prepare scheduler conf_scheduler)
    (.schedule scheduler topologies cluster)
    (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology2")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology3")))
    (.schedule scheduler topologies cluster)
    (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology2")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology3")))))

;; test multiple schedulings with scheduling resource aware topology first
(deftest test-consecutive-scheduling-resource-aware-first
  (let [supers (gen-supervisors 5 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {} 
                   {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "backtype.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        conf_scheduler {MULTITENANT-SCHEDULER-USER-POOLS {"userJerry" 1}}
        builder1 (TopologyBuilder.)
        _ (doto (.setSpout builder1 "spout1" (TestWordSpout.) 5) 
            (.setMemoryLoad 500.0 12.0)
            (.setCPULoad 10.0))
        conf1 (Config.)
        _ (.setTopologyStrategy conf1 RESOURCE-AWARE-SCHEDULER)
        _ (.put conf1 Config/TOPOLOGY_NAME "topology-name-1")
        _ (.put conf1 Config/TOPOLOGY_SUBMITTER_USER "userPeng")
        _ (.put conf1 Config/TOPOLOGY_WORKERS 5)
        _ (.put conf1 Config/TOPOLOGY_WORKER_CHILDOPTS "-Xmx128m")
                
        storm-topology1 (.createTopology builder1)
        topology1 (TopologyDetails. "topology1"
                    conf1
                    storm-topology1
                    5
                    (mk-ed-map [["spout1" 0 5]]))

        topologies (Topologies. (to-top-map [topology1]))
        scheduler (MultitenantResourceAwareBridgeScheduler.)

        builder2 (TopologyBuilder.)
        _ (doto (.setSpout builder2 "spout1" (TestWordSpout.) 1))
        conf2 (Config.)
        _ (.setTopologyStrategy conf2 MULTITENANT-SCHEDULER)
        _ (.put conf2 Config/TOPOLOGY_NAME "topology-name-2")
        _ (.put conf2 Config/TOPOLOGY_SUBMITTER_USER "userJerry")
        _ (.put conf2 Config/TOPOLOGY_WORKERS 1)

        storm-topology2 (.createTopology builder2)
        topology2 (TopologyDetails. "topology2"
                    conf2
                    storm-topology2
                    1
                    (mk-ed-map [["spout1" 0 1]]))
      
        builder3 (TopologyBuilder.)
        _ (doto (.setSpout builder3 "spout1" (TestWordSpout.) 5))
        conf3 (Config.)
        _ (.setTopologyStrategy conf3 MULTITENANT-SCHEDULER)
        _ (.put conf3 Config/TOPOLOGY_NAME "topology-name-3")
        _ (.put conf3 Config/TOPOLOGY_SUBMITTER_USER "userPeng")
        _ (.put conf3 Config/TOPOLOGY_WORKERS 5)
        _ (.put conf3 Config/TOPOLOGY_WORKER_CHILDOPTS "-Xmx128m")
                
        storm-topology3 (.createTopology builder3)
        topology3 (TopologyDetails. "topology3"
                    conf3
                    storm-topology3
                    5
                    (mk-ed-map [["spout1" 0 5]]))
        topologies2 (Topologies. (to-top-map [topology1 topology2 topology3]))
        scheduler (MultitenantResourceAwareBridgeScheduler.)]
    (.prepare scheduler conf_scheduler)
    (.schedule scheduler topologies cluster)
    (.schedule scheduler topologies2 cluster)
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology2")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology3")))
    (.schedule scheduler topologies2 cluster)
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology2")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology3")))))