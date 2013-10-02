(ns backtype.storm.worker-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:use [backtype.storm bootstrap config testing])
  (:use [backtype.storm.daemon common])
  (:require [conjure.core])
  (:use [conjure core])
  (:import [org.apache.zookeeper data.ACL data.Id ZooDefs$Ids ZooDefs$Perms])
  )

(bootstrap)

(deftest test-worker-data-acls
  (testing "worker-data uses correct ACLs"
    (let [scheme "digest"
          digest "storm:thisisapoorpassword"
          auth-conf {STORM-ZOOKEEPER-AUTH-SCHEME scheme
                     STORM-ZOOKEEPER-AUTH-PAYLOAD digest}
          topo-conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME scheme
                     STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD digest}
 
          expected-acls (concat [(ACL. ZooDefs$Perms/READ (Id. "sasl" "@"))] ZooDefs$Ids/CREATOR_ALL_ACL)]
      (stubbing [read-supervisor-storm-conf topo-conf
                 worker/read-worker-executors #{}
                 cluster/mk-distributed-cluster-state nil
                 cluster/mk-storm-cluster-state nil
                 disruptor/disruptor-queue nil
                 read-supervisor-topology nil
                 worker/recursive-map-worker-data nil
                 ]
        (worker/worker-data auth-conf true nil nil nil nil)
        (verify-call-times-for cluster/mk-distributed-cluster-state 1)
        (verify-first-call-args-for-indices
          cluster/mk-distributed-cluster-state [4] expected-acls)
        (verify-call-times-for cluster/mk-storm-cluster-state 1)
        (verify-first-call-args-for-indices cluster/mk-storm-cluster-state [2]
                                            expected-acls)
))))
