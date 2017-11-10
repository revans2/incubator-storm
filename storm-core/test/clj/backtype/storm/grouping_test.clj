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
(ns backtype.storm.grouping-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter TestWordBytesCounter NGrouping]
           [backtype.storm.generated JavaObject JavaObjectArg Grouping NullStruct])
  (:import [backtype.storm.grouping LoadMapping])
  (:import [backtype.storm.utils Utils])
  (:import [org.apache.storm Thrift])
  (:import [org.apache.storm.daemon GrouperFactory])
  (:use [backtype.storm testing clojure log config])
  (:use [backtype.storm.daemon common executor])
  (:require [backtype.storm [thrift :as thrift]]))

(def shuffle-grouping (Grouping/shuffle (NullStruct. )))

(deftest test-shuffle
 (let [shuffle (GrouperFactory/mkGrouper nil "comp" "stream" nil shuffle-grouping [(int 1) (int 2)] {TOPOLOGY-DISABLE-LOADAWARE-MESSAGING true})
       num-messages 100000
       min-prcnt (int (* num-messages 0.49))
       max-prcnt (int (* num-messages 0.51))
       data [1 2]
       freq (frequencies (for [x (range 0 num-messages)] (.chooseTasks shuffle (int 1) data)))
       load1 (.get freq [(int 1)])
       load2 (.get freq [(int 2)])]
    (log-message "FREQ:" freq)
    (is (>= load1 min-prcnt))
    (is (<= load1 max-prcnt))
    (is (>= load2 min-prcnt))
    (is (<= load2 max-prcnt))))

(deftest test-field
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [spout-phint 4
          bolt-phint 6
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                          (TestWordSpout. true) (Integer. spout-phint))}
                    {"2" (Thrift/prepareBoltDetails
                          {(Utils/getGlobalStreamId "1" nil)
                           (Thrift/prepareFieldsGrouping ["word"])}
                          (TestWordBytesCounter.) (Integer. bolt-phint))
                     })
          results (complete-topology
                    cluster
                    topology
                    :mock-sources {"1" (->> [[(.getBytes "a")]
                                             [(.getBytes "b")]]
                                            (repeat (* spout-phint bolt-phint))
                                            (apply concat))})]
      (is (ms= (apply concat
                      (for [value '("a" "b")
                            sum (range 1 (inc (* spout-phint bolt-phint)))]
                        [[value sum]]))
               (read-tuples results "2"))))))

(deftest test-field
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [spout-phint 4
          bolt-phint 6
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                          (TestWordSpout. true) (Integer. spout-phint))}
                    {"2" (Thrift/prepareBoltDetails
                          {(Utils/getGlobalStreamId "1" nil)
                           (Thrift/prepareFieldsGrouping ["word"])}
                          (TestWordBytesCounter.) (Integer. bolt-phint))
                     })
          results (complete-topology
                   cluster
                   topology
                   :mock-sources {"1" (->> [[(.getBytes "a")]
                                            [(.getBytes "b")]]
                                           (repeat (* spout-phint bolt-phint))
                                           (apply concat))})]
      (is (ms= (apply concat
                      (for [value '("a" "b")
                            sum (range 1 (inc (* spout-phint bolt-phint)))]
                        [[value sum]]))
               (read-tuples results "2"))))))

(defbolt id-bolt ["val"] [tuple collector]
  (emit-bolt! collector (.getValues tuple))
  (ack! collector tuple))

(deftest test-custom-groupings
  (with-simulated-time-local-cluster [cluster]
    (let [topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                          (TestWordSpout. true))}
                    {"2" (Thrift/prepareBoltDetails
                          {(Utils/getGlobalStreamId "1" nil)
                           (Thrift/prepareCustomStreamGrouping (NGrouping. (Integer. 2)))}
                          id-bolt
                          (Integer. 4))
                     "3" (Thrift/prepareBoltDetails
                          {(Utils/getGlobalStreamId "1" nil)
                           (Thrift/prepareCustomJavaObjectGrouping
                            (JavaObject. "backtype.storm.testing.NGrouping"
                                         [(JavaObjectArg/int_arg 3)]))}
                          id-bolt
                          (Integer. 6))
                     })
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [["a"]
                                                         ["b"]
                                                         ]}
                                     )]
      (is (ms= [["a"] ["a"] ["b"] ["b"]]
               (read-tuples results "2")))
      (is (ms= [["a"] ["a"] ["a"] ["b"] ["b"] ["b"]]
               (read-tuples results "3")))
      )))