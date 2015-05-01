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

(ns org.apache.storm.pacemaker.pacemaker-state-factory
  (:require [org.apache.storm.pacemaker pacemaker]
            [backtype.storm.cluster-state [zookeeper-state-factory :as zk-factory]])
  (:import [backtype.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType Message MessageData Pulse]
           [backtype.storm.cluster_state zookeeper_state_factory]
           [backtype.storm.cluster ClusterState]
           [org.apache.storm.pacemaker PacemakerServerFactory])
  (:use [backtype.storm config cluster log])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

(defn -mkState [this conf auth-conf acls]
  (let [zk-state (.mkState (zookeeper_state_factory.) conf auth-conf acls)
        pacemaker-client (PacemakerServerFactory/makeClient conf)]

    (reify
      ClusterState
      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set_ephemeral_node [this path data acls] (.set_ephemeral_node zk-state path data acls))
      (create_sequential [this path data acls] (.create_sequential zk-state path data acls))
      (set_data [this path data acls] (.set_data zk-state path data acls))
      (delete_node [this path] (.delete_node zk-state path))
      (get_data [this path watch?] (.get_data zk-state path watch?))
      (get_data_with_version [this path watch?] (.get_data_with_version zk-state path watch?))
      (get_version [this path watch?] (.get_version zk-state path watch?))
      (get_children [this path watch?] (.get_children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (node_exists [this path watch?] (.node_exists zk-state path watch?))

      (set_worker_hb [this path data acls]
        (let [response
               (.send pacemaker-client
                       (Message. HBServerMessageType/SEND_PULSE
                                 (MessageData/pulse
                                  (doto (Pulse.)
                                    (.set_id path)
                                    (.set_details data)))))]
          (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
            nil
            (throw HBExecutionException "Invalid Response Type"))))

      (delete_worker_hb [this path]
        (let [response
               (.send pacemaker-client
                       (Message. HBServerMessageType/DELETE_PATH
                                 (MessageData/path path)))]
          (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
            nil
            (throw HBExecutionException "Invalid Response Type"))))

      (get_worker_hb [this path watch?]
        (let [response
               (.send pacemaker-client
                       (Message. HBServerMessageType/GET_PULSE
                                 (MessageData/path path)))]
          (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
            (.get_details (.get_pulse (.get_data response)))
            (throw HBExecutionException "Invalid Response Type"))))

      (get_worker_hb_children [this path watch?]
        (let [response
               (.send pacemaker-client
                       (Message. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                 (MessageData/path path)))]
          (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
            (into [] (.get_pulseIds (.get_nodes (.get_data response))))
            (throw HBExecutionException "Invalid Response Type"))))

      (close [this]
        (.close zk-state)
        (.close pacemaker-client)))))
