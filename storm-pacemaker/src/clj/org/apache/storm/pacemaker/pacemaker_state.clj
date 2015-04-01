(ns org.apache.storm.pacemaker.pacemaker-state
  (:require [org.apache.storm pacemaker]
            [backtype.storm.cluster-state [zookeeper-state :as zk-state]]
            [finagle-clojure.futures :as futures])
  (:import [backtype.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType Message MessageData Pulse]
           [backtype.storm.cluster_state zookeeper_state]
           [org.apache.storm.pacemaker PacemakerServerFactory])
  (:use [backtype.storm config cluster log])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

(defn -mkState [this conf auth-conf acls]
  (let [zk-state (.mkState (zookeeper_state.) conf auth-conf acls)
        pacemaker-client (PacemakerServerFactory/makeClient (str (conf PACEMAKER-HOST) ":" (conf PACEMAKER-PORT)))]
    
    
    (reify
      ClusterState

      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set-ephemeral-node [this path data acls] (.set-ephemeral-node zk-state path data acls))
      (create-sequential [this path data acls] (.create-sequential zk-state path data acls))
      (set-data [this path data acls] (.set-data zk-state path data acls))
      (delete-node [this path] (.delete-node zk-state path))
      (get-data [this path watch?] (.get-data zk-state path watch?))
      (get-data-with-version [this path watch?] (.get-data-with-version zk-state path watch?))
      (get-version [this path watch?] (.get-version zk-state path watch?))
      (get-children [this path watch?] (.get-children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (exists-node? [this path watch?] (.exists-node? zk-state path watch?))
        
      (set-worker-hb [this path data acls]
        (let [response
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/SEND_PULSE
                                 (MessageData/pulse
                                  (doto (Pulse.)
                                    (.set_id path)
                                    (.set_details data))))))]
          (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
            nil
            (throw HBExecutionException "Invalid Response Type"))))

      (delete-worker-hb [this path]
        (let [response 
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/DELETE_PATH
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
            nil
            (throw HBExecutionException "Invalid Response Type"))))

      (get-worker-hb [this path watch?]
        (let [response
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/GET_PULSE
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
            (.get_details (.get_pulse (.get_data response)))
            (throw HBExecutionException "Invalid Response Type"))))

      (get-worker-hb-children [this path watch?]
        (let [response
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
            (into [] (.get_pulseIds (.get_nodes (.get_data response))))
            (throw HBExecutionException "Invalid Response Type"))))
            
      (close [this]
        (.close zk-state)
        (futures/await (.close pacemaker-client))))))

