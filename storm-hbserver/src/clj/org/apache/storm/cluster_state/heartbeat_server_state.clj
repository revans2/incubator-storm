(ns org.apache.storm.cluster-state.heartbeat-server-state
  (:require [org.apache.storm pacemaker]
            [backtype.storm.cluster-state [zookeeper-state :as zk-state]]
            [finagle-clojure.futures :as futures])
  (:import [backtype.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType Message MessageData Pulse]
           [backtype.storm.cluster_state zookeeper_state]
           [org.apache.storm PacemakerServerFactory])
  (:use [backtype.storm config cluster])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

;;(defn sample []
;;  (try
;;    (do 
;;      (def service (service-handler nil))
;;      (def server (thrift/serve "127.0.0.1:8089" service))
;;      (def client (thrift/client "127.0.0.1:8089" HBServer))
;;      
;;      (let [buff (java.nio.ByteBuffer/wrap (.getBytes "foo_bar"))
;;            buff2 (java.nio.ByteBuffer/wrap (.getBytes "foo_bar_baz"))]
;;        (println "Buffer: " buff)
;;        (println (futures/await (.sendPulse client (Pulse. "/foo" buff))))
;;        (println (futures/await (.sendPulse client (Pulse. "/foo/bar" buff2))))
;;        (println (futures/await (.sendPulse client (Pulse. "/foo/baz" buff2))))
;;        (println (String. (.getDetails (futures/await (.getPulse client "/foo")))))
;;        (println (String. (.getDetails (futures/await (.getPulse client "/foo/bar")))))
;;        (println (String. (.getDetails (futures/await (.getPulse client "/foo/baz")))))
;;        (println (futures/await (.getAllNodesForPath client "/foo")))))
;;    (catch Exception e (.printStackTrace e))))

(defn -mkState [this conf auth-conf acls]
  (let [zk-client (.mkState (zookeeper_state.) conf auth-conf acls)
        pacemaker-client (PacemakerServerFactory/makeClient (str (conf HBSERVER-HOST) ":" (conf HBSERVER-PORT)))]
    
    
    (reify
      ClusterState

      ;; Let these pass through to the zk-client. We only want to handle heartbeats.
      (register [this callback] (.register zk-client callback))
      (unregister [this callback] (.unregister zk-client callback))
      (set-ephemeral-node [this path data acls] (.set-ephemeral-node zk-client path data acls))
      (create-sequential [this path data acls] (.create-sequential zk-client path data acls))
      (set-data [this path data acls] (.set-data zk-client path data acls))
      (delete-node [this path] (.delete-node zk-client path))
      (get-data [this path watch?] (.get-data zk-client path watch?))
      (get-data-with-version [this path watch?] (.get-data-with-version zk-client path watch?))
      (get-version [this path watch?] (.get-version zk-client path watch?))
      (get-children [this path watch?] (.get-children zk-client path watch?))
      (mkdirs [this path acls] (.mkdirs zk-client path acls))
      (exists-node? [this path watch?] (.exists-node? zk-client path watch?))
        
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
;         (.sendPulse ecg-client
;                     (doto (Pulse.)
;                       (.set_id path)
;                       (.set_details data)))))

      (delete-worker-hb [this path]
        (let [response 
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/DELETE_PATH
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
            nil
            (throw HBExecutionException "Invalid Response Type"))))
;         (.delete-path ecg-client path)))

      (get-worker-hb [this path watch?]
        (let [response
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/GET_PULSE
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
            (.get_details (.get_pulse (.get_data response)))
            (throw HBExecutionException "Invalid Response Type"))))
;         (.getPulse ecg-client path)))

      (get-worker-hb-children [this path watch?]
        (let [response
              (futures/await
               (.apply pacemaker-client
                       (Message. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                 (MessageData/path path))))]
          (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
            (into [] (.get_pulseIds (.get_nodes (.get_data response))))
            (throw HBExecutionException "Invalid Response Type"))))
;         (.getAllNodesForPath ecg-client path)))

      (close [this]
        (.close zk-client)
        (futures/await (.close pacemaker-client))))))

