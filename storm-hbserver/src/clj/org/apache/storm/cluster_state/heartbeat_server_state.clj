(ns org.apache.storm.cluster-state.heartbeat-server-state
  (:import [org.apache.storm.generated HBServer Pulse])
  (:require [org.apache.storm ecg]
            [backtype.storm.cluster-state [zookeeper-state :as zk-state]]
            [finagle-clojure.thrift :as thrift]
            [finagle-clojure.futures :as futures])
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
  (let [zk-client (zk-state/-mkState conf auth-conf acls)
        conf (into {} conf)
        auth-conf (if auth-conf (into {} auth-conf))
        ecg-client (thrift/client (str ":" (conf HBSERVER-THRIFT-PORT)) HBServer)]
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
      (exists-node? [this path watch?] (.exists-node? path watch?))
        
      (set-worker-hb [this path data acls]
        (futures/await
         (.sendPulse ecg-client
                     (doto (Pulse.)
                       (.set_id path)
                       (.set_details data)))))

      (delete-worker-hb [this path]
        (futures/await
         (.delete-path ecg-client path)))

      (get-worker-hb [this path watch?]
        (futures/await
         (.getPulse ecg-client path)))

      (get-worker-hb-children [this path watch?]
        (futures/await
         (.getAllNodesForPath ecg-client path)))

      (close [this]
        (.close zk-client)
        (.shutdown ecg-client)))))
