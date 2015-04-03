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

(ns backtype.storm.cluster-state.zookeeper-state-factory
  (:import [org.apache.zookeeper KeeperException KeeperException$NoNodeException ZooDefs ZooDefs$Ids ZooDefs$Perms]
           [backtype.storm.cluster ClusterState])
  (:use [backtype.storm cluster config log util])
  (:require [backtype.storm [zookeeper :as zk]])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

(defn -mkState [this conf auth-conf acls]
  (let [zk (zk/mk-client conf (conf STORM-ZOOKEEPER-SERVERS) (conf STORM-ZOOKEEPER-PORT) :auth-conf auth-conf)]
    (zk/mkdirs zk (conf STORM-ZOOKEEPER-ROOT) acls)
    (.close zk))
  (let [callbacks (atom {})
        active (atom true)
        zk (zk/mk-client conf
                         (conf STORM-ZOOKEEPER-SERVERS)
                         (conf STORM-ZOOKEEPER-PORT)
                         :auth-conf auth-conf
                         :root (conf STORM-ZOOKEEPER-ROOT)
                         :watcher (fn [state type path]
                                    (when @active
                                      (when-not (= :connected state)
                                        (log-warn "Received event " state ":" type ":" path " with disconnected Zookeeper."))
                                      (when-not (= :none type)
                                        (doseq [callback (vals @callbacks)]
                                          (callback type path))))))]
    (reify
      ClusterState
      
      (register
        [this callback]
        (let [id (uuid)]
          (swap! callbacks assoc id callback)
          id))
      
      (unregister
        [this id]
        (swap! callbacks dissoc id))
      
      (set_ephemeral_node
        [this path data acls]
        (zk/mkdirs zk (parent-path path) acls)
        (if (zk/exists zk path false)
          (try-cause
           (zk/set-data zk path data) ; should verify that it's ephemeral
           (catch KeeperException$NoNodeException e
             (log-warn-error e "Ephemeral node disappeared between checking for existing and setting data")
             (zk/create-node zk path data :ephemeral acls)))
          (zk/create-node zk path data :ephemeral acls)))
      
      (create_sequential
        [this path data acls]
        (zk/create-node zk path data :sequential acls))
      
      (set_data
        [this path data acls]
        ;; note: this does not turn off any existing watches
        (if (zk/exists zk path false)
          (zk/set-data zk path data)
          (do
            (zk/mkdirs zk (parent-path path) acls)
            (zk/create-node zk path data :persistent acls))))
      
      (set_worker_hb
        [this path data acls]
        ;; note: this does not turn off any existing watches
        (.set_data this path data acls))

      (delete_node
        [this path]
        (zk/delete-recursive zk path))

      (delete-worker-hb
        [this path]
        (.delete_node this path))

      (get_data
        [this path watch?]
        (zk/get-data zk path watch?))

      (get_data_with_version
        [this path watch?]
        (zk/get-data-with-version zk path watch?))

      (get_version 
        [this path watch?]
        (zk/get-version zk path watch?))

      (get_worker_hb
        [this path watch?]
        (.get_data this path watch?))

      (get_children
        [this path watch?]
        (zk/get-children zk path watch?))

      (get_worker_hb_children
        [this path watch?]
        (.get_children this path watch?))

      (mkdirs
        [this path acls]
        (zk/mkdirs zk path acls))

      (node_exists
        [this path watch?]
        (zk/exists-node? zk path watch?))

      (close
        [this]
        (reset! active false)
        (.close zk)))))
