(ns backtype.storm.cluster-state.zookeeper-state
  (:import [org.apache.zookeeper KeeperException KeeperException$NoNodeException ZooDefs ZooDefs$Ids ZooDefs$Perms])
  (:use [backtype.storm cluster config log util])
  (:require [backtype.storm [zookeeper :as zk] [heartbeatsutil :as hbu]])
  (:gen-class
   :implements [backtype.storm.cluster.ClusterStateFactory]))

(defn -mkState [this conf auth-conf acls]
  (let [conf (into {} conf)
        auth-conf (if auth-conf (into {} auth-conf))]
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
                                            (callback type path))))))
          use-hbserver (Boolean/valueOf (conf HBSERVER-ROUTE-WORKER-HEARTBEATS))]
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

        (set-ephemeral-node
          [this path data acls]
          (zk/mkdirs zk (parent-path path) acls)
          (if (zk/exists zk path false)
            (try-cause
             (zk/set-data zk path data) ; should verify that it's ephemeral
             (catch KeeperException$NoNodeException e
               (log-warn-error e "Ephemeral node disappeared between checking for existing and setting data")
               (zk/create-node zk path data :ephemeral acls)))
            (zk/create-node zk path data :ephemeral acls)))

        (create-sequential
          [this path data acls]
          (zk/create-node zk path data :sequential acls))

        (set-data
          [this path data acls]
          ;; note: this does not turn off any existing watches
          (if (zk/exists zk path false)
            (zk/set-data zk path data)
            (do
              (zk/mkdirs zk (parent-path path) acls)
              (zk/create-node zk path data :persistent acls))))

        (set-worker-hb
          [this path data acls]
          ;; note: this does not turn off any existing watches
          (if use-hbserver
            (hbu/send-pulse path data)
            (set-data this path data acls)))

        (delete-node
          [this path]
          (zk/delete-recursive zk path))

        (delete-worker-hb
          [this path]
          (if use-hbserver
            (hbu/delete-pulse-recursive path)
            (delete-node this path)))

        (get-data
          [this path watch?]
          (zk/get-data zk path watch?))

        (get-data-with-version
          [this path watch?]
          (zk/get-data-with-version zk path watch?))

        (get-version 
          [this path watch?]
          (zk/get-version zk path watch?))

        (get-worker-hb
          [this path watch?]
          (if use-hbserver
            (hbu/get-pulse-data path)
            (get-data this path watch?)))

        (get-children
          [this path watch?]
          (zk/get-children zk path watch?))

        (get-worker-hb-children
          [this path watch?]
          (if use-hbserver
            (hbu/get-pulse-children path)
            (get-children this path watch?)))

        (mkdirs
          [this path acls]
          (zk/mkdirs zk path acls))

        (exists-node?
          [this path watch?]
          (zk/exists-node? zk path watch?))

        (close
          [this]
          (reset! active false)
          (.close zk))))))
