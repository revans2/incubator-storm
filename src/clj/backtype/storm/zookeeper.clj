(ns backtype.storm.zookeeper
  (:import [com.netflix.curator.retry RetryNTimes])
  (:import [com.netflix.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory])
  (:import [java.net InetSocketAddress BindException])
  (:import [java.io File])
  (:import [backtype.storm.utils Utils ZookeeperAuthInfo ZookeeperServerCnxnFactory])
  (:use [backtype.storm util log config]))

(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired
  })

(def zk-event-types
  {Watcher$Event$EventType/None :none
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed
  })

(defn- default-watcher [state type path]
  (log-message "Zookeeper state update: " state type path))

(defnk mk-client [conf servers port :root "" :watcher default-watcher :auth-conf nil]
  (let [fk (Utils/newCurator conf servers port root (when auth-conf (ZookeeperAuthInfo. auth-conf)))]
    (.. fk
        (getCuratorListenable)
        (addListener
         (reify CuratorListener
           (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
             (when (= (.getType e) CuratorEventType/WATCHED)                  
               (let [^WatchedEvent event (.getWatchedEvent e)]
                 (watcher (zk-keeper-states (.getState event))
                          (zk-event-types (.getType event))
                          (.getPath event))))))))
;;    (.. fk
;;        (getUnhandledErrorListenable)
;;        (addListener
;;         (reify UnhandledErrorListener
;;           (unhandledError [this msg error]
;;             (if (or (exception-cause? InterruptedException error)
;;                     (exception-cause? java.nio.channels.ClosedByInterruptException error))
;;               (do (log-warn-error error "Zookeeper exception " msg)
;;                   (let [to-throw (InterruptedException.)]
;;                     (.initCause to-throw error)
;;                     (throw to-throw)
;;                     ))
;;               (do (log-error error "Unrecoverable Zookeeper error " msg)
;;                   (halt-process! 1 "Unrecoverable Zookeeper error")))
;;             ))))
    (.start fk)
    fk))

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT
   :sequential CreateMode/PERSISTENT_SEQUENTIAL})

(defn create-node
  ([^CuratorFramework zk ^String path ^bytes data mode]
    (.. zk (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath (normalize-path path) data)))
  ([^CuratorFramework zk ^String path ^bytes data]
    (create-node zk path data :persistent)))

(defn exists-node? [^CuratorFramework zk ^String path watch?]
  ((complement nil?)
    (if watch?
       (.. zk (checkExists) (watched) (forPath (normalize-path path))) 
       (.. zk (checkExists) (forPath (normalize-path path))))))

(defnk delete-node [^CuratorFramework zk ^String path :force false]
  (try-cause  (.. zk (delete) (forPath (normalize-path path)))
    (catch KeeperException$NoNodeException e
      (when-not force (throw e))
      )))

(defn mkdirs [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try-cause
        (create-node zk path (barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data [^CuratorFramework zk ^String path watch?]
  (let [path (normalize-path path)]
    (try-cause
      (if (exists-node? zk path watch?)
        (if watch?
          (.. zk (getData) (watched) (forPath path))
          (.. zk (getData) (forPath path))))
    (catch KeeperException$NoNodeException e
      ;; this is fine b/c we still have a watch from the successful exists call
      nil ))))

(defn get-children [^CuratorFramework zk ^String path watch?]
  (if watch?
    (.. zk (getChildren) (watched) (forPath (normalize-path path)))
    (.. zk (getChildren) (forPath (normalize-path path)))))

(defn set-data [^CuratorFramework zk ^String path ^bytes data]
  (.. zk (setData) (forPath (normalize-path path) data)))

(defn exists [^CuratorFramework zk ^String path watch?]
  (exists-node? zk path watch?))

(defn delete-recursive [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when (exists-node? zk path false)
      (let [children (try-cause (get-children zk path false)
                                (catch KeeperException$NoNodeException e
                                  []
                                  ))]
        (doseq [c children]
          (delete-recursive zk (full-path path c)))
        (delete-node zk path :force true)
        ))))

(defnk mk-inprocess-zookeeper [localdir :port nil]
  (let [localfile (File. localdir)
        zk (ZooKeeperServer. localfile localfile 2000)
        input_port (if port port 0)
        cnxnFactory (ZookeeperServerCnxnFactory. input_port 10)
        factory (.factory cnxnFactory) 
        retport (.port cnxnFactory) ]
    (log-message "Starting inprocess zookeeper at port " retport " and dir " localdir)    
    (.startup factory zk)
    [retport factory]
    ))

(defn shutdown-inprocess-zookeeper [handle]
  (.shutdown handle))
