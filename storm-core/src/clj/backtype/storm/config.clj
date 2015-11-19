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

(ns backtype.storm.config
  (:import [java.io FileReader File IOException]
           [java.nio.file Files LinkOption]
           [backtype.storm.generated StormTopology]
           [backtype.storm Config ConfigValidation$FieldValidator]
           [backtype.storm.utils Utils LocalState]
           [org.apache.commons.io FileUtils]
           [backtype.storm.generated InvalidTopologyException])
  (:require [clojure [string :as str]]
            [clojure.java [io :as io]]
            [clojure.set :as set])
  (:use [backtype.storm log util]))

(def RESOURCES-SUBDIR "resources")
(def NIMBUS-DO-NOT-REASSIGN "NIMBUS-DO-NOT-REASSIGN")

(defn- clojure-config-name [name]
  (.replace (.toUpperCase name) "_" "-"))

;; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (clojure-config-name name)]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))))

(def ALL-CONFIGS
  (dofor [f (seq (.getFields Config))]
         (.get f nil)))

(defmulti get-FieldValidator class-selector)

(defmethod get-FieldValidator nil [_]
  (throw (IllegalArgumentException. "Cannot validate a nil field.")))

(defmethod get-FieldValidator
  ConfigValidation$FieldValidator [validator] validator)

(defmethod get-FieldValidator Object
  [klass]
  {:pre [(not (nil? klass))]}
  (reify ConfigValidation$FieldValidator
    (validateField [this name v]
                   (if (and (not (nil? v))
                            (not (instance? klass v)))
                     (throw (IllegalArgumentException.
                              (str "field " name " '" v "' must be a '" (.getName klass) "'")))))))

;; Create a mapping of config-string -> validator
;; Config fields must have a _SCHEMA field defined
(def CONFIG-SCHEMA-MAP
  (->> (.getFields Config)
       (filter #(not (re-matches #".*_SCHEMA$" (.getName %))))
       (map (fn [f] [(.get f nil)
                     (get-FieldValidator
                       (-> Config
                           (.getField (str (.getName f) "_SCHEMA"))
                           (.get nil)))]))
       (into {})))

(defn cluster-mode
  [conf & args]
  (keyword (conf STORM-CLUSTER-MODE)))

(defn local-mode?
  [conf]
  (let [mode (conf STORM-CLUSTER-MODE)]
    (condp = mode
      "local" true
      "distributed" false
      (throw (IllegalArgumentException.
               (str "Illegal cluster mode in conf: " mode))))))

(defn sampling-rate
  [conf]
  (->> (conf TOPOLOGY-STATS-SAMPLE-RATE)
       (/ 1)
       int))

(defn mk-stats-sampler
  [conf]
  (even-sampler (sampling-rate conf)))

; storm.zookeeper.servers:
;     - "server1"
;     - "server2"
;     - "server3"
; nimbus.host: "master"
;
; ########### These all have default values as shown
;
; ### storm.* configs are general configurations
; # the local dir is where jars are kept
; storm.local.dir: "/mnt/storm"
; storm.zookeeper.port: 2181
; storm.zookeeper.root: "/storm"

(defn read-default-config
  []
  (clojurify-structure (Utils/readDefaultConfig)))

(defn get-keys-from-blob-store-map
  [blobstore-map]
  (for [[k v] blobstore-map]
    k))

(defn validate-topology-blob-store-map
  [storm-conf blob-store-key-set]
  (let [map-key-set (set (get-keys-from-blob-store-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)))
        common-blobs (set/intersection blob-store-key-set map-key-set)
        missing-blobs (set/difference map-key-set common-blobs)]
    (if (not (empty? missing-blobs))
      (throw (InvalidTopologyException. (str "Blob store does not contain the keys "
                                          missing-blobs " mentioned in the map"))))))

(defn validate-configs-with-schemas
  [conf]
  (doseq [[k v] conf
          :let [schema (CONFIG-SCHEMA-MAP k)]]
    (if (not (nil? schema))
      (.validateField schema k v))))

(defn read-storm-config
  []
  (let [conf (clojurify-structure (Utils/readStormConfig))]
    (validate-configs-with-schemas conf)
    conf))

(defn read-yaml-config
  ([name must-exist]
     (let [conf (clojurify-structure (Utils/findAndReadConfigFile name must-exist))]
       (validate-configs-with-schemas conf)
       conf))
  ([name]
     (read-yaml-config true)))

(defn absolute-storm-local-dir [conf]
  (let [storm-home (System/getProperty "storm.home")
        path (conf STORM-LOCAL-DIR)]
    (if path
      (if (is-absolute-path? path) path (str storm-home file-path-separator path))
      (str storm-home file-path-separator "storm-local"))))

(defn master-local-dir
  [conf]
  (let [ret (str (absolute-storm-local-dir conf) file-path-separator "nimbus")]
    (FileUtils/forceMkdir (File. ret))
    ret))

(defn get-id-from-blob-key
  [key]
    (if-let [groups (re-find #"^(.*)((-stormjar\.jar)|(-stormcode\.ser)|(-stormconf\.ser))$" key)]
      (nth groups 1)))

(defn master-stormjar-key
  [topology-id]
  (str topology-id "-stormjar.jar"))

(defn master-stormcode-key
  [topology-id]
  (str topology-id "-stormcode.ser"))

(defn master-stormconf-key
  [topology-id]
  (str topology-id "-stormconf.ser"))

(defn master-inbox
  [conf]
  (let [ret (str (master-local-dir conf) file-path-separator "inbox")]
    (FileUtils/forceMkdir (File. ret))
    ret))

(defn master-inimbus-dir
  [conf]
  (str (master-local-dir conf) file-path-separator "inimbus"))

(defn supervisor-local-dir
  [conf]
  (let [ret (str (absolute-storm-local-dir conf) file-path-separator "supervisor")]
    (FileUtils/forceMkdir (File. ret))
    ret))

(defn supervisor-isupervisor-dir
  [conf]
  (str (supervisor-local-dir conf) file-path-separator "isupervisor"))

(defn supervisor-stormdist-root
  ([conf]
   (str (supervisor-local-dir conf) file-path-separator "stormdist"))
  ([conf storm-id]
   (str (supervisor-stormdist-root conf) file-path-separator (url-encode storm-id))))

(defn supervisor-stormjar-path
  [stormroot]
  (str stormroot file-path-separator "stormjar.jar"))

(defn supervisor-stormcode-path
  [stormroot]
  (str stormroot file-path-separator "stormcode.ser"))

(defn supervisor-stormconf-path
  [stormroot]
  (str stormroot file-path-separator "stormconf.ser"))

(defn supervisor-tmp-dir
  [conf]
  (let [ret (str (supervisor-local-dir conf) file-path-separator "tmp")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn supervisor-storm-resources-path
  [stormroot]
  (str stormroot file-path-separator RESOURCES-SUBDIR))

(defn ^LocalState supervisor-state
  [conf]
  (LocalState. (str (supervisor-local-dir conf) file-path-separator "localstate")))

(defn ^LocalState nimbus-topo-history-state
  [conf]
  (LocalState. (str (master-local-dir conf) file-path-separator "history")))

(defn read-supervisor-storm-conf-given-path
  [conf stormconf-path]
    (merge conf (clojurify-structure (Utils/fromCompressedJsonConf (FileUtils/readFileToByteArray (File. stormconf-path))))))

(defn read-supervisor-storm-conf
  [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        conf-path (supervisor-stormconf-path stormroot)
        topo-conf-file (File. conf-path)
        _ (if-not (.exists topo-conf-file) (log-warn (str "The topology configuration file is missing: " conf-path)))
        topo-conf (if (.exists topo-conf-file)
                    (clojurify-structure
                      (Utils/fromCompressedJsonConf (FileUtils/readFileToByteArray topo-conf-file))))]
    (merge conf topo-conf)))

(defn read-supervisor-topology
  [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        topology-path (supervisor-stormcode-path stormroot)]
    (Utils/deserialize (FileUtils/readFileToByteArray (File. topology-path)) StormTopology)))

(defn worker-user-root [conf]
  (str (absolute-storm-local-dir conf) "/workers-users"))

(defn worker-user-file [conf worker-id]
  (str (worker-user-root conf) "/" worker-id))
  
(defn set-worker-user! [conf worker-id user]
  (log-debug "SET worker-user " worker-id " " user)
  (let [file (worker-user-file conf worker-id)]
    (.mkdirs (.getParentFile (File. file)))
    (spit (worker-user-file conf worker-id) user)))

(defn remove-worker-user! [conf worker-id]
  (log-debug "REMOVE worker-user " worker-id)
  (.delete (File. (worker-user-file conf worker-id))))

(defn worker-artifacts-root
  ([conf]
   (str (conf STORM-LOCAL-DIR) file-path-separator "workers-artifacts"))
  ([conf id]
   (str (worker-artifacts-root conf) file-path-separator id))
  ([conf id port]
   (str (worker-artifacts-root conf id) file-path-separator port)))

(defn worker-artifacts-pid-path
  [conf id port]
  (str (worker-artifacts-root conf id port) file-path-separator "worker.pid"))

(defn get-log-metadata-file
  ([fname]
    (let [[id port & _] (str/split fname (re-pattern file-path-separator))]
      (get-log-metadata-file (read-storm-config) id port)))
  ([conf id port]
    (clojure.java.io/file (str (worker-artifacts-root conf id) file-path-separator port file-path-separator) "worker.yaml")))

(defn get-worker-dir-from-root
  [log-root id port]
  (clojure.java.io/file (str log-root file-path-separator id file-path-separator port)))

(defn worker-root
  ([conf]
   (str (absolute-storm-local-dir conf) file-path-separator "workers"))
  ([conf id]
   (str (worker-root conf) file-path-separator id)))

(defn worker-pids-root
  [conf id]
  (str (worker-root conf id) file-path-separator "pids"))

(defn worker-pid-path
  [conf id pid]
  (str (worker-pids-root conf id) file-path-separator pid))

(defn get-file-owner
  "Get the owner of the file at a path, or throw NoSuchFileException."
  [^String path]
  (.getName
   (Files/getOwner
    (.toPath (io/file path))
    (make-array LinkOption 0))))

(defn get-worker-user [conf worker-id]
  (log-debug "GET worker-user " worker-id)
  (let [user
        (try 
          (str/trim (slurp (worker-user-file conf worker-id)))
          (catch IOException e))]
    (if (and (not (= user nil))
             (not (= user "")))
      user
      (try
        (let [some-pidfile (.getPath (first (file-seq (io/file (worker-pids-root conf worker-id)))))]
          (get-file-owner some-pidfile))
        (catch IOException e
          (log-warn-error e "Failed to get worker user for " worker-id "."))))))

(defn worker-heartbeats-root
  [conf id]
  (str (worker-root conf id) file-path-separator "heartbeats"))

;; workers heartbeat here with pid and timestamp
;; if supervisor stops receiving heartbeat, it kills and restarts the process
;; in local mode, keep a global map of ids to threads for simulating process management
(defn ^LocalState worker-state
  [conf id]
  (LocalState. (worker-heartbeats-root conf id)))

(defn get-topo-logs-users
  [topology-conf]
  (sort (distinct (remove nil?
                    (concat
                      (topology-conf LOGS-USERS)
                      (topology-conf TOPOLOGY-USERS))))))

(defn get-topo-logs-groups
  [topology-conf]
  (sort (distinct (remove nil?
                    (concat
                      (topology-conf LOGS-GROUPS)
                      (topology-conf TOPOLOGY-GROUPS))))))

(defn override-login-config-with-system-property [conf]
  (if-let [login_conf_file (System/getProperty "java.security.auth.login.config")]
    (assoc conf "java.security.auth.login.config" login_conf_file)
    conf))
