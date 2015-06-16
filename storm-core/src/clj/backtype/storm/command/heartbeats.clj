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
(ns backtype.storm.command.heartbeats
  (:use [backtype.storm config log cluster])
  (:gen-class))

(defn -main [command path & args]
  (let [conf (read-storm-config)
        cluster (mk-distributed-cluster-state conf :auth-conf conf)]
    (println "Command: [" command "]")
    (condp = command
      "list"
      (log-message "Heartbeats: " (.get_worker_hb_children cluster path false))
      
      "get"
      (log-message "HB: " (.get_worker_hb cluster path false))

      (log-message "Usage: heartbeats [list|get] path"))
    
    (try
      (log-message "Closing cluster.")
      (.close cluster)
      (catch Exception e
        (log-message "Caught exception: " e " on close."))))
  (System/exit 0))
         
