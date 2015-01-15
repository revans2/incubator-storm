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

(ns backtype.storm.heartbeatsutil
  (:import [backtype.storm.utils Utils ZookeeperAuthInfo HBClient])
  (:import [backtype.storm.generated HBNodes HBRecords Pulse ])
  (:use [backtype.storm util log config]))

(defn get-pulse-data [conf id]
  (let [_ (log-message "Getting hb pulse for id: " id)]
    (try (.. (HBClient/getConfiguredClient conf) (getClient) (getPulse id) (get_details))
      (catch Exception e (log-error e (str "Failed to get hb for id: [" id "]."))))))

(defn send-pulse [conf id details]
  (let [_ (log-message "Sending pulse to hbserver for id: " id " data [ " (str details) "].")]
  (try (.. (HBClient/getConfiguredClient conf) (getClient) (sendPulse (doto (Pulse. ) (.set_id id) (.set_details details))))
    (catch Exception e (log-error e (str "Failed to send pulse for id [" id "]."))))))

(defn get-pulse-children [conf path]
  (let [_ (log-message "Getting children from hbserver for path: " path)]
  (try (.. (HBClient/getConfiguredClient conf)  (getClient) (getAllNodesForPath path) (get_pulseIds))
    (catch Exception e (log-error e (str "Failed to get children from hbserver for path: [" path "]."))))))

(defn delete-pulse-recursive [conf path]
  (let [_ (log-message "Deleting all pulses from hbserver for path: " path)]
  (try (.. (HBClient/getConfiguredClient conf) (getClient) (deletePath path))
    (catch Exception e (log-error e (str "Failed to delete all pulses from hbserver for path: [" path "]."))))))

(defn delete-pulse [conf id]
  (let [_ (log-message "Deleting the pulse from hbserver for id: " id)]
  (try (.. (HBClient/getConfiguredClient conf) (getClient) (deletePulseId id))
    (catch Exception e (log-error e (str "Failed to delete the pulse from hbserver for id: [" id "]."))))))



