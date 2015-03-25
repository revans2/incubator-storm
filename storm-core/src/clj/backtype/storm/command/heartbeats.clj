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
  (:import [java.io InputStream OutputStream])
  (:import [backtype.storm.generated  AuthorizationException
            KeyNotFoundException]) ;HBNodes Pulse HBRecords 
  (:use [clojure.string :only [split]])
  (:use [clojure.tools.cli :only [cli]])
  (:use [clojure.java.io :only [copy input-stream output-stream]])
  (:use [backtype.storm log]) ; [heartbeatsutil :as hbu]
  (:gen-class))

;(defn list-hbs [args]
;    (let [keys (if (empty? args) ["/"] args)]
;      (doseq [key keys]
;        (let [path (if (.endsWith key "/") (clojure.string/replace key #"\/$" "") key)]
;          (try
;            (doseq [id (hbu/get-pulse-children path)]
;              (log-message (str path "/" id)))
;            (catch AuthorizationException ae
;                                          (if-not (empty? args) (log-message "ACCESS DENIED to key: " key)))
;            (catch KeyNotFoundException knf
;                                        (if-not (empty? args) (log-message key " NOT FOUND"))))))))
;

(defn -main [& args])
;  (let [command (first args)
;        new-args (rest args)]
;    (condp = command
;             "list" (list-hbs new-args)
;             :else (throw (RuntimeException. (str command " is not a supported heartbeats command"))))))
