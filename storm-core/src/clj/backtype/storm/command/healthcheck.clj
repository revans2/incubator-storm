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
(ns backtype.storm.command.healthcheck
  (:require [backtype.storm
             [config :refer :all]
             [log :refer :all]]
            [clojure.java [io :as io]]
            [clojure [string :refer [split]]])
  (:gen-class))

(defn check-for-errors [] nil)

(defn interrupt-after [thread ms]
  (let [thread (Thread.
                (fn []
                  (try
                    (Thread/sleep ms)
                    (.interrupt thread)
                    (catch InterruptedException e))))]
    (.start thread)
    thread))

(defn check-output [lines]
  (if (some #(.startsWith % "ERROR") lines)
    :failed
    :success))

(defn process-script [script]
  (let [p (. (Runtime/getRuntime) (exec script))
        curthread (Thread/currentThread)
        t (interrupt-after curthread 5000)
        ret (try
              (.waitFor p)
              (.interrupt t)
              (if (not (= (.exitValue p) 0))
                :failed_with_exit_code
                (check-output (split
                               (slurp (.getInputStream p))
                               #"\n+")))
              (catch InterruptedException e
                (println "Script" script "timed out.")
                :timeout)
              (catch Exception e
                (println "Script failed with exception: " e)
                :failed_with_exception))]
    (.interrupt t)
    ret))

(defn -main [& args]
  (let [conf (read-storm-config)
        health-dir (conf STORM-HEALTH-DIR)
        health-files (file-seq (io/file health-dir))
        health-scripts (filter #(and (.canExecute %)
                                     (not (.isDirectory %)))
                               health-files)
        results (->> health-scripts
                     (map #(do (.getAbsolutePath %)))
                     (map process-script))]
    (println (pr-str results))
    (System/exit
     (if (every? #(or (= % :failed_with_exit_code)
                      (= % :success))
                 results)
       0
       1))))
