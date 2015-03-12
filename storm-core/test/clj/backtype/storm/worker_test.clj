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
(ns backtype.storm.worker-test
  (:use [clojure test])
  (:require [backtype.storm [util :as util]])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:import [backtype.storm.generated LogConfig LogLevel LogLevelAction])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:require [conjure.core])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:import [org.apache.logging.log4j Level])
  (:import [org.apache.logging.log4j LogManager])
  (:import [org.slf4j Logger])
  (:use [conjure core])
  )

(bootstrap)

(deftest test-log-reset-should-not-trigger-for-future-time
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          the-future (coerce/to-long (time/plus present (time/secs 1)))
          mock-config {"foo" {:timeout the-future}}]
      (stubbing [time/now present]
        (def mock-config-atom (atom mock-config))
        (#'worker/reset-log-levels mock-config-atom)
        ;; if the worker doesn't reset log levels, the atom should not be nil
        (is (not(= @mock-config-atom nil))))
      )))

(deftest test-log-reset-triggers-for-past-time
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (time/plus present (time/secs -1))
          mock-config {"foo" { :timeout (coerce/to-long past)
                               :target-log-level Level/INFO
                               :reset-log-level Level/WARN}}]
      (stubbing [time/now present]
        (def mock-config-atom (atom mock-config))
        (#'worker/reset-log-levels mock-config-atom)
        ;; the logger config is removed from atom
        (is (= @mock-config-atom {})))
      )))

(deftest test-log-reset-resets-does-nothing-for-empty-log-config
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {}
          result (atom false)]
      ;; the mocking function from conjure doesn't like private functions
      (with-redefs-fn {#'worker/set-logger-level 
          (fn [logger-name level] (reset! result true))}
        #(stubbing [time/now present]
          (def mock-config-atom (atom mock-config))
          (#'worker/reset-log-levels mock-config-atom)
          ;; if the worker resets log level, the atom is nil'ed out
          (is (= @mock-config-atom {}))
          ;; test that the set-logger-level function was not called
          (is (= @result false)))))))

(deftest test-log-reset-resets-root-logger-if-set
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {LogManager/ROOT_LOGGER_NAME  {:timeout past
                                                     :target-log-level Level/DEBUG
                                                     :reset-log-level Level/WARN}}
          result (atom {})]

      ;; the mocking function from conjure doesn't like private functions
      (with-redefs-fn {#'worker/set-logger-level 
          (fn [logger-name level] (reset! result (assoc @result logger-name level)))}
        #(stubbing [time/now present]
          (def mock-config-atom (atom mock-config))
          (#'worker/reset-log-levels mock-config-atom)
          ;; if the worker resets log level, the atom is reset to {}
          (is (= @mock-config-atom {}))
          ;; ensure we reset back to WARN level
          (is (= (get @result LogManager/ROOT_LOGGER_NAME) Level/WARN)))))))

(deftest test-log-resets-named-loggers-with-past-timeout
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {"my_debug_logger" {:timeout past
                                          :target-log-level Level/DEBUG
                                          :reset-log-level Level/INFO} 
                       "my_info_logger" {:timeout past
                                         :target-log-level Level/INFO
                                         :reset-log-level Level/WARN}
                       "my_error_logger" {:timeout past
                                          :target-log-level Level/ERROR
                                          :reset-log-level Level/INFO}}
          result (atom {})]
      (with-redefs-fn {#'worker/set-logger-level 
          (fn [logger-name level] (reset! result (assoc @result logger-name level)))}
        #(stubbing [time/now present]
          (def mock-config-atom (atom mock-config))
          (#'worker/reset-log-levels mock-config-atom)
          ;; if the worker resets log level, the atom is reset to {}
          (is (= @mock-config-atom {}))
          (log-message @result)
          (is (= (get @result "my_debug_logger") Level/INFO))
          ;; the info logger didn't need to be reset
          (is (= (get @result "my_info_logger") Level/WARN))
          ;; the error logger didn't need to be reset
          (is (= (get @result "my_error_logger") Level/INFO))
          ))
      )))

(deftest test-process-empty-log-level-changes-nothing
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          mock-config (LogConfig.)
          mock-config-atom (atom nil)
          orig-levels (atom {})
          result (atom {})]
      ;; the mocking function from conjure doesn't like private functions
      (with-redefs-fn {#'worker/set-logger-level 
        (fn [logger-name level] (reset! result (assoc @result logger-name level)))}
          #(#'worker/process-log-config-change mock-config-atom orig-levels mock-config))
      ;; test that the set-logger-level function was not called
      (log-message @result)
      (is (= @result {})))))

(deftest test-process-root-log-level-to-debug-sets-logger-and-timeout
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          mock-config (LogConfig.)
          root-level (LogLevel.)
          mock-config-atom (atom nil)
          orig-levels (atom {})
          present (time/now)
          in-thirty-seconds (coerce/to-long (time/plus present (time/secs 30)))
          result (atom {})]
      ;; configure the root logger to be debug
      (.set_reset_log_level_timeout_epoch root-level in-thirty-seconds)
      (.set_target_log_level root-level "DEBUG")
      (.set_action root-level LogLevelAction/UPDATE)
      (.put_to_named_logger_level mock-config "ROOT" root-level)
      ;; the mocking function from conjure doesn't like private functions
      (with-redefs-fn {#'worker/set-logger-level 
        (fn [logger-name level] (reset! result (assoc @result logger-name level)))}
        #(stubbing [time/now present]
          (#'worker/process-log-config-change mock-config-atom orig-levels mock-config)))
      ;; test that the set-logger-level function was not called
      (log-message "Tests " @mock-config-atom)
      (is (= (get @result LogManager/ROOT_LOGGER_NAME) Level/DEBUG))
      (log-message "the config ends up being " @mock-config-atom)
      (let [root-result (get @mock-config-atom LogManager/ROOT_LOGGER_NAME)]
        (is (= (:action root-result) LogLevelAction/UPDATE))
        (is (= (:target-log-level root-result) Level/DEBUG))
        ;; defaults to INFO level when the logger isn't found previously
        (is (= (:reset-log-level root-result) Level/INFO))
        (is (= (:timeout root-result) in-thirty-seconds))))))

(deftest test-process-root-log-level-to-debug-sets-logger-and-timeout
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          mock-config (LogConfig.)
          root-level (LogLevel.)
          orig-levels (atom {})
          present (time/now)
          in-thirty-seconds (coerce/to-long (time/plus present (time/secs 30)))
          result (atom {})]
      ;; configure the root logger to be debug
      (doseq [named {"ROOT" "DEBUG"
                     "my_debug_logger" "DEBUG"
                     "my_info_logger" "INFO"
                     "my_error_logger" "ERROR"}]
        (let [level (LogLevel.)]
          (.set_action level LogLevelAction/UPDATE)
          (.set_reset_log_level_timeout_epoch level in-thirty-seconds)
          (.set_target_log_level level (val named))
          (.put_to_named_logger_level mock-config (key named) level)))
      (log-message "Tests " mock-config)
      ;; the mocking function from conjure doesn't like private functions
      (with-redefs-fn {#'worker/set-logger-level 
        (fn [logger-name level] (reset! result (assoc @result logger-name level)))}
        #(stubbing [time/now present]
          (#'worker/process-log-config-change mock-config-atom orig-levels mock-config)))
      ;; test that the set-logger-level function was not called
      (log-message "Tests " @result)
      (is (= (get @result LogManager/ROOT_LOGGER_NAME) Level/DEBUG))
      (is (= (get @result "my_debug_logger") Level/DEBUG))
      (is (= (get @result "my_info_logger") Level/INFO))
      (is (= (get @result "my_error_logger") Level/ERROR)))))
