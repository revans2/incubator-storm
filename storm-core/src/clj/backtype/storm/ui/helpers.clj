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
(ns backtype.storm.ui.helpers
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [clojure
         [string :only [blank? join]]
         [walk :only [keywordize-keys]]])
  (:use [backtype.storm config log])
  (:use [backtype.storm.util :only [clojurify-structure uuid defnk to-json
                                    url-encode]])
  (:use [clj-time coerce format])
  (:import [backtype.storm.generated ExecutorInfo ExecutorSummary])
  (:import [org.mortbay.jetty.security SslSocketConnector])
  (:import (org.mortbay.jetty Server)
           (org.mortbay.jetty.bio SocketConnector))
  (:require [ring.util servlet])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]))

(defn split-divide [val divider]
  [(Integer. (int (/ val divider))) (mod val divider)]
  )

(def PRETTY-SEC-DIVIDERS
     [["s" 60]
      ["m" 60]
      ["h" 24]
      ["d" nil]])

(def PRETTY-MS-DIVIDERS
     (cons ["ms" 1000]
           PRETTY-SEC-DIVIDERS))

(defn pretty-uptime-str* [val dividers]
  (let [val (if (string? val) (Integer/parseInt val) val)
        vals (reduce (fn [[state val] [_ divider]]
                       (if (pos? val)
                         (let [[divided mod] (if divider
                                               (split-divide val divider)
                                               [nil val])]
                           [(concat state [mod])
                            divided]
                           )
                         [state val]
                         ))
                     [[] val]
                     dividers)
        strs (->>
              (first vals)
              (map
               (fn [[suffix _] val]
                 (str val suffix))
               dividers
               ))]
    (join " " (reverse strs))
    ))

(defn pretty-uptime-sec [secs]
  (pretty-uptime-str* secs PRETTY-SEC-DIVIDERS))

(defn pretty-uptime-ms [ms]
  (pretty-uptime-str* ms PRETTY-MS-DIVIDERS))


(defelem table [headers-map data]
  [:table
   [:thead
    [:tr
     (for [h headers-map]
       [:th (if (:text h) [:span (:attr h) (:text h)] h)])
     ]]
   [:tbody
    (for [row data]
      [:tr
       (for [col row]
         [:td col]
         )]
      )]
   ])

(defn float-str [n]
  (if n
    (format "%.3f" (float n))
    "0"
    ))

(defn swap-map-order [m]
  (->> m
       (map (fn [[k v]]
              (into
               {}
               (for [[k2 v2] v]
                 [k2 {k v2}]
                 ))
              ))
       (apply merge-with merge)
       ))

(defn date-str [secs]
  (let [dt (from-long (* 1000 (long secs)))]
    (unparse (:rfc822 formatters) dt)
    ))

(defn url-format [fmt & args]
  (String/format fmt 
    (to-array (map #(url-encode (str %)) args))))

(defn to-tasks [^ExecutorInfo e]
  (let [start (.get_task_start e)
        end (.get_task_end e)]
    (range start (inc end))
    ))

(defn sum-tasks [executors]
  (reduce + (->> executors
                 (map #(.get_executor_info ^ExecutorSummary %))
                 (map to-tasks)
                 (map count))))

(defn pretty-executor-info [^ExecutorInfo e]
  (str "[" (.get_task_start e) "-" (.get_task_end e) "]"))

(defn unauthorized-user-json
  [user]
  {"error" "No Authorization"
   "errorMessage" (str "User " user " is not authorized.")})

(defn unauthorized-user-html [user]
  [[:h2 "User '" (escape-html user) "' is not authorized."]])

(defn- mk-ssl-connector [port ks-path ks-password ks-type]
  (doto (SslSocketConnector.)
    (.setExcludeCipherSuites (into-array String ["SSL_RSA_WITH_RC4_128_MD5" "SSL_RSA_WITH_RC4_128_SHA"]))
    (.setAllowRenegotiate false)
    (.setKeystore ks-path)
    (.setKeystoreType ks-type)
    (.setKeyPassword ks-password)
    (.setPassword ks-password)
    (.setPort port)))

(defn config-ssl [server port ks-path ks-password ks-type]
  (when (> port 0)
    (.addConnector server (mk-ssl-connector port ks-path ks-password ks-type))))

(defn config-filter [server handler filters-confs]
  (if filters-confs
    (let [servlet-holder (org.mortbay.jetty.servlet.ServletHolder.
                           (ring.util.servlet/servlet handler))
          context (doto (org.mortbay.jetty.servlet.Context. server "/")
                    (.addServlet servlet-holder "/"))]
      (doseq [{:keys [filter-name filter-class filter-params]} filters-confs]
        (if filter-class
          (let [filter-holder (doto (org.mortbay.jetty.servlet.FilterHolder.)
                                (.setClassName filter-class)
                                (.setName (or filter-name filter-class))
                                (.setInitParameters (or filter-params {})))]
            (.addFilter context filter-holder "/*" org.mortbay.jetty.Handler/ALL))))
      (.addHandler server context))))

(defn ring-response-from-exception [ex]
  {:headers {}
   :status 400
   :body (.getMessage ex)})

;; Stolen right from ring.adapter.jetty
(defn- jetty-create-server
  "Construct a Jetty Server instance."
  [options]
  (let [connector (doto (SocketConnector.)
                    (.setPort (options :port))
                    (.setHost (options :host)))
        server (doto (Server.)
                 (.addConnector connector)
                 (.setSendDateHeader true))]
    server))

(defn storm-run-jetty
  "Modified version of run-jetty
  Must contain configurator, and configurator must set handler."
  [config]
  (let [#^Server s (jetty-create-server (dissoc config :configurator))
        configurator (:configurator config)]
    (configurator s)
    (.start s)))

(defn json-response [data & [status]]
  {:status (or status 200)
   :headers {"Content-Type" "application/json"}
   :body (to-json data)})

(defn exception->json
  [ex]
  {"error" "Internal Server Error"
   "errorMessage"
   (let [sw (java.io.StringWriter.)]
     (.printStackTrace ex (java.io.PrintWriter. sw))
     (.toString sw))})
