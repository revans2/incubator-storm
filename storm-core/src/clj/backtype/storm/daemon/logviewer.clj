(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
  (:use [backtype.storm.ui helpers])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [org.apache.log4j Level])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:require [compojure.route :as route]
            [compojure.handler :as handler])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(def LOGS-DIR (get-logdir-path))

(defn tail-file [path tail]
  (let [flen (.length (clojure.java.io/file path))
        skip (- flen tail)]
    (with-open [input (clojure.java.io/input-stream path)
                output (java.io.ByteArrayOutputStream.)]
      (if (> skip 0) (.skip input skip))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (let [size (.read input buffer)]
            (when (and (pos? size) (< (.size output) tail))
              (do (.write output buffer 0 size)
                  (recur))))))
      (.toString output))
    ))

(defn get-log-whitelist-file [fname]
  (if-let [prefix (second (re-matches #"(.*-\d+-\d+-worker-\d+).log" fname))]
    (clojure.java.io/file LOGS-DIR "metadata" (str prefix ".yaml"))))

(defn get-log-user-whitelist [fname]
  (try
    (let [wl-file (get-log-whitelist-file fname)
          m (.load (Yaml. (SafeConstructor.)) (java.io.FileReader. wl-file))]
      (if-let [whitelist (m LOGS-USERS)] whitelist []))
    (catch Exception ex
      (log-error ex))))

(defn authorized-log-user? [user fname]
  (if (or (blank? user) (blank? fname)) 
    nil
    (let [whitelist (get-log-user-whitelist fname)
          logs-users (concat (*STORM-CONF* LOGS-USERS) whitelist)]
       (some #(= % user) logs-users))))

(defn log-page [fname tail grep user]
  (let [path (str LOGS-DIR "/" fname)
        tail (if tail
               (min 10485760 (Integer/parseInt tail))
               10240)
        tail-string (tail-file path tail)]
    (if (or (blank? (*STORM-CONF* UI-FILTER))
            (authorized-log-user? user fname))
      (if grep
         (clojure.string/join "\n<br>"
           (filter #(.contains % grep) (.split tail-string "\n")))
         (.replaceAll tail-string "\n" "\n<br>"))

      (unauthorized-user-html user))))


(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log)
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defn log-template
  ([body] (log-template body nil))
  ([body user]
    (html
     [:head
      [:title "Storm log viewer"]
      (include-css "/css/bootstrap-1.1.0.css")
      (include-css "/css/style.css")
      (include-js "/js/jquery-1.6.2.min.js")
      (include-js "/js/jquery.tablesorter.min.js")
      (include-js "/js/jquery.cookies.2.2.0.min.js")
      (include-js "/js/script.js")
      ]
     [:body
      (concat
        (when (not (blank? user)) [[:div.ui-user [:p "User: " user]]])
        (seq body))
      ])))

(defroutes log-routes
  (GET "/log" [:as {servlet-request :servlet-request} & m]
       (let [user (get-servlet-user servlet-request)]
         (log-template (log-page (:file m) (:tail m) (:grep m) user) user)))
  (GET "/loglevel" [:as {servlet-request :servlet-request} & m]
       (let [user (get-servlet-user servlet-request)]
         (log-template (log-level-page (:name m) (:level m)) user) user))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/site log-routes)
 )

(defn start-logviewer [port]
  (run-jetty logapp {:port port}))

(defn -main []
  (let [conf (read-storm-config)]
    (start-logviewer (int (conf LOGVIEWER-PORT)))))
