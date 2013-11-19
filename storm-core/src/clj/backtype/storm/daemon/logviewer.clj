(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [clojure.set :only [difference]])
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [java.io File FileFilter])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [org.apache.log4j Level])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defn cleanup-cutoff-age-millis [conf now-millis]
  (- now-millis (* (conf LOGVIEWER-CLEANUP-AGE-MINS) 60 1000)))

(defn mk-FileFilter-for-log-cleanup [conf now-millis]
  (let [cutoff-age-millis (cleanup-cutoff-age-millis conf now-millis)]
    (reify FileFilter (^boolean accept [this ^File file]
                        (boolean (and
                          (.isFile file)
                          (re-find worker-log-filename-pattern (.getName file))
                          (<= (.lastModified file) cutoff-age-millis)))))))

(defn select-files-for-cleanup [conf now-millis]
  (let [file-filter (mk-FileFilter-for-log-cleanup conf now-millis)]
    (.listFiles (File. LOG-DIR) file-filter)))

(defn get-metadata-file-for-log-root-name [root-name]
  (let [metaFile (clojure.java.io/file LOG-DIR "metadata"
                                       (str root-name ".yaml"))]
    (if (.exists metaFile)
      metaFile
      (do
        (log-warn "Could not find " (.getCanonicalPath metaFile)
                  " to clean up for " root-name)
        nil))))

(defn get-worker-id-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) "worker-id"))

(defn get-topo-owner-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) TOPOLOGY-SUBMITTER-USER))

(defn get-log-root->files-map [log-files]
  "Returns a map of \"root name\" to a the set of files in log-files having the
  root name.  The \"root name\" of a log file is the part of the name preceding
  the extension."
  (reduce #(assoc %1                                      ;; The accumulated map so far
                  (first %2)                              ;; key: The root name of the log file
                  (conj (%1 (first %2) #{}) (second %2))) ;; val: The set of log files with the root name
          {}                                              ;; initial (empty) map
          (map #(list
                  (second (re-find worker-log-filename-pattern (.getName %))) ;; The root name of the log file
                  %)                                                          ;; The log file
               log-files)))

(defn identify-worker-log-files [log-files]
  (into {} (for [log-root-entry (get-log-root->files-map log-files)
                 :let [metaFile (get-metadata-file-for-log-root-name
                                  (key log-root-entry))
                       log-root (key log-root-entry)
                       files (val log-root-entry)]
                 :when metaFile]
             {(get-worker-id-from-metadata-file metaFile)
              ;; Delete the metadata file also if each log for this root name
              ;; is to be deleted.
              {:owner (get-topo-owner-from-metadata-file metaFile)
               :files
                 (if (empty? (difference
                                  (set (filter #(re-find (re-pattern log-root) %)
                                               (read-dir-contents LOG-DIR)))
                                  (set (map #(.getName %) files))))
                  (conj files metaFile)
                  files)}})))

(defn get-files-of-dead-workers [conf now-secs log-files]
  (if (empty? log-files)
    {}
    (let [id->heartbeat (supervisor/read-worker-heartbeats conf)
          alive-ids (keys (remove
                            #(or (not (val %))
                                 (supervisor/is-worker-hb-timed-out? now-secs (val %) conf))
                            id->heartbeat))
          id->entries (identify-worker-log-files log-files)]
      (for [[id {:keys [owner files]}] id->entries
            :when (not (contains? (set alive-ids) id))]
        {:owner owner
         :files files}))))

(defn cleanup-fn! []
  (let [now-secs (current-time-secs)
        old-log-files (select-files-for-cleanup *STORM-CONF* (* now-secs 1000))
        dead-worker-files (get-files-of-dead-workers *STORM-CONF*
                                                     now-secs old-log-files)]
    (dofor [{:keys [owner files]} dead-worker-files
            file files]
      (let [path (.getCanonicalPath file)]
        (log-message "Cleaning up: Removing " path)
        (try
          (if-not (blank? owner)
            (supervisor/worker-launcher *STORM-CONF* owner (str "rmr " path))
            (rmr path))
          (catch Exception ex
            (log-error ex)))))))

(defn start-log-cleaner! [conf]
  (let [interval-secs (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
    (when interval-secs
      (log-debug "starting log cleanup thread at interval: " interval-secs)
      (schedule-recurring (mk-timer :thread-name "logviewer-cleanup")
                          0 ;; Start immediately.
                          interval-secs
                          cleanup-fn!))))

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

(defn get-log-user-whitelist [fname]
  (let [wl-file (get-log-metadata-file fname)
        m (clojure-from-yaml-file wl-file)]
    (if-let [whitelist (.get m LOGS-USERS)] whitelist [])))

(defn authorized-log-user? [user fname conf]
  (if (or (blank? user) (blank? fname))
    nil
    (let [whitelist (get-log-user-whitelist fname)
          logs-users (concat (conf LOGS-USERS)
                             (conf NIMBUS-ADMINS)
                             whitelist)]
       (some #(= % user) logs-users))))

(defn log-page [fname tail grep user]
  (let [file (-> (File. LOG-DIR fname) .getCanonicalFile)
        path (.getCanonicalPath file)]
    (if (= (File. LOG-DIR)
           (.getParentFile file))
      (let [tail (if tail
                   (min 10485760 (Integer/parseInt tail))
                   10240)
            tail-string (tail-file path tail)]
        (if (or (blank? (*STORM-CONF* UI-FILTER))
                (authorized-log-user? user fname *STORM-CONF*))
          (if grep
             (clojure.string/join "\n<br>"
               (filter #(.contains % grep) (.split tail-string "\n")))
             (.replaceAll tail-string "\n" "\n<br>"))

          (unauthorized-user-html user)))

      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log)
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defn log-template
  ([body] (log-template body nil))
  ([body user]
    (html4
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

(defn start-logviewer! [conf]
  (try
    (run-jetty logapp {:port (int (conf LOGVIEWER-PORT))
                       :join? false
                       :configurator (fn [server]
                                       (config-filter server logapp conf))})
  (catch Exception ex
    (log-error ex))))

(defn -main []
  (let [conf (read-storm-config)]
    (start-log-cleaner! conf)
    (start-logviewer! (read-storm-config))))
