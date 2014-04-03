(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [clojure.set :only [difference]])
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [java.io File FileFilter FileInputStream])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.ui InvalidRequestException]
           [backtype.storm.security.auth AuthUtils])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
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
              {:owner (get-topo-owner-from-metadata-file metaFile)
               :files
                 ;; If each log for this root name is to be deleted, then
                 ;; include the metadata file also.
                 (if (empty? (difference
                                  (set (filter #(re-find (re-pattern log-root) %)
                                               (read-dir-contents LOG-DIR)))
                                  (set (map #(.getName %) files))))
                  (conj files metaFile)
                  ;; Otherwise, keep the list of files as it is.
                  files)}})))

(defn get-dead-worker-files-and-owners [conf now-secs log-files]
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
        dead-worker-files (get-dead-worker-files-and-owners *STORM-CONF* now-secs old-log-files)]
    (log-debug "log cleanup: now(" now-secs
               ") old log files (" (seq (map #(.getName %) old-log-files))
               ") dead worker files (" (seq (map #(.getName %) dead-worker-files)) ")")
    (dofor [{:keys [owner files]} dead-worker-files
            file files]
      (let [path (.getCanonicalPath file)]
        (log-message "Cleaning up: Removing " path)
        (try
          (if (or (blank? owner) (re-matches #".*\.yaml$" path))
            (rmr path)
            ;; worker-launcher does not actually launch a worker process.  It
            ;; merely executes one of a prescribed set of commands.  In this case, we ask it
            ;; to delete a file as the owner of that file.
            (supervisor/worker-launcher *STORM-CONF* owner (str "rmr " path)))
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

(defn page-file
  ([path tail]
    (let [flen (.length (clojure.java.io/file path))
          skip (- flen tail)]
      (page-file path skip tail)))
  ([path start length]
    (with-open [input (FileInputStream. path)
                output (java.io.ByteArrayOutputStream.)]
      (if (>= start (.length (clojure.java.io/file path)))
        (throw
          (InvalidRequestException. "Cannot start past the end of the file")))
      (if (> start 0)
        ;; FileInputStream#skip may not work the first time.
        (loop [skipped 0]
          (let [skipped (+ skipped (.skip input (- start skipped)))]
            (if (< skipped start) (recur skipped)))))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (when (< (.size output) length)
            (let [size (.read input buffer 0 (min 1024 (- length (.size output))))]
              (when (pos? size)
                (.write output buffer 0 size)
                (recur)))))
      (.toString output)))))

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

(defn pager-links [fname start length file-size]
  (let [prev-start (max 0 (- start length))
        next-start (if (> file-size 0)
                     (min (max 0 (- file-size length)) (+ start length))
                     (+ start length))]
    [[:div.pagination
      [:ul
        (concat
          [[(if (< prev-start start) (keyword "li") (keyword "li.disabled"))
            (link-to (url "/log"
                          {:file fname
                             :start (max 0 (- start length))
                             :length length})
                     "Prev")]]
          [[:li (link-to
                  (url "/log"
                       {:file fname
                        :start 0
                        :length length})
                  "First")]]
          [[:li (link-to
                  (url "/log"
                       {:file fname
                        :length length})
                  "Last")]]
          [[(if (> next-start start) (keyword "li.next") (keyword "li.next.disabled"))
            (link-to (url "/log"
                          {:file fname
                           :start (min (max 0 (- file-size length))
                                       (+ start length))
                           :length length})
                     "Next")]])]]]))

(defn- download-link [fname]
  [[:p (link-to (url-format "/download/%s" fname) "Download Full Log")]])

(defn log-page [fname start length grep user]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
          (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. LOG-DIR fname))
          file-length (.length file)
          path (.getCanonicalPath file)]
      (if (= (File. LOG-DIR)
             (.getParentFile file))
        (let [default-length 51200
              length (if length
                       (min 10485760 length)
                     default-length)
              log-string (escape-html
                           (if start
                             (page-file path start length)
                             (page-file path length)))
              start (or start (- file-length length))]
          (if grep
            (html [:pre
                   (if grep
                     (filter #(.contains % grep)
                             (.split log-string "\n"))
                     log-string)])
            (let [pager-data (pager-links fname start length file-length)]
              (html (concat pager-data
                            (download-link fname)
                            [[:pre log-string]]
                            pager-data)))))
        (-> (resp/response "Page not found")
            (resp/status 404))))
    (unauthorized-user-html user)))

(defn download-log-file [fname req resp user]
  (let [file (.getCanonicalFile (File. LOG-DIR fname))
        path (.getCanonicalPath file)]
    (if (= (File. LOG-DIR) (.getParentFile file))
      (if (or (blank? (*STORM-CONF* UI-FILTER))
              (authorized-log-user? user fname *STORM-CONF*))
        (-> (resp/response file)
            (resp/content-type "application/octet-stream"))
        (unauthorized-user-html user))
      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn log-template
  ([body] (log-template body nil nil))
  ([body fname user]
    (html4
     [:head
      [:title (str (escape-html fname) " - Storm Log Viewer")]
      (include-css "/css/bootstrap-1.4.0.css")
      (include-css "/css/style.css")
      ]
     [:body
      (concat
        (when (not (blank? user)) [[:div.ui-user [:p "User: " user]]])
        [[:h3 (escape-html fname)]]
        (seq body))
      ])))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

(defn- parse-long-from-map [m k]
  (try
    (Long/parseLong (k m))
    (catch NumberFormatException ex
      (throw (InvalidRequestException.
               (str "Could not make an integer out of the query parameter '"
                    (name k) "'")
               ex)))))

(defroutes log-routes
  (GET "/log" [:as {servlet-request :servlet-request} & m]
       (try
         (let [user (.getUserName http-creds-handler servlet-request)
               start (if (:start m) (parse-long-from-map m :start))
               length (if (:length m) (parse-long-from-map m :length))]
           (log-template (log-page (:file m) start length (:grep m) user)
                         (:file m) user))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response]} file & m]
       (try
         (let [user (.getUserName http-creds-handler servlet-request)]
           (download-log-file file servlet-request servlet-response user))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn start-logviewer! [conf]
  (try
    (let [header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filter-class (conf UI-FILTER)
          filter-params (conf UI-FILTER-PARAMS)
          logapp (handler/api log-routes) ;; query params as map
          filters-confs (if (conf UI-FILTER)
                          [{:filter-class filter-class
                            :filter-params (or (conf UI-FILTER-PARAMS) {})}]
                          [])
          filters-confs (concat filters-confs
                          [{:filter-class "org.mortbay.servlet.GzipFilter"
                            :filter-name "Gzipper"
                            :filter-params {}}])]
      (run-jetty logapp {:port (int (conf LOGVIEWER-PORT))
                         :join? false
                         :configurator (fn [server]
                                         (doseq [connector (.getConnectors server)]
                                           (.setHeaderBufferSize connector header-buffer-size))
                                         (config-filter server logapp filters-confs))}))
  (catch Exception ex
    (log-error ex))))

(defn -main []
  (let [conf (read-storm-config)]
    (start-log-cleaner! conf)
    (start-logviewer! (read-storm-config))))
