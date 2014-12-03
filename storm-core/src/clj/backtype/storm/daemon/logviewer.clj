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
(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [clojure.set :only [difference intersection]])
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:import [java.util Arrays])
  (:import [org.slf4j LoggerFactory])
  (:import [ch.qos.logback.classic Logger])
  (:import [ch.qos.logback.core FileAppender])
  (:import [java.io BufferedInputStream File FileFilter FileInputStream
                    InputStream InputStreamReader])
  (:import [java.nio ByteBuffer])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.ui InvalidRequestException]
           [backtype.storm.security.auth AuthUtils])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
            [ring.util.response :as resp])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [clojure.string :as string])
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

(defn select-files-for-cleanup [conf now-millis root-dir]
  (let [file-filter (mk-FileFilter-for-log-cleanup conf now-millis)]
    (.listFiles (File. root-dir) file-filter)))

(defn get-metadata-file-for-log-root-name [root-name root-dir]
  (let [metaFile (clojure.java.io/file root-dir "metadata"
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

(defn identify-worker-log-files [log-files root-dir]
  (into {} (for [log-root-entry (get-log-root->files-map log-files)
                 :let [metaFile (get-metadata-file-for-log-root-name
                                  (key log-root-entry) root-dir)
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
                                               (read-dir-contents root-dir)))
                                  (set (map #(.getName %) files))))
                  (conj files metaFile)
                  ;; Otherwise, keep the list of files as it is.
                  files)}})))

(defn get-dead-worker-files
  "Return a sorted set of java.io.Files that were written by workers that are
  now dead"
  [conf now-secs log-files root-dir]
  (if (empty? log-files)
    (sorted-set)
    (let [alive-ids (->> 
                      (supervisor/read-worker-heartbeats conf)
                      (remove
                        #(or (not (val %))
                             (supervisor/is-worker-hb-timed-out? now-secs
                                                                 (val %)
                                                                 conf)))
                      keys
                      set)
          id->entries (identify-worker-log-files log-files root-dir)]
      (reduce clojure.set/union
              (sorted-set)
              (for [[id {files :files}] id->entries
                    :when (not (contains? alive-ids id))]
                files)))))

(defn cleanup-fn!
  "Delete old log files for which the workers are no longer alive"
  [log-root-dir]
  (let [now-secs (current-time-secs)
        old-log-files (select-files-for-cleanup *STORM-CONF*
                                                (* now-secs 1000)
                                                log-root-dir)
        dead-worker-files (get-dead-worker-files *STORM-CONF*
                                                 now-secs
                                                 old-log-files
                                                 log-root-dir)]
    (log-debug "log cleanup: now=" now-secs
               " old log files " (pr-str (map #(.getName %) old-log-files))
               " dead worker files " (pr-str
                                       (map #(.getName %) dead-worker-files)))
    (dofor [file dead-worker-files]
      (let [path (.getCanonicalPath file)]
        (log-message "Cleaning up: Removing " path)
        (try (rmr path) (catch Exception ex (log-error ex)))))))

(defn start-log-cleaner! [conf log-root-dir]
  (let [interval-secs (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
    (when interval-secs
      (log-debug "starting log cleanup thread at interval: " interval-secs)
      (schedule-recurring (mk-timer :thread-name "logviewer-cleanup")
                          0 ;; Start immediately.
                          interval-secs
                          (fn [] (cleanup-fn! log-root-dir))))))

(defn- skip-bytes
  "FileInputStream#skip may not work the first time, so ensure it successfully
  skips the given number of bytes."
  [^InputStream stream n]
  (loop [skipped 0]
    (let [skipped (+ skipped (.skip stream (- n skipped)))]
      (if (< skipped n) (recur skipped)))))

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
      (if (> start 0) (skip-bytes input start))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (when (< (.size output) length)
            (let [size (.read input buffer 0 (min 1024 (- length (.size output))))]
              (when (pos? size)
                (.write output buffer 0 size)
                (recur)))))
      (.toString output)))))

(defn get-log-user-group-whitelist [fname]
  (let [wl-file (get-log-metadata-file fname)
        m (clojure-from-yaml-file wl-file)
        user-wl (.get m LOGS-USERS)
        user-wl (if user-wl user-wl [])
        group-wl (.get m LOGS-GROUPS)
        group-wl (if group-wl group-wl [])]
    [user-wl group-wl]))

(def igroup-mapper (AuthUtils/GetGroupMappingServiceProviderPlugin *STORM-CONF*))
(defn user-groups
  [user]
  (if (blank? user) [] (.getGroups igroup-mapper user)))

(defn authorized-log-user? [user fname conf]
  (if (or (blank? user) (blank? fname))
    nil
    (let [groups (user-groups user)
          [user-wl group-wl] (get-log-user-group-whitelist fname)
          logs-users (concat (conf LOGS-USERS)
                             (conf NIMBUS-ADMINS)
                             user-wl)
          logs-groups (concat (conf LOGS-GROUPS)
                              group-wl)]
       (or (some #(= % user) logs-users)
           (< 0 (.size (intersection (set groups) (set group-wl))))))))

(defn log-root-dir
  "Given an appender name, as configured, get the parent directory of the appender's log file.

Note that if anything goes wrong, this will throw an Error and exit."
  [appender-name]
  (let [appender (.getAppender (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) appender-name)]
    (if (and appender-name appender (instance? FileAppender appender))
      (.getParent (File. (.getFile appender)))
      (throw
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and logback agree.")))))

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

(def default-bytes-per-page 51200)

(defn log-page [fname start length grep user root-dir]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
          (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. root-dir fname))
          file-length (.length file)
          path (.getCanonicalPath file)]
      (if (= (File. root-dir)
             (.getParentFile file))
        (let [length (if length
                       (min 10485760 length)
                     default-bytes-per-page)
              log-string (escape-html
                           (if start
                             (page-file path start length)
                             (page-file path length)))
              start (or start (- file-length length))]
          (if grep
            (html [:pre#logContent
                   (if grep
                     (filter #(.contains % grep)
                             (.split log-string "\n"))
                     log-string)])
            (let [pager-data (pager-links fname start length file-length)]
              (html (concat pager-data
                            (download-link fname)
                            [[:pre#logContent log-string]]
                            pager-data)))))
        (-> (resp/response "Page not found")
            (resp/status 404))))
    (unauthorized-user-html user)))

(defn download-log-file [fname req resp user ^String root-dir]
  (let [file (.getCanonicalFile (File. root-dir fname))]
    (if (= (File. root-dir) (.getParentFile file))
      (if (or (blank? (*STORM-CONF* UI-FILTER))
              (authorized-log-user? user fname *STORM-CONF*))
        (-> (resp/response file)
            (resp/content-type "application/octet-stream"))
        (unauthorized-user-html user))
      (-> (resp/response "Page not found")
          (resp/status 404)))))

(def grep-max-search-size 1024)
(def grep-buf-size 2048)
(def grep-context-size 128)

(defn logviewer-port
  []
  (int (*STORM-CONF* LOGVIEWER-PORT)))

(defn url-to-match-centered-in-log-page
  [needle fname offset port]
  (let [host (local-hostname)
        port (logviewer-port)]
    (url (str "http://" host ":" port "/log")
         {:file fname
          :start (max 0
                      (- offset
                         (/ default-bytes-per-page 2)
                         (/ (alength needle) -2))) ;; Addition
          :length default-bytes-per-page})))

(defnk mk-match-data
  [^bytes needle ^ByteBuffer haystack haystack-offset file-offset fname
   :before-bytes nil :after-bytes nil]
  (let [url (url-to-match-centered-in-log-page needle
                                               fname
                                               file-offset
                                               (*STORM-CONF* LOGVIEWER-PORT))
        haystack-bytes (.array haystack)
        before-string (if (>= haystack-offset grep-context-size)
                        (String. haystack-bytes
                                 (- haystack-offset grep-context-size)
                                 grep-context-size
                                 "UTF-8")
                        (let [num-desired (max 0 (- grep-context-size
                                                    haystack-offset))
                              before-size (if before-bytes
                                            (alength before-bytes)
                                            0)
                              num-expected (min before-size num-desired)]
                          (if (pos? num-expected)
                            (str (String. before-bytes
                                          (- before-size num-expected)
                                          num-expected
                                          "UTF-8")
                                 (String. haystack-bytes
                                          0
                                          haystack-offset
                                          "UTF-8"))
                            (String. haystack-bytes
                                     0
                                     haystack-offset
                                     "UTF-8"))))
        after-string (let [needle-size (alength needle)
                           after-offset (+ haystack-offset needle-size)
                           haystack-size (.limit haystack)]
                       (if (< (+ after-offset grep-context-size) haystack-size)
                         (String. haystack-bytes
                                  after-offset
                                  grep-context-size
                                  "UTF-8")
                         (let [num-desired (- grep-context-size
                                              (- haystack-size after-offset))
                               after-size (if after-bytes
                                            (alength after-bytes)
                                            0)
                               num-expected (min after-size num-desired)]
                           (if (pos? num-expected)
                             (str (String. haystack-bytes
                                           after-offset
                                           (- haystack-size after-offset)
                                           "UTF-8")
                                  (String. after-bytes 0 num-expected "UTF-8"))
                             (String. haystack-bytes
                                      after-offset
                                      (- haystack-size after-offset)
                                      "UTF-8")))))]
    {"byteOffset" file-offset
     "beforeString" before-string
     "afterString" after-string
     "matchString" (String. needle "UTF-8")
     "logviewerURL" url}))

(defn- try-read-ahead!
  "Tries once to read ahead in the stream to fill the context and resets the
  stream to its position before the call."
  [^BufferedInputStream stream haystack offset file-len bytes-read]
  (let [num-expected (min (- file-len bytes-read)
                          grep-context-size)
        after-bytes (byte-array num-expected)]
    (.mark stream num-expected)
    ;; Only try reading once.
    (.read stream after-bytes 0 num-expected)
    (.reset stream)
    after-bytes))

(defn offset-of-bytes
  "Searches a given byte array for a match of a sub-array of bytes.  Returns
  the offset to the byte that matches, or -1 if no match was found."
  [^bytes buf ^bytes value init-offset]
  {:pre [(> (alength value) 0)
         (not (neg? init-offset))]}
  (loop [offset init-offset
         candidate-offset init-offset
         val-offset 0]
    (if-not (pos? (- (alength value) val-offset))
      ;; Found
      candidate-offset
      (if (>= offset (alength buf))
        ;; We ran out of buffer for the search.
        -1
        (if (not= (aget value val-offset) (aget buf offset))
          ;; The match at this candidate offset failed, so start over with the
          ;; next candidate byte from the buffer.
          (let [new-offset (inc candidate-offset)]
            (recur new-offset new-offset 0))
          ;; So far it matches.  Keep going...
          (recur (inc offset) candidate-offset (inc val-offset)))))))

(defn- buffer-substring-search!
  "As the file is read into a buffer, 1/2 the buffer's size at a time, we
  search the buffer for matches of the substring and return a list of zero or
  more matches."
  [file file-len offset-to-buf init-buf-offset stream bytes-read
   ^ByteBuffer haystack ^bytes needle initial-matches num-matches
   ^bytes before-bytes]
  (loop [buf-offset init-buf-offset
         matches initial-matches]
   (let [offset (offset-of-bytes (.array haystack) needle buf-offset)]
     (if (and (< (count matches) num-matches) (not (neg? offset)))
       (let [file-offset (+ offset-to-buf offset)
             bytes-needed-after-match (- (.limit haystack)
                                         grep-context-size
                                         (alength needle))
             before-arg (if (< offset grep-context-size) before-bytes)
             after-arg (if (> offset bytes-needed-after-match)
                         (try-read-ahead! stream
                                          haystack
                                          offset
                                          file-len
                                          bytes-read))]
         (recur (+ offset (alength needle))
                (conj matches
                      (mk-match-data needle
                                     haystack
                                     offset
                                     file-offset
                                     (.getName file)
                                     :before-bytes before-arg
                                     :after-bytes after-arg))))
       (let [before-str-to-offset (min (.limit haystack)
                                       grep-max-search-size)
             before-str-from-offset (max 0 (- before-str-to-offset
                                              grep-context-size))
             new-before-bytes (Arrays/copyOfRange (.array haystack)
                                                  before-str-from-offset
                                                  before-str-to-offset)
             ;; It's OK if new-byte-offset is negative.  This is normal if
             ;; we are out of bytes to read from a small file.
             new-byte-offset (if (>= (count matches) num-matches)
                               (+ (get (last matches) "byteOffset")
                                  (alength needle))
                               (- bytes-read grep-max-search-size))]
         [matches new-byte-offset new-before-bytes])))))

(defn- mk-grep-response
  "This response data only includes a next byte offset if there is more of the
  file to read."
  [search-bytes offset matches next-byte-offset]
  (merge {"searchString" (String. search-bytes "UTF-8")
          "startByteOffset" offset
          "matches" matches}
          (and next-byte-offset {"nextByteOffset" next-byte-offset})))

(defn rotate-grep-buffer!
  [^ByteBuffer buf ^BufferedInputStream stream total-bytes-read file file-len]
  (let [buf-arr (.array buf)]
    ;; Copy the 2nd half of the buffer to the first half.
    (System/arraycopy buf-arr
                      grep-max-search-size
                      buf-arr
                      0
                      grep-max-search-size)
    ;; Zero-out the 2nd half to prevent accidental matches.
    (Arrays/fill buf-arr
                 grep-max-search-size
                 (count buf-arr)
                 (byte 0))
    ;; Fill the 2nd half with new bytes from the stream.
    (let [bytes-read (.read stream
                            buf-arr
                            grep-max-search-size
                            (min file-len grep-max-search-size))]
      (.limit buf (+ grep-max-search-size bytes-read))
      (swap! total-bytes-read + bytes-read))))

(defnk substring-search
  "Searches for a substring in a log file, starting at the given offset,
  returning the given number of matches, surrounded by the given number of
  context lines.  Other information is included to be useful for progressively
  searching through a file for display in a UI. The search string must
  grep-max-search-size bytes or fewer when decoded with UTF-8."
  [file ^String search-string :num-matches 10 :start-byte-offset 0]
  {:pre [(not (empty? search-string))
         (<= (count (.getBytes search-string "UTF-8")) grep-max-search-size)]}
  (let [stream ^BufferedInputStream (BufferedInputStream.
                                      (FileInputStream. file))
        file-len (.length file)
        buf ^ByteBuffer (ByteBuffer/allocate grep-buf-size)
        buf-arr ^bytes (.array buf)
        string nil
        total-bytes-read (atom 0)
        matches []
        search-bytes ^bytes (.getBytes search-string "UTF-8")
        num-matches (or num-matches 10)
        start-byte-offset (or start-byte-offset 0)]
    ;; Start at the part of the log file we are interested in.
    (if (>= start-byte-offset file-len)
      (throw
        (InvalidRequestException. "Cannot search past the end of the file")))
    (if (> start-byte-offset 0)
      (skip-bytes stream start-byte-offset))
    (java.util.Arrays/fill buf-arr (byte 0))
    (let [bytes-read (.read stream buf-arr 0 (min file-len grep-buf-size))]
      (.limit buf bytes-read)
      (swap! total-bytes-read + bytes-read))
    (loop [initial-matches []
           init-buf-offset 0
           byte-offset start-byte-offset
           before-bytes nil]
      (let [[matches new-byte-offset new-before-bytes]
              (buffer-substring-search! file
                                        file-len
                                        byte-offset
                                        init-buf-offset
                                        stream
                                        @total-bytes-read
                                        buf
                                        search-bytes
                                        initial-matches
                                        (- num-matches (count matches))
                                        before-bytes)]
        (if (and (< (count matches) num-matches)
                 (< @total-bytes-read file-len))
          (let [;; The start index is positioned to find any possible
                ;; occurrence search string that did not quite fit in the
                ;; buffer on the previous read.
                new-buf-offset (- (min (.limit ^ByteBuffer buf)
                                       grep-max-search-size)
                                  (alength search-bytes))]
            (rotate-grep-buffer! buf stream total-bytes-read file file-len)
            (recur matches
                   new-buf-offset
                   new-byte-offset
                   new-before-bytes))
          (mk-grep-response search-bytes
                            start-byte-offset
                            matches
                            (if-not (and (< (count matches) num-matches)
                                         (>= @total-bytes-read file-len))
                              (let [next-byte-offset (+ (get (last matches)
                                                             "byteOffset")
                                                        (alength search-bytes))]
                                (if (> @total-bytes-read next-byte-offset)
                                  next-byte-offset)))))))))

(defn- try-parse-int-param
  [nam value]
  (try
    (Integer/parseInt value)
    (catch java.lang.NumberFormatException e
      (->
        (str "Could not parse " nam " to an integer")
        (InvalidRequestException. e)
        throw))))

(defn search-log-file
  [fname user ^String root-dir search num-matches offset]
  (let [file (.getCanonicalFile (File. root-dir fname))]
    (if (= (File. root-dir) (.getParentFile file))
      (if (or (blank? (*STORM-CONF* UI-FILTER))
              (authorized-log-user? user fname *STORM-CONF*))
        (let [num-matches-int (if num-matches
                                (try-parse-int-param "num-matches"
                                                     num-matches))
              offset-int (if offset
                           (try-parse-int-param "start-byte-offset" offset))]
          (try
            (if (and (not (empty? search))
                     <= (count (.getBytes search "UTF-8")) grep-max-search-size)
              (json-response
                (substring-search file
                                  search
                                  :num-matches num-matches-int
                                  :start-byte-offset offset-int))
              (throw
                (-> (str "Search substring must be between 1 and 1024 UTF-8 "
                         "bytes in size (inclusive)")
                    InvalidRequestException.)))
            (catch Exception ex
              (json-response (exception->json ex) 500))))
        (json-response (unauthorized-user-json user) 401))
      (json-response {"error" "Not Found"
                      "errorMessage" "The file was not found on this node."}
                     404))))

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
  (GET "/log" [:as req & m]
       (try
         (let [servlet-request (:servlet-request req)
               log-root (:log-root req)
               user (.getUserName http-creds-handler servlet-request)
               start (if (:start m) (parse-long-from-map m :start))
               length (if (:length m) (parse-long-from-map m :length))]
           (log-template (log-page (:file m) start length (:grep m) user log-root)
                         (:file m) user))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
       ;; We do not use servlet-response here, but do not remove it from the
       ;; :keys list, or this rule could stop working when an authentication
       ;; filter is configured.
       (try
         (let [user (.getUserName http-creds-handler servlet-request)]
           (download-log-file file servlet-request servlet-response user log-root))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (GET "/search/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
       ;; We do not use servlet-response here, but do not remove it from the
       ;; :keys list, or this rule could stop working when an authentication
       ;; filter is configured.
       (try
         (let [user (.getUserName http-creds-handler servlet-request)]
           (search-log-file file
                            user
                            log-root
                            (:search-string m)
                            (:num-matches m)
                            (:start-byte-offset m)))
         (catch InvalidRequestException ex
           (log-error ex)
           (json-response (exception->json ex) 400))))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn conf-middleware
  "For passing the storm configuration with each request."
  [app log-root]
  (fn [req]
    (app (assoc req :log-root log-root))))

(defn start-logviewer! [conf log-root-dir]
  (try
    (let [header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filter-class (conf UI-FILTER)
          filter-params (conf UI-FILTER-PARAMS)
          logapp (handler/api log-routes) ;; query params as map
          middle (conf-middleware logapp log-root-dir)
          filters-confs (if (conf UI-FILTER)
                          [{:filter-class filter-class
                            :filter-params (or (conf UI-FILTER-PARAMS) {})}]
                          [])
          filters-confs (concat filters-confs
                          [{:filter-class "org.mortbay.servlet.GzipFilter"
                            :filter-name "Gzipper"
                            :filter-params {}}])]
      (storm-run-jetty {:port (int (conf LOGVIEWER-PORT))
                        :configurator (fn [server]
                                        (doseq [connector (.getConnectors server)]
                                          (.setHeaderBufferSize connector header-buffer-size))
                                        (config-filter server middle filters-confs))}))
  (catch Exception ex
    (log-error ex))))

(defn -main []
  (let [conf (read-storm-config)
        log-root (log-root-dir (conf LOGVIEWER-APPENDER-NAME))]
    (start-log-cleaner! conf log-root)
    (start-logviewer! conf log-root)))
