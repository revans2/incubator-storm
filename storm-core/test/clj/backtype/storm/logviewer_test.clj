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
(ns backtype.storm.logviewer-test
  (:use [backtype.storm config log util])
  (:require [backtype.storm.daemon [logviewer :as logviewer]
                                   [supervisor :as supervisor]])
  (:require [conjure.core])
  (:use [clojure test])
  (:use [conjure core])
  (:import [java.io File])
  (:import [org.mockito Mockito]))

(defmulti mk-mock-File #(:type %))

(defmethod mk-mock-File :file [{file-name :name mtime :mtime
                                :or {file-name "afile" mtime 1}}]
  (let [mockFile (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockFile)) thenReturn file-name)
    (. (Mockito/when (.lastModified mockFile)) thenReturn mtime)
    (. (Mockito/when (.isFile mockFile)) thenReturn true)
    (. (Mockito/when (.getCanonicalPath mockFile))
       thenReturn (str "/mock/canonical/path/to/" file-name))
    mockFile))

(defmethod mk-mock-File :directory [{dir-name :name mtime :mtime
                                     :or {dir-name "adir" mtime 1}}]
  (let [mockDir (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockDir)) thenReturn dir-name)
    (. (Mockito/when (.lastModified mockDir)) thenReturn mtime)
    (. (Mockito/when (.isFile mockDir)) thenReturn false)
    mockDir))

(deftest test-mk-FileFilter-for-log-cleanup
  (testing "log file filter selects the correct log files for purge"
    (let [now-millis (current-time-millis)
          conf {LOGVIEWER-CLEANUP-AGE-MINS 60
                LOGVIEWER-CLEANUP-INTERVAL-SECS 300}
          cutoff-millis (logviewer/cleanup-cutoff-age-millis conf now-millis)
          old-mtime-millis (- cutoff-millis 500)
          new-mtime-millis (+ cutoff-millis 500)
          matching-files (map #(mk-mock-File %)
                              [{:name "oldlog-1-2-worker-3.log"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "oldlog-1-2-worker-3.log.8"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "foobar*_topo-1-24242-worker-2834238.log"
                                :type :file
                                :mtime old-mtime-millis}])
          excluded-files (map #(mk-mock-File %)
                              [{:name "oldlog-1-2-worker-.log"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "olddir-1-2-worker.log"
                                :type :directory
                                :mtime old-mtime-millis}
                               {:name "newlog-1-2-worker.log"
                                :type :file
                                :mtime new-mtime-millis}
                               {:name "some-old-file.txt"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "metadata"
                                :type :directory
                                :mtime old-mtime-millis}
                               {:name "newdir-1-2-worker.log"
                                :type :directory
                                :mtime new-mtime-millis}
                               {:name "newdir"
                                :type :directory
                                :mtime new-mtime-millis}
                              ])
          file-filter (logviewer/mk-FileFilter-for-log-cleanup conf now-millis)]
        (is   (every? #(.accept file-filter %) matching-files))
        (is (not-any? #(.accept file-filter %) excluded-files))
      )))

(deftest test-get-log-root->files-map
  (testing "returns map of root name to list of files"
    (let [files (vec (map #(java.io.File. %) ["log-1-2-worker-3.log"
                                              "log-1-2-worker-3.log.1"
                                              "log-2-4-worker-6.log.1"]))
          expected {"log-1-2-worker-3" #{(files 0) (files 1)}
                    "log-2-4-worker-6" #{(files 2)}}]
      (is (= expected (logviewer/get-log-root->files-map files))))))

(deftest test-identify-worker-log-files
  (testing "Does not include metadata file when there are any log files that
           should not be cleaned up"
    (let [cutoff-millis 2000
          old-logFile (mk-mock-File {:name "mock-1-1-worker-1.log.1"
                                     :type :file
                                     :mtime (- cutoff-millis 1000)})
          mock-metaFile (mk-mock-File {:name "mock-1-1-worker-1.yaml"
                                       :type :file
                                       :mtime 1})
          new-logFile (mk-mock-File {:name "mock-1-1-worker-1.log"
                                     :type :file
                                     :mtime (+ cutoff-millis 1000)})
          exp-id "id12345"
          exp-user "alice"
          expected {exp-id {:owner exp-user
                            :files #{old-logFile}}}]
      (stubbing [supervisor/read-worker-heartbeats nil
                logviewer/get-metadata-file-for-log-root-name mock-metaFile
                read-dir-contents [(.getName old-logFile) (.getName new-logFile)]
                logviewer/get-worker-id-from-metadata-file exp-id
                logviewer/get-topo-owner-from-metadata-file exp-user]
        (is (= expected (logviewer/identify-worker-log-files [old-logFile] "/tmp/")))))))

(deftest test-get-dead-worker-files
  (testing "removes any files of workers that are still alive"
    (let [conf {SUPERVISOR-WORKER-TIMEOUT-SECS 5}
          id->hb {"42" {:time-secs 1}}
          now-secs 2
          log-files #{:expected-file :unexpected-file}
          exp-owner "alice"]
      (stubbing [logviewer/identify-worker-log-files {"42" {:owner exp-owner
                                                            :files #{:unexpected-file}}
                                                      "007" {:owner exp-owner
                                                             :files #{:expected-file}}}
                 logviewer/get-topo-owner-from-metadata-file "alice"
                 supervisor/read-worker-heartbeats id->hb]
        (is (= #{:expected-file}
               (logviewer/get-dead-worker-files conf now-secs log-files "/tmp/")))))))

(deftest test-cleanup-fn
  (testing "cleanup function rmr's files of dead workers"
    (let [mockfile1 (mk-mock-File {:name "delete-me1" :type :file})
          mockfile2 (mk-mock-File {:name "delete-me2" :type :file})]
      (stubbing [logviewer/select-files-for-cleanup nil
                 logviewer/get-dead-worker-files (sorted-set mockfile1 mockfile2)
                 rmr nil]
        (logviewer/cleanup-fn! "/bogus/path")
        (verify-call-times-for rmr 2)
        (verify-nth-call-args-for 1 rmr (.getCanonicalPath mockfile1))
        (verify-nth-call-args-for 2 rmr (.getCanonicalPath mockfile2))))))

(deftest test-authorized-log-user
  (testing "allow cluster admin"
    (let [conf {NIMBUS-ADMINS ["alice"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "ignore any cluster-set topology.users topology.groups"
    (let [conf {TOPOLOGY-USERS ["alice"]
                TOPOLOGY-GROUPS ["alice-group"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups ["alice-group"]]
        (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" conf)))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "allow cluster logs user"
    (let [conf {LOGS-USERS ["alice"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "allow whitelisted topology user"
    (stubbing [logviewer/get-log-user-group-whitelist [["alice"] []]
               logviewer/user-groups []]
      (is (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice")))

  (testing "allow whitelisted topology group"
    (stubbing [logviewer/get-log-user-group-whitelist [[] ["alice-group"]]
               logviewer/user-groups ["alice-group"]]
      (is (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice")))

  (testing "disallow user not in nimbus admin, topo user, logs user, or whitelist"
    (stubbing [logviewer/get-log-user-group-whitelist [[] []]
               logviewer/user-groups []]
      (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" {})))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice"))))

(deftest test-search-via-rest-api
  (testing "Throws if bogus file is given"
    (thrown-cause? java.lang.RuntimeException
                      (logviewer/substring-search nil "a string")))

  (let [pattern "needle"
        expected-host "dev.null.invalid"
        expected-port 8888
        ;; When we click a link to the logviewer, we expect the match line to
        ;; be somewhere near the middle of the page.  So we subtract half of
        ;; the default page length from the offset at which we found the
        ;; match.
        exp-offset-fn #(- (/ logviewer/default-bytes-per-page 2) %)]

    (stubbing [local-hostname expected-host
               logviewer/logviewer-port expected-port]

      (testing "Logviewer link centers the match in the page"
        (let [expected-fname "foobar.log"]
          (is (= (str "http://"
                      expected-host
                      ":"
                      expected-port
                      "/log?file="
                      expected-fname
                      "&start=1947&length="
                      logviewer/default-bytes-per-page)
                 (logviewer/url-to-match-centered-in-log-page (byte-array 42)
                                                              expected-fname
                                                              27526
                                                              8888)))))

      (let [file (->> "logviewer-search-context-tests.log"
                      (clojure.java.io/file "src" "dev"))]
        (testing "returns correct before/after context"
          (is (= {"searchString" pattern
                  "startByteOffset" 0
                  "matches" [{"byteOffset" 0
                              "beforeString" ""
                              "afterString" " needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle "
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                  expected-host
                                                  ":"
                                                  expected-port
                                                  "/log?file="
                                                  (.getName file)
                                                  "&start=0&length=51200")}
                             {"byteOffset" 7
                              "beforeString" "needle "
                              "afterString" "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle needle\n"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                  expected-host
                                                  ":"
                                                  expected-port
                                                  "/log?file="
                                                  (.getName file)
                                                  "&start=0&length=51200")}
                             {"byteOffset" 127
                              "beforeString" "needle needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                              "afterString" " needle\n"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                  expected-host
                                                  ":"
                                                  expected-port
                                                  "/log?file="
                                                  (.getName file)
                                                  "&start=0&length=51200")}
                             {"byteOffset" 134
                              "beforeString" " needle000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000needle "
                              "afterString" "\n"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                  expected-host
                                                  ":"
                                                  expected-port
                                                  "/log?file="
                                                  (.getName file)
                                                  "&start=0&length=51200")}
                             ]}
                 (logviewer/substring-search file pattern)))))

      (let [file (clojure.java.io/file "src" "dev" "small-worker.log")]
        (testing "a really small log file"
          (is (= {"searchString" pattern
                  "startByteOffset" 0
                  "matches" [{"byteOffset" 7
                              "beforeString" "000000 "
                              "afterString" " 000000\n"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                  expected-host
                                                  ":"
                                                  expected-port
                                                  "/log?file="
                                                  (.getName file)
                                                  "&start=0&length=51200")}]}
                   (logviewer/substring-search file pattern)))))

      (let [file (clojure.java.io/file "src" "dev" "test-3072.log")]
        (testing "no offset returned when file ends on buffer offset"
          (let [expected
                  {"searchString" pattern
                   "startByteOffset" 0
                   "matches" [{"byteOffset" 3066
                               "beforeString" (->>
                                                (repeat 128 '.)
                                                clojure.string/join)
                               "afterString" ""
                               "matchString" pattern
                               "logviewerURL" (str "http://"
                                                   expected-host
                                                   ":"
                                                   expected-port
                                                   "/log?file="
                                                   (.getName file)
                                                   "&start=0&length=51200")}]}]
          (is (= expected
                 (logviewer/substring-search file pattern)))
          (is (= expected
                 (logviewer/substring-search file pattern :num-matches 1))))))

      (let [file (clojure.java.io/file "src" "dev" "test-worker.log")]

        (testing "next byte offsets are correct for each match"
          (doseq [[num-matches expected-next-byte-offset] [[1 11]
                                                           [2 2042]
                                                           [3 2052]
                                                           [4 3078]
                                                           [5 3196]
                                                           [6 3202]
                                                           [7 6252]
                                                           [8 6321]
                                                           [9 6395]
                                                           [10 6474]
                                                           [11 6552]
                                                           [12 nil]
                                                           [13 nil]]]
            (let [result (logviewer/substring-search file
                                                     pattern
                                                     :num-matches num-matches)]
              (is (= expected-next-byte-offset
                     (get result "nextByteOffset"))))))

        (is
          (= {"nextByteOffset" 3202
              "searchString" pattern
              "startByteOffset" 0
              "matches" [
                         {"byteOffset" 5
                          "beforeString" "Test "
                          "afterString" " is near the beginning of the file.\nThis file assumes a buffer size of 2048 bytes, a max search string size of 1024 bytes, and a"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         {"byteOffset" 2036
                          "beforeString" "ng 146\npadding 147\npadding 148\npadding 149\npadding 150\npadding 151\npadding 152\npadding 153\nNear the end of a 1024 byte block, a "
                          "afterString" ".\nA needle that straddles a 1024 byte boundary should also be detected.\n\npadding 157\npadding 158\npadding 159\npadding 160\npadding"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         {"byteOffset" 2046
                          "beforeString" "ding 147\npadding 148\npadding 149\npadding 150\npadding 151\npadding 152\npadding 153\nNear the end of a 1024 byte block, a needle.\nA "
                          "afterString" " that straddles a 1024 byte boundary should also be detected.\n\npadding 157\npadding 158\npadding 159\npadding 160\npadding 161\npaddi"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         {"byteOffset" 3072
                          "beforeString" "adding 226\npadding 227\npadding 228\npadding 229\npadding 230\npadding 231\npadding 232\npadding 233\npadding 234\npadding 235\n\n\nHere a "
                          "afterString" " occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: needleneedle\n\npa"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         {"byteOffset" 3190
                          "beforeString" "\n\n\nHere a needle occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: "
                          "afterString" "needle\n\npadding 243\npadding 244\npadding 245\npadding 246\npadding 247\npadding 248\npadding 249\npadding 250\npadding 251\npadding 252\n"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         {"byteOffset" 3196
                          "beforeString" "e a needle occurs just after a 1024 byte boundary.  It should have the correct context.\n\nText with two adjoining matches: needle"
                          "afterString" "\n\npadding 243\npadding 244\npadding 245\npadding 246\npadding 247\npadding 248\npadding 249\npadding 250\npadding 251\npadding 252\npaddin"
                          "matchString" pattern
                          "logviewerURL" (str "http://"
                                               expected-host
                                               ":"
                                               expected-port
                                               "/log?file="
                                               (.getName file)
                                               "&start=0&length=51200")}
                         ]}
                 (logviewer/substring-search file pattern :num-matches 6)))

          (let [pattern (clojure.string/join (repeat 1024 'X))]
            (is
              (= {"nextByteOffset" 6183
                  "searchString" pattern
                  "startByteOffset" 0
                  "matches" [
                             {"byteOffset" 4075
                              "beforeString" "\n\nThe following match of 1024 bytes completely fills half the byte buffer.  It is a search substring of the maximum size......\n\n"
                              "afterString" "\nThe following max-size match straddles a 1024 byte buffer.\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                   expected-host
                                                   ":"
                                                   expected-port
                                                   "/log?file="
                                                   (.getName file)
                                                   "&start=0&length=51200")}
                             {"byteOffset" 5159
                              "beforeString" "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\nThe following max-size match straddles a 1024 byte buffer.\n"
                              "afterString" "\n\nHere are four non-ascii 1-byte UTF-8 characters: αβγδε\n\nneedle\n\nHere are four printable 2-byte UTF-8 characters: ¡¢£¤"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                   expected-host
                                                   ":"
                                                   expected-port
                                                   "/log?file="
                                                   (.getName file)
                                                   "&start=0&length=51200")}
                             ]}
                   (logviewer/substring-search file pattern :num-matches 2))))

          (let [pattern "𐄀𐄁𐄂"]
            (is
              (= {"nextByteOffset" 7174
                  "searchString" pattern
                  "startByteOffset" 0
                  "matches" [
                             {"byteOffset" 7162
                              "beforeString" "padding 372\npadding 373\npadding 374\npadding 375\n\nThe following tests multibyte UTF-8 Characters straddling the byte boundary:   "
                              "afterString" "\n\nneedle"
                              "matchString" pattern
                              "logviewerURL" (str "http://"
                                                   expected-host
                                                   ":"
                                                   expected-port
                                                   "/log?file="
                                                   (.getName file)
                                                   "&start=0&length=51200")}
                             ]}
                   (logviewer/substring-search file pattern :num-matches 1))))

        (testing "Returns 0 matches for unseen pattern"
          (let [pattern "Not There"]
            (is (= {"searchString" pattern
                    "startByteOffset" 0
                    "matches" []}
                   (logviewer/substring-search file pattern)))))))))
