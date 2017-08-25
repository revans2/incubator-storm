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
(ns backtype.storm.utils-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.generated AuthorizationException])
  (:import [backtype.storm.utils NimbusClient Utils])
  (:import [backtype.storm.blobstore BlobStoreAclHandler])
  (:import [org.apache.curator.retry ExponentialBackoffRetry])
  (:import [org.apache.thrift.transport TTransportException])
  (:use [backtype.storm config util])
  (:use [clojure.string :only [split]])
  (:use [clojure test])
  (:use [clojure.java io])
  )

(deftest test-new-curator-uses-exponential-backoff
  (let [expected_interval 2400
        expected_retries 10
        expected_ceiling 3000
        conf (merge (clojurify-structure (Utils/readDefaultConfig))
          {Config/STORM_ZOOKEEPER_RETRY_INTERVAL expected_interval
           Config/STORM_ZOOKEEPER_RETRY_TIMES expected_retries
           Config/STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING expected_ceiling})
        servers ["bogus_server"]
        arbitrary_port 42
        curator (Utils/newCurator conf servers arbitrary_port nil)
        retry (-> curator .getZookeeperClient .getRetryPolicy)
       ]
    (is (.isAssignableFrom ExponentialBackoffRetry (.getClass retry)))
    (is (= (.getBaseSleepTimeMs retry) expected_interval))
    (is (= (.getN retry) expected_retries))
    (is (= (.getSleepTimeMs retry 10 0) expected_ceiling))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-config
  (let [storm-conf (merge (read-storm-config)
                     {STORM-THRIFT-TRANSPORT-PLUGIN
                       "backtype.storm.security.auth.SimpleTransportPlugin"
                      Config/NIMBUS_HOST ""
                      Config/NIMBUS_THRIFT_PORT 65535
                      STORM-NIMBUS-RETRY-TIMES 0})]
    (is (thrown-cause? RuntimeException
      (NimbusClient/getConfiguredClient storm-conf)))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-args
  (let [storm-conf (merge
                    (read-storm-config)
                    {STORM-NIMBUS-RETRY-TIMES 0})]
    (is (thrown-cause? TTransportException
      (NimbusClient. storm-conf "" 65535)
    ))
  )
)

(deftest test-isZkAuthenticationConfiguredTopology
    (testing "Returns false on null config"
      (is (not (Utils/isZkAuthenticationConfiguredTopology nil))))
    (testing "Returns false on scheme key missing"
      (is (not (Utils/isZkAuthenticationConfiguredTopology
          {STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME nil}))))
    (testing "Returns false on scheme value null"
      (is (not
        (Utils/isZkAuthenticationConfiguredTopology
          {STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME nil}))))
    (testing "Returns true when scheme set to string"
      (is
        (Utils/isZkAuthenticationConfiguredTopology
          {STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME "foobar"}))))

(deftest test-isZkAuthenticationConfiguredStormServer
  (let [k "java.security.auth.login.config"
        oldprop (System/getProperty k)]
    (try
      (.remove (System/getProperties) k)
      (testing "Returns false on null config"
        (is (not (Utils/isZkAuthenticationConfiguredStormServer nil))))
      (testing "Returns false on scheme key missing"
        (is (not (Utils/isZkAuthenticationConfiguredStormServer
            {STORM-ZOOKEEPER-AUTH-SCHEME nil}))))
      (testing "Returns false on scheme value null"
        (is (not
          (Utils/isZkAuthenticationConfiguredStormServer
            {STORM-ZOOKEEPER-AUTH-SCHEME nil}))))
      (testing "Returns true when scheme set to string"
        (is
          (Utils/isZkAuthenticationConfiguredStormServer
            {STORM-ZOOKEEPER-AUTH-SCHEME "foobar"})))
      (testing "Returns true when java.security.auth.login.config is set"
        (do
          (System/setProperty k "anything")
          (is (Utils/isZkAuthenticationConfiguredStormServer {}))))
      (testing "Returns false when java.security.auth.login.config is set"
        (do
          (System/setProperty k "anything")
          (is (Utils/isZkAuthenticationConfiguredStormServer {}))))
    (finally 
      (if (not-nil? oldprop) 
        (System/setProperty k oldprop)
        (.remove (System/getProperties) k))))))

(deftest test-secs-to-millis-long
  (is (= 0 (secs-to-millis-long 0)))
  (is (= 2 (secs-to-millis-long 0.002)))
  (is (= 500 (secs-to-millis-long 0.5)))
  (is (= 1000 (secs-to-millis-long 1)))
  (is (= 1080 (secs-to-millis-long 1.08)))
  (is (= 10000 (secs-to-millis-long 10)))
  (is (= 10100 (secs-to-millis-long 10.1)))
)


(deftest test-localVersionOfBlob
  (testing "validates the correct local version is reported, given the correct version file"
    (let [tmpVersionFile (java.io.File/createTempFile "blob_" ".version")
          version-path (.getAbsolutePath tmpVersionFile)
          blob-path (clojure.string/replace version-path #".version" "")
          version 123456789
          input-text (str version)
          _ (spit tmpVersionFile input-text)
          currentBlobVersion (Utils/localVersionOfBlob blob-path)]
      (try
        (is (= currentBlobVersion version))
        (finally
          (delete-file tmpVersionFile))))))

(defn as-access-control
  "Convert a parameter to an AccessControl object"
  [param]
  (BlobStoreAclHandler/parseAccessControl (str param)))

(defn as-acl
  [param]
  (map as-access-control (split param #",")))

(deftest test-validateSettableACLs
  (let [dup-user-acls (as-acl "u:user:rwa,u:user:r-a")
        valid-user-acls (as-acl "u:goodguy:rwa,u:user:r-a,o::rwa")]
    (testing "Duplicate User ACLs should throw exception"
      (is (thrown? AuthorizationException
            (BlobStoreAclHandler/validateSettableACLs "BlobwithDupACLs" dup-user-acls))))
    (testing "Valid User in ACLs should pass"
      (is (nil?
            (try
              (BlobStoreAclHandler/validateSettableACLs "BlobwithValidACLs" valid-user-acls)
              (catch Exception auth-ex auth-ex)))))
))


(deftest test-scheduler-strategy-whitelist
  (let [conf {NIMBUS-SCHEDULER-STRATEGY-CLASS-WHITELIST
              '("backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy")}]
    (is (.getCanonicalName (class (Utils/newSchedulerStrategyInstance "backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy" conf)))
        "backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy")

    (is (thrown? RuntimeException
                 (Utils/newSchedulerStrategyInstance "backtype.storm.scheduler.resource.strategies.scheduling.ConstraintSolverStrategy" conf))))

  ;; Empty config should allow all strategies by default.
  (is (.getCanonicalName (class (Utils/newSchedulerStrategyInstance "backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy" {})))
      "backtype.storm.scheduler.resource.strategies.scheduling.MultitenantStrategy")

  (is (.getCanonicalName (class (Utils/newSchedulerStrategyInstance "backtype.storm.scheduler.resource.strategies.scheduling.ConstraintSolverStrategy" {})))
      "backtype.storm.scheduler.resource.strategies.scheduling.ConstraintSolverStrategy"))
