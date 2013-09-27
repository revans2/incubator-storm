(ns backtype.storm.submitter-test
  (:use [clojure test])
  (:use [backtype.storm config testing])
  (:import [backtype.storm StormSubmitter])
  )

(deftest test-md5-digest-secret-generation
  (testing "No payload or scheme are generated when already present"
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (nil? actual-payload))
      (is (= "digest" actual-scheme))))

  (testing "Scheme is set to digest if not already."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (nil? actual-payload))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when no payload is present."
    (let [conf {STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is not correctly formatted."
    (let [bogus-payload "not-a-valid-payload"
          conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD bogus-payload
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (StormSubmitter/validateZKDigestPayload bogus-payload))) ; Is this test correct?
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is null."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD nil
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is blank."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD ""
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme)))))
