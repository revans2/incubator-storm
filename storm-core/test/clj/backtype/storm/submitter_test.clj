(ns backtype.storm.submitter-test
  (:use [clojure test])
  (:use [conjure core])
  (:use [backtype.storm config testing])
  (:import [backtype.storm TestStormSubmitter])
  )

(deftest test-md5-digest-secret-generation
  (testing "StormSubmitter does not generate a payload with secret when unnecessary."
    (testing "No payload is generated when ZK auth is not configured on the cluster."
      (let [expected nil
            conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD expected}
            result (TestStormSubmitter/prepareZookeeperAuthentication conf)
            actual (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)]
        (is (nil? actual)))
      (let [result (TestStormSubmitter/prepareZookeeperAuthentication {})]
        (is (nil? (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)))))

    (testing "No payload is generated when one is already present and ZK is configured on the cluster."
      (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"
                  STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
            result (TestStormSubmitter/prepareZookeeperAuthentication conf)
            actual (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)]
        (is (nil? actual))))

    (testing "No payload is generated when one is already present with and ZK auth is not configured on the cluster."
      (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"}
            result (TestStormSubmitter/prepareZookeeperAuthentication conf)
            actual (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)]
        (is (nil? actual)))))

  (testing "StormSubmitter generates a payload with secret when necessary."
    (testing "A payload is generated no payload is present and ZK auth is configured on the cluster."
      (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD nil
                  STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
            result (TestStormSubmitter/prepareZookeeperAuthentication conf)
            actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
            actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
        (is (not (nil? actual-payload)))
        (is (not (= "" actual-payload)))
        (is (= "digest" actual-scheme))))))
