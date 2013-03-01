(ns backtype.storm.utils.ZookeeperServerCnxnFactory-test
  (:import [backtype.storm.utils ZookeeperServerCnxnFactory])
  (:use [clojure test])
)

(deftest test-constructor-throws-runtimeexception-if-port-too-large
  (is (thrown? RuntimeException (ZookeeperServerCnxnFactory. 65536 42)))
)

(deftest test-factory-has-correct-max-num-clients
  (let [arbitrary-max-clients 42
        factory (ZookeeperServerCnxnFactory. 65535 arbitrary-max-clients)]
    (is (= (-> factory .factory .getMaxClientCnxnsPerHost) arbitrary-max-clients))
  )
)

(deftest test-constructor-handles-negative-port
  (is (not (nil? (ZookeeperServerCnxnFactory. -42 42))))
)
