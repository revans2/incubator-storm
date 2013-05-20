(ns backtype.storm.utils.ZookeeperServerCnxnFactory-test
  (:import [backtype.storm.utils ZookeeperServerCnxnFactory])
  (:use [clojure test])
)

(deftest test-constructor-throws-runtimeexception-if-port-too-large
  (is (thrown? RuntimeException (ZookeeperServerCnxnFactory. 65536 42)))
)

(deftest test-factory
  (let [zkcf-negative (ZookeeperServerCnxnFactory. -42 42)
        next-port (+ (.port zkcf-negative) 1)
        arbitrary-max-clients 42
        zkcf-next (ZookeeperServerCnxnFactory. next-port arbitrary-max-clients)]
    ; Test handling negative port
    (is (not (nil? zkcf-negative)))
    ; Test max-clients is correctly set.
    (is (= (-> zkcf-next .factory .getMaxClientCnxnsPerHost) arbitrary-max-clients))
  )
)
