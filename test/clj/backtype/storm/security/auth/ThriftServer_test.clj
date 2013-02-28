(ns backtype.storm.security.auth.ThriftServer-test
  (:use [backtype.storm config])
  (:use [clojure test])
  (:import [backtype.storm.security.auth ThriftServer])
  (:import [org.apache.thrift7.transport TTransportException])
)

(deftest test-stop-checks-for-null
  (let [server (ThriftServer. (read-default-config) nil 12345)]
    (.stop server)))

(deftest test-ctor-catches-exceptions
  (let [conf {"java.security.auth.login.config" "!!!this is a bad file name!!!"}]
    (ThriftServer. conf nil 12345)))
