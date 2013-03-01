(ns backtype.storm.utils-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient Utils])
  (:import [com.netflix.curator.retry ExponentialBackoffRetry])
  (:import [org.apache.thrift7.transport TTransportException])
  (:use [backtype.storm config util])
  (:use [clojure test])
)

(deftest test-new-curator-uses-exponential-backoff
  (let [expected_interval 2400
        expected_retries 10
        conf (merge (read-storm-config)
          {Config/STORM_ZOOKEEPER_RETRY_INTERVAL expected_interval
           Config/STORM_ZOOKEEPER_RETRY_TIMES expected_retries})
        servers ["bogus_server"]
        arbitrary_port 42
        curator (Utils/newCurator conf servers arbitrary_port)
        retry (-> curator .getZookeeperClient .getRetryPolicy)
       ]
    (is (.isAssignableFrom ExponentialBackoffRetry (.getClass retry)))
    (is (= (.getBaseSleepTimeMs retry) expected_interval))
    (is (= (.getN retry) expected_retries))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-config
  (let [storm-conf (merge (read-storm-config)
                     {STORM-THRIFT-TRANSPORT-PLUGIN
                       "backtype.storm.security.auth.SimpleTransportPlugin"
                      Config/NIMBUS_HOST ""
                      Config/NIMBUS_THRIFT_PORT 65535
                     })]
    (is (thrown? RuntimeException
      (NimbusClient/getConfiguredClient storm-conf)))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-args
  (let [storm-conf (read-storm-config)]
    (is (thrown? TTransportException
      (NimbusClient. storm-conf "" 65535)
    ))
  )
)
