(ns backtype.storm.security.auth.auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm Config])
  (:import [backtype.storm.generated AuthorizationException])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth.authorizer SimpleWhitelistAuthorizer SimpleACLAuthorizer])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
            ReqContext SimpleTransportPlugin])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client]))

(bootstrap)

(def nimbus-timeout (Integer. 30))

(defn nimbus-data [storm-conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf storm-conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (storm-conf NIMBUS-AUTHORIZER) storm-conf)
     :submitted-count (atom 0)
     :storm-cluster-state nil
     :submit-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders nil
     :uploaders nil
     :uptime (uptime-computer)
     :validator nil
     :timer nil
     :scheduler nil
     }))

(defn dummy-service-handler [conf inimbus]
  (let [nimbus-d (nimbus-data conf inimbus)
        topo-conf (atom nil)]
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
                                     ^SubmitOptions submitOptions]
        (if (not (nil? serializedConf)) (swap! topo-conf (fn [prev new] new) (from-json serializedConf)))
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "submitTopology"))
      
      (^void killTopology [this ^String storm-name]
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology"))
      
      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology"))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "rebalance"))

      (activate [this storm-name]
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "activate"))

      (deactivate [this storm-name]
        (nimbus/check-authorization! nimbus-d storm-name @topo-conf "deactivate"))

      (beginFileUpload [this])

      (^void uploadChunk [this ^String location ^ByteBuffer chunk])

      (^void finishFileUpload [this ^String location])

      (^String beginFileDownload [this ^String file]
        (nimbus/check-authorization! nimbus-d nil @topo-conf "fileDownload")
        "Done!")

      (^ByteBuffer downloadChunk [this ^String id])

      (^String getNimbusConf [this])

      (^String getTopologyConf [this ^String id])

      (^StormTopology getTopology [this ^String id])

      (^StormTopology getUserTopology [this ^String id])

      (^ClusterSummary getClusterInfo [this])
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]))))

(defn launch-server [server-port login-cfg aznClass transportPluginClass serverConf] 
  (let [conf1 (merge (read-storm-config)
                     {NIMBUS-AUTHORIZER aznClass 
                      NIMBUS-HOST "localhost"
                      NIMBUS-THRIFT-PORT server-port
                      STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass})
        conf2 (if login-cfg (merge conf1 {"java.security.auth.login.config" login-cfg}) conf1)
        conf (if serverConf (merge conf2 serverConf) conf2)
        nimbus (nimbus/standalone-nimbus)
        service-handler (dummy-service-handler conf nimbus)
        server (ThriftServer. 
                conf 
                (Nimbus$Processor. service-handler) 
                (int (conf NIMBUS-THRIFT-PORT))
                backtype.storm.Config$ThriftServerPurpose/NIMBUS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    (.start (Thread. #(.serve server)))
    (wait-for-condition #(.isServing server))
    server ))

(defmacro with-server [args & body]
  `(let [server# (launch-server ~@args)]
     ~@body
     (.stop server#)
     ))

(deftest Simple-authentication-test 
  (with-server [6632 nil nil "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6632 nimbus-timeout)
          nimbus_client (.getClient client)]
      (.activate nimbus_client "security_auth_test_topology")
      (.close client))

    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})]
      (testing "(Negative authentication) Server: Simple vs. Client: Digest"
        (is (thrown-cause?  java.net.SocketTimeoutException
                            (NimbusClient. storm-conf "localhost" 6632 nimbus-timeout)))))))

(deftest negative-whitelist-authorization-test 
  (with-server [6633 nil
                "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer" 
                "backtype.storm.testing.SingleUserSimpleTransport" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6633 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization) Authorization plugin should reject client request"
        (is (thrown-cause? AuthorizationException
                           (.activate nimbus_client "security_auth_test_topology"))))
      (.close client))))

(deftest positive-whitelist-authorization-test 
  (with-server [6634 nil
                "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer" 
                "backtype.storm.testing.SingleUserSimpleTransport" {SimpleWhitelistAuthorizer/WHITELIST_USERS_CONF ["user"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6634 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
        (.activate nimbus_client "security_auth_test_topology"))
      (.close client))))

(deftest negative-acl-authorization-test
  (with-server [6633 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6633 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization) Authorization plugin should reject client request"
        (is (thrown-cause? AuthorizationException
                           (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil 
                                                    (to-json
                                                     {"storm.auth.simple-acl.users" ["user"]
                                                      "storm.auth.simple-acl.users.commands" ["submitTopology"]}) 
                                                    nil nil))))
      (.close client))))

(deftest negative-acl-authorization-user-privilege-test
  (with-server [6635 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6635 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization user privilege) Authorization plugin should reject client request"
        (is (thrown-cause? AuthorizationException
                           (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil 
                                                    (to-json
                                                     {"storm.auth.simple-acl.users" ["user"]
                                                      "storm.auth.simple-acl.users.commands" ["submitTopology"]})
                                                    nil nil))))
      (.close client))))

(deftest negative-acl-authorization-user-topo-privilege-test
  (with-server [6634 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]
                                                                    "storm.auth.simple-acl.users.commands" ["submitTopology"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6634 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization user privilege) Authorization plugin should reject client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil 
                                 (to-json
                                  {"storm.auth.simple-acl.users" ["user"]
                                   "storm.auth.simple-acl.users.commands" ["submitTopology"]})
                                 nil nil)
        (is (thrown-cause? AuthorizationException
                           (.activate nimbus_client "security_auth_test_topology"))))
      (.close client))))

(deftest negative-acl-authorization-supervisor-privilege-test
  (with-server [6636 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]
                                                                    "storm.auth.simple-acl.users.commands" ["submitTopology"]
                                                                    "supervisor.supervisors" ["user"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6636 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization supervisor privilege) Authorization plugin should reject client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil nil nil nil)
        (is (thrown-cause? AuthorizationException
                           (.beginFileDownload nimbus_client nil))))
      (.close client))))

(deftest positive-acl-authorization-admin
  (with-server [6637 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.admins" ["user"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6637 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization admin) Authorization plugin should accept client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil nil nil nil)
        (.killTopology nimbus_client "security_auth_test_topology")
        (.activate nimbus_client "security_auth_test_topology"))
      (.close client))))

(deftest positive-acl-authorization-user
  (with-server [6642 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]
                                                                    "storm.auth.simple-acl.users.commands" ["submitTopology"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6642 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization admin) Authorization plugin should accept client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil nil nil nil))
      (.close client))))

(deftest positive-acl-authorization-user-topo
  (with-server [6642 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]
                                                                    "storm.auth.simple-acl.users.commands" ["submitTopology"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6642 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization admin) Authorization plugin should accept client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil 
                                 (to-json
                                  {"storm.auth.simple-acl.users" ["user"]
                                   "storm.auth.simple-acl.users.commands" ["activate"]})
                                 nil nil)
        (.activate nimbus_client "security_auth_test_topology"))
      (.close client))))

(deftest positive-acl-authorization-supervisor
  (with-server [6638 nil
                "backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"
                "backtype.storm.testing.SingleUserSimpleTransport" {"storm.auth.simple-acl.users" ["user"]
                                                                    "storm.auth.simple-acl.users.commands" ["submitTopology"]
                                                                    "supervisor.supervisors" ["user"]
                                                                    "supervisor.supervisors.commands" ["fileDownload"]}]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
          client (NimbusClient. storm-conf "localhost" 6638 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization admin) Authorization plugin should accept client request"
        (.submitTopologyWithOpts nimbus_client "security_auth_test_topology" nil nil nil nil)
        (.beginFileDownload nimbus_client nil))
      (.close client))))

(deftest positive-authorization-test 
  (with-server [6639 nil
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6639 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
        (.activate nimbus_client "security_auth_test_topology"))
      (.close client))))

(deftest deny-authorization-test 
  (with-server [6640 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6640
                             Config/NIMBUS_TASK_TIMEOUT_SECS nimbus-timeout})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization) Authorization plugin should reject client request"
        (is (thrown-cause? AuthorizationException
                           (.activate nimbus_client "security_auth_test_topology"))))
      (.close client))))

(deftest digest-authentication-test
  (with-server [6641
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                nil
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authentication) valid digest authentication"
        (.activate nimbus_client "security_auth_test_topology"))
      (.close client))
    
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authentication) Server: Digest vs. Client: Simple"
        (is (thrown-cause? java.net.SocketTimeoutException
                           (.activate nimbus_client "security_auth_test_topology"))))
      (.close client))
    
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_bad_password.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})]
      (testing "(Negative authentication) Invalid  password"
        (is (thrown-cause? TTransportException
                           (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)))))
    
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_unknown_user.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})]
      (testing "(Negative authentication) Unknown user"
        (is (thrown-cause? TTransportException 
                           (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)))))

    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/nonexistent.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})]
      (testing "(Negative authentication) nonexistent configuration file"
        (is (thrown-cause? RuntimeException 
                           (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)))))
    
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_missing_client.conf"
                             STORM-NIMBUS-RETRY-TIMES 0})]
      (testing "(Negative authentication) Missing client"
        (is (thrown-cause? java.io.IOException
                           (NimbusClient. storm-conf "localhost" 6641 nimbus-timeout)))))))

(deftest test-GetTransportPlugin-throws-RuntimeException
  (let [conf (merge (read-storm-config)
                    {Config/STORM_THRIFT_TRANSPORT_PLUGIN "null.invalid"})]
    (is (thrown-cause? RuntimeException (AuthUtils/GetTransportPlugin conf nil nil)))))
