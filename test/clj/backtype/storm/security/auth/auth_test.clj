(ns backtype.storm.security.auth.auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth ThriftServer ThriftClient ReqContext ReqContext$OperationType])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client])
  )

(bootstrap)

(defn mk-authorization-handler [conf]
  (let [klassname (conf NIMBUS-AUTHORIZATION-CLASSNAME) 
        aznClass (if klassname (Class/forName klassname))
        aznHandler (if aznClass (.newInstance aznClass))] 
    (log-debug "authorization class name:" klassname
                 " class:" aznClass
                 " handler:" aznHandler)
    aznHandler
  ))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler conf)
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

(defn update-req-context! [nimbus storm-name storm-conf operation]
  (let [req (ReqContext/context)]
    (.setOperation req operation)
    (if storm-conf (.setTopologyConf req storm-conf) 
      (let [topologyConf { TOPOLOGY-NAME storm-name} ]
        (.setTopologyConf req topologyConf)))
    req))

(defn check-authorization! [nimbus storm-name storm-conf operation]
 (let [aclHandler (:authorization-handler nimbus)]
   (log-debug "check-authorization with handler: " aclHandler)
   (if aclHandler
       (let [req (update-req-context! nimbus storm-name storm-conf operation)]
        (if-not (.permit aclHandler req)
          (throw (RuntimeException. (str operation " on topology " storm-name " is not authorized")))
          )))))

(defn dummy-service-handler [conf inimbus]
  (let [nimbus (nimbus-data conf inimbus)]
     (reify Nimbus$Iface
      (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
      	     			     ^SubmitOptions submitOptions]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/SUBMIT_TOPOLOGY)))
      
      (^void killTopology [this ^String storm-name]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/KILL_TOPOLOGY)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/KILL_TOPOLOGY)))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/REBALANCE_TOPOLOGY)))

      (activate [this storm-name]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/ACTIVATE_TOPOLOGY)))

      (deactivate [this storm-name]
	(check-authorization! nimbus storm-name nil (ReqContext$OperationType/DEACTIVATE_TOPOLOGY)))

      (beginFileUpload [this])

      (^void uploadChunk [this ^String location ^ByteBuffer chunk])

      (^void finishFileUpload [this ^String location])

      (^String beginFileDownload [this ^String file])

      (^ByteBuffer downloadChunk [this ^String id])

      (^String getNimbusConf [this])

      (^String getTopologyConf [this ^String id])

      (^StormTopology getTopology [this ^String id])

      (^StormTopology getUserTopology [this ^String id])

      (^ClusterSummary getClusterInfo [this])
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]))))

(defn launch-test-server [server-port login-cfg aznClass] 
    (System/setProperty "java.security.auth.login.config" login-cfg)
    (let [conf (merge (read-storm-config)
                     {NIMBUS-AUTHORIZATION-CLASSNAME aznClass 
		      NIMBUS-HOST "localhost"
		      NIMBUS-THRIFT-PORT server-port})
           nimbus (nimbus/standalone-nimbus)
           service-handler (dummy-service-handler conf nimbus)
           server (ThriftServer. (Nimbus$Processor. service-handler) (int (conf NIMBUS-THRIFT-PORT)))]
    	   (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    	   (log-message "Starting Nimbus server...")
    	   (.serve server)))

(defn launch-server-w-wait [server-port ms login-cfg aznClass]
   (.start (Thread. #(launch-test-server server-port login-cfg aznClass)))
   (log-message "Waiting for Nimbus Server...")
   (Thread/sleep ms)
   (log-message "Continue..."))

(deftest authorization-test 
  (launch-server-w-wait 6627 2000 "" "backtype.storm.security.auth.DenyAuthorizer")
  (log-message "Starting Nimbus client w/ anonymous authentication")
  (let [client (NimbusClient. "localhost" 6627)
        nimbus_client (.getClient client)]
     (is (thrown? TTransportException
            (.activate nimbus_client "security_auth_test_topology")))
     (.close client)))

(deftest authentication-test
  (launch-server-w-wait 6628 2000 "./conf/jaas_digest.conf" "backtype.storm.security.auth.NoopAuthorizer")
  (System/setProperty "java.security.auth.login.config" "")
  (log-message "Starting Nimbus client w/ anonymous authentication (expect authentication failure")
  (is (= "Peer indicated failure: Unsupported mechanism type ANONYMOUS" 
         (try (NimbusClient. "localhost" 6628)
	      nil
              (catch java.lang.RuntimeException ex 
	      	 (.getMessage (.getCause ex))))))
  (log-message "Starting Nimbus client w/ digest authentication (expect authentication success)")
  (System/setProperty "java.security.auth.login.config" "./conf/jaas_digest.conf")
  (let [client (NimbusClient. "localhost" 6628)
        nimbus_client (.getClient client)]
     (.activate nimbus_client "security_auth_test_topology")
     (.close client)))

