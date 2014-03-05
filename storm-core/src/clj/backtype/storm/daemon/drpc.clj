(ns backtype.storm.daemon.drpc
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [org.apache.thrift7 TException])
  (:import [backtype.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor
            DRPCRequest DRPCExecutionException DistributedRPCInvocations DistributedRPCInvocations$Iface
            DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:import [backtype.storm.generated AuthorizationException])
  (:use [backtype.storm bootstrap config log])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm.ui helpers])
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:require [compojure.handler :as handler])
  (:gen-class))

(bootstrap)

(def TIMEOUT-CHECK-SECS 5)

(defn acquire-queue [queues-atom function]
  (swap! queues-atom
    (fn [amap]
      (if-not (amap function)
        (assoc amap function (ConcurrentLinkedQueue.))
        amap)
        ))
  (@queues-atom function))

(defn check-authorization! [aclHandler storm-conf operation]
  (log-debug "DRPC check-authorization with handler: " aclHandler)
  (if aclHandler
    (if-not (.permit aclHandler (ReqContext/context) operation storm-conf)
          (throw (AuthorizationException. (str "DRPC request " operation " is not authorized")))
          )))

;; TODO: change this to use TimeCacheMap
(defn service-handler [conf]
  (let [drpc-acl-handler (mk-authorization-handler (conf DRPC-AUTHORIZER) conf)
        invocations-acl-handler (mk-authorization-handler (conf DRPC-INVOCATIONS-AUTHORIZER) conf)
        ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                         (swap! id->result dissoc id)
                         (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        clear-thread (async-loop
                      (fn []
                        (doseq [[id start] @id->start]
                          (when (> (time-delta start) (conf DRPC-REQUEST-TIMEOUT-SECS))
                            (when-let [sem (@id->sem id)]
                              (swap! id->result assoc id (DRPCExecutionException. "Request timed out"))
                              (.release sem))
                            (cleanup id)
                            ))
                        TIMEOUT-CHECK-SECS
                        ))
        ]
    (reify DistributedRPC$Iface
      (^String execute [this ^String function ^String args]
        (log-debug "Received DRPC request for " function " " args " at " (System/currentTimeMillis))
        (check-authorization! drpc-acl-handler conf "execute")
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. args id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues function)
              ]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (.add queue req)
          (log-debug "Waiting for DRPC result for " function " " args " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired DRPC result for " function " " args " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning DRPC result for " function " " args " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              result
              ))))
      DistributedRPCInvocations$Iface
      (^void result [this ^String id ^String result]
        (check-authorization! invocations-acl-handler conf "result")
        (let [^Semaphore sem (@id->sem id)]
          (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
          (when sem
            (swap! id->result assoc id result)
            (.release sem)
            )))
      (^void failRequest [this ^String id]
        (check-authorization! invocations-acl-handler conf "failRequest")
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
            (.release sem)
            )))
      (^DRPCRequest fetchRequest [this ^String func]
        (check-authorization! invocations-acl-handler conf "fetchRequest")
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for " func " at " (System/currentTimeMillis))
                ret)
            (DRPCRequest. "" ""))
          ))
      Shutdownable
      (shutdown [this]
        (.interrupt clear-thread))
      )))

(defn handle-request [handler]
  (fn [request]
    (handler request)))

(defn webapp [handler http-creds-handler]
  (handler/site (->
    (defroutes http-routes
      (GET "/drpc/:func/:args" [:as {:keys [servlet-request]} func args & m]
        (let [user (.getUserName http-creds-handler servlet-request)]
          (log-debug "Servlet user was: " user)
          (.execute handler func args)))
      (GET "/drpc/:func/" [:as {:keys [servlet-request]} func & m]
        (let [user (.getUserName http-creds-handler servlet-request)]
          (log-debug "Servlet user was: " user)
          (.execute handler func "")))
      (GET "/drpc/:func" [:as {:keys [servlet-request]} func & m]
        (let [user (.getUserName http-creds-handler servlet-request)]
          (log-debug "Servlet user was: " user)
          (.execute handler func ""))))
    (wrap-reload '[backtype.storm.daemon.drpc])
    handle-request)))

(defn launch-server!
  ([]
    (let [conf (read-storm-config)
          worker-threads (int (conf DRPC-WORKER-THREADS))
          queue-size (int (conf DRPC-QUEUE-SIZE))
          drpc-http-port (int (conf DRPC-HTTP-PORT))
          drpc-port (int (conf DRPC-PORT))
          drpc-service-handler (service-handler conf)
          ;; requests and returns need to be on separate thread pools, since calls to
          ;; "execute" don't unblock until other thrift methods are called. So if 
          ;; 64 threads are calling execute, the server won't accept the result
          ;; invocations that will unblock those threads
          handler-server (when (> drpc-port 0)
                           (ThriftServer. conf
                             (DistributedRPC$Processor. drpc-service-handler)
                             drpc-port
                             backtype.storm.Config$ThriftServerPurpose/DRPC
                             (ThreadPoolExecutor. worker-threads worker-threads
                               60 TimeUnit/SECONDS (ArrayBlockingQueue. queue-size))))
          invoke-server (ThriftServer. conf
                          (DistributedRPCInvocations$Processor. drpc-service-handler)
                          (int (conf DRPC-INVOCATIONS-PORT))
                          backtype.storm.Config$ThriftServerPurpose/DRPC)
          http-creds-handler (AuthUtils/GetDrpcHttpCredentialsPlugin conf)]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                        (if handler-server (.stop handler-server))
                                                        (.stop invoke-server))))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (when (> drpc-http-port 0)
        (let [app (webapp drpc-service-handler http-creds-handler)
              filter-class (conf DRPC-HTTP-FILTER)
              filter-params (conf DRPC-HTTP-FILTER-PARAMS)
              https-port (int (conf DRPC-HTTPS-PORT))
              https-ks-path (conf DRPC-HTTPS-KEYSTORE-PATH)
              https-ks-password (conf DRPC-HTTPS-KEYSTORE-PASSWORD)
              https-ks-type (conf DRPC-HTTPS-KEYSTORE-TYPE)]

          (run-jetty app
            {:port drpc-http-port :join? false
             :configurator (fn [server]
                             (config-ssl server
                                         https-port 
                                         https-ks-path 
                                         https-ks-password
                                         https-ks-type)
                             (config-filter server app
                                            filter-class
                                            filter-params))})))
      (when handler-server
        (.serve handler-server)))))

(defn -main []
  (launch-server!))
