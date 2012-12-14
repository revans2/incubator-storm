(ns backtype.storm.security.auth.SaslClientCallbackHandler-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth SaslClientCallbackHandler]
           [javax.security.auth.login Configuration AppConfigurationEntry]
           [javax.security.auth.login AppConfigurationEntry$LoginModuleControlFlag]
           [javax.security.auth.callback NameCallback PasswordCallback]
  )
)

; Exceptions are getting wrapped in RuntimeException.  This might be due to
; CLJ-855.
(defn- unpack-runtime-exception [expression]
  (try (eval expression)
    nil
    (catch java.lang.RuntimeException gripe
      (throw (.getCause gripe)))
  )
)

(defn- handles-namecallback [config handler expected]
  (let [callback (new NameCallback "bogus prompt" "not right")]
    (do
      (-> handler (.handle (into-array [callback]))) ; side-effects on callback
      (is (= expected (.getName callback)))
    )
  )
)

(defn- handles-passwordcallback [config handler expected]
  (let [callback (new PasswordCallback "bogus prompt" false)]
    (do
      (-> handler (.handle (into-array [callback]))) ; side-effects on callback
      (is (= expected (new String (.getPassword callback))))
    )
  )
)


(deftest handle-sets-correct-name-in-cb
  (let [
        expected-username "Test User"
        expected-password "a really lame password"
        config 
          (proxy [Configuration] []
            (getAppConfigurationEntry [^String nam]
              (into-array [(new AppConfigurationEntry "bogusLoginModuleName" 
                 AppConfigurationEntry$LoginModuleControlFlag/REQUIRED
                 {"username" expected-username
                  "password" expected-password}
              )])
            )
          )
        handler (new SaslClientCallbackHandler config)
       ]
    (handles-namecallback config handler expected-username)
    (handles-passwordcallback config handler expected-password)
  )
)
