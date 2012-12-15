(ns backtype.storm.security.auth.SaslServerCallbackHandler-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth SaslServerCallbackHandler]
           [javax.security.auth.login Configuration AppConfigurationEntry]
           [javax.security.auth.login AppConfigurationEntry$LoginModuleControlFlag]
           [javax.security.auth.callback NameCallback PasswordCallback]
           [javax.security.sasl AuthorizeCallback RealmCallback]
  )
)

(defn- mk-configuration-with-appconfig-mapping [mapping]
  ; The following defines a subclass of Configuration
  (proxy [Configuration] []
    (getAppConfigurationEntry [^String _]
      (into-array [(new AppConfigurationEntry "bogusLoginModuleName"
         AppConfigurationEntry$LoginModuleControlFlag/REQUIRED
         mapping
      )])
    )
  )
)

(defn- mk-configuration-with-null-appconfig []
  ; The following defines a subclass of Configuration
  (proxy [Configuration] []
    (getAppConfigurationEntry [^String nam] nil)
  )
)

(defn- handles-namecallback [handler username]
  (let [callback (new NameCallback "bogus prompt" username)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= (.getDefaultName callback) (.getName callback))
      "Sets default name")
  )
)

(defn- handles-passwordcallback [handler expected]
  (let [callback (new PasswordCallback "bogus prompt" false)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected (new String (.getPassword callback)))
      "Sets correct password when user credentials are present.")
  )
)

;(defn- does-not-set-passwd-if-noname [handler]
;  (let [callback (new PasswordCallback "bogus prompt" false)]
;    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
;    (is (= "" (new String (.getPassword callback)))
;      "Does not set password if no user name is set")
;  )
;)

;(defn- handles-authorized-callback [handler]
;  (let [
;         id "an ID"
;         callback
;           (new AuthorizeCallback id id)
;         another-id "bogus authorization ID"
;         callback2
;           (new AuthorizeCallback id another-id)
;       ]
;    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
;    (is (.isAuthorized callback))
;    (is (= id (.getAuthorizedID callback)))
;
;    (-> handler (.handle (into-array [callback2]))) ; side-effects on callback
;    (not (.isAuthorized callback))
;    (not (= another-id (.getAuthorizedID callback)))
;  )
;)

;(defn- handles-realm-callback [handler]
;  (let [
;        expected-default-text "the default text"
;        callback (new RealmCallback "bogus prompt" expected-default-text)
;       ]
;    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
;    (is (= expected-default-text (.getText callback)))
;  )
;)

(deftest handle-sets-callback-fields-properly
  (let [
        username "Test User"
        expected-password "a really lame password"
        mapping {(str "user_" username) expected-password}
        config (mk-configuration-with-appconfig-mapping mapping)
        handler (new SaslServerCallbackHandler config)
       ]
    (handles-namecallback handler username)
    (handles-passwordcallback handler expected-password)
;    (does-not-set-passwd-if-noname noname-handler)
;    (handles-authorized-callback handler)
;    (handles-realm-callback handler)
  )
)

(deftest handles-password-callback-for-super
  (let [
        username "super"
        expected-password "not a wise choice"
        mapping {(str "user_" username) expected-password}
        config (mk-configuration-with-appconfig-mapping mapping)
        handler (new SaslServerCallbackHandler config)
        name-callback (new NameCallback "bogus prompt" username)
        pass-callback (new PasswordCallback "bogus prompt" false)
       ]
    (java.lang.System/setProperty
      "storm.SASLAuthenticationProvider.superPassword" expected-password)
    (-> handler (.handle (into-array [name-callback]))) ; side-effects on name-callback
    (-> handler (.handle (into-array [pass-callback]))) ; side-effects on pass-callback
    (is (= expected-password (new String (.getPassword pass-callback)))
      "Sets correct password when user credentials are present.")

    ; Clean-up
    (java.lang.System/setProperty
      "storm.SASLAuthenticationProvider.superPassword" "")
  )
)

;(deftest throws-on-null-appconfig
;  (let [conf (mk-configuration-with-null-appconfig)]
;    (is (thrown? java.io.IOException
;      (new SaslServerCallbackHandler conf))
;      "Throws IOException when no AppConfiguration is given"
;    )
;  )
;)
