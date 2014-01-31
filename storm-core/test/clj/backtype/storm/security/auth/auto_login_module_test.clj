(ns backtype.storm.security.auth.auto-login-module-test
  (:use [clojure test])
  (:use [backtype.storm util])
  (:import [backtype.storm.security.auth.kerberos AutoTGT
            AutoTGTKrb5LoginModule AutoTGTKrb5LoginModuleTest])
  (:import [javax.security.auth Subject Subject])
  (:import [javax.security.auth.kerberos KerberosTicket])
  (:import [org.mockito Mockito])
  )

(deftest login-module-no-subj-no-tgt-test
  (testing "Behavior is correct when there is no Subject or TGT"
    (let [login-module (AutoTGTKrb5LoginModule.)]

      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.login login-module)))
      (is (not (.commit login-module)))
      (is (not (.abort login-module)))
      (is (.logout login-module)))))

(deftest login-module-readonly-subj-no-tgt-test
  (testing "Behavior is correct when there is a read-only Subject and no TGT"
    (let [readonly-subj (Subject. true #{} #{} #{})
          login-module (AutoTGTKrb5LoginModule.)]
      (.initialize login-module readonly-subj nil nil nil)
      (is (not (.commit login-module)))
      (is (.logout login-module)))))

(deftest login-module-with-subj-no-tgt-test
  (testing "Behavior is correct when there is a Subject and no TGT"
    (let [login-module (AutoTGTKrb5LoginModule.)]
      (.initialize login-module (Subject.) nil nil nil)
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.login login-module)))
      (is (not (.commit login-module)))
      (is (not (.abort login-module)))
      (is (.logout login-module)))))

(deftest login-module-no-subj-with-tgt-test
  (testing "Behavior is correct when there is no Subject and a TGT"
    (let [login-module (AutoTGTKrb5LoginModuleTest.)]
      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.login login-module))
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.commit login-module)))

      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.abort login-module))
      (is (.logout login-module)))))

(deftest login-module-readonly-subj-with-tgt-test
  (testing "Behavior is correct when there is a read-only Subject and a TGT"
    (let [readonly-subj (Subject. true #{} #{} #{})
          login-module (AutoTGTKrb5LoginModuleTest.)]
      (.initialize login-module readonly-subj nil nil nil)
      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.login login-module))
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.commit login-module)))

      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.abort login-module))
      (is (.logout login-module)))))

(deftest login-module-with-subj-and-tgt
  (testing "Behavior is correct when there is a Subject and a TGT"
    (let [login-module (AutoTGTKrb5LoginModuleTest.)
          _ (set! (. login-module client) (Mockito/mock
                                            java.security.Principal))
          ticket (Mockito/mock KerberosTicket)]
      (.initialize login-module (Subject.) nil nil nil)
      (.setKerbTicket login-module ticket)
      (is (.login login-module))
      (is (.commit login-module))
      (is (.abort login-module))
      (is (.logout login-module)))))
