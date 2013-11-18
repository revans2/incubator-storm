(ns backtype.storm.ui-test
  (:use [clojure test])
  (:use [backtype.storm config])
  (:use [backtype.storm testing])
  (:require [backtype.storm.ui [core :as core]])
  )

(deftest test-authorized-ui-user
  (testing "allow cluster admin"
    (let [conf {NIMBUS-ADMINS ["alice"]}]
      (is (core/authorized-ui-user? "alice" conf {}))))

  (testing "ignore any cluster-set topology.users"
    (let [conf {TOPOLOGY-USERS ["alice"]}]
      (is (not (core/authorized-ui-user? "alice" conf {})))))

  (testing "allow cluster ui user"
    (let [conf {UI-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" conf {}))))

  (testing "allow submitted topology user"
    (let [topo-conf {TOPOLOGY-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" {} topo-conf))))

  (testing "allow submitted ui user"
    (let [topo-conf {UI-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" {} topo-conf))))

  (testing "disallow user not in nimbus admin, topo user, or ui user"
    (is (not (core/authorized-ui-user? "alice" {} {}))))

  (testing "user cannot override nimbus admin"
    (let [topo-conf {NIMBUS-ADMINS ["alice"]}]
      (is (not (core/authorized-ui-user? "alice" {} topo-conf))))))
