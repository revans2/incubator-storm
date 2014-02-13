(ns backtype.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer-test
  (:use [clojure test])
  (:import [org.mockito Mockito])
  (:import [backtype.storm Config])
  (:import [backtype.storm.security.auth ReqContext SingleUserPrincipal])
  (:import [backtype.storm.security.auth.authorizer DRPCSimpleACLAuthorizer])
  (:use [backtype.storm config util])
  )

(defn- mk-mock-context [user]
  (let [mock-context (Mockito/mock ReqContext)]
    (. (Mockito/when (.principal mock-context)) thenReturn
       (SingleUserPrincipal. user))
    mock-context))

(let [function "jump"
      alice-context (mk-mock-context "alice")
      bob-context (mk-mock-context "bob")
      strict-handler (doto (DRPCSimpleACLAuthorizer.)
                           (.prepare {DRPC-AUTHORIZER-ACL-STRICT true}))
      permissive-handler (doto (DRPCSimpleACLAuthorizer.)
                               (.prepare {DRPC-AUTHORIZER-ACL-STRICT false}))
      acl-file "resources/drpc-simple-acl-test-scenario.yaml"]

  (.setAclFileName strict-handler acl-file)
  (.setAclFileName permissive-handler acl-file)

  (deftest test-client-authorization-strict
    (testing "deny execute to unauthorized user"
      (is (not
            (.permit strict-handler
                     (ReqContext/context)
                     "execute"
                     {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))

    (testing "deny execute to valid user for incorrect function"
      (is (not
            (.permit
              strict-handler
              alice-context
              "execute"
              {DRPCSimpleACLAuthorizer/FUNCTION_KEY "wrongFunction"}))))

    (testing "allow execute to authorized user for correct function"
      (is
        (.permit
          strict-handler
          alice-context
          "execute"
          {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))


  (deftest test-client-authorization-permissive
    (testing "deny execute to unauthorized user for correct function"
      (is (not
            (.permit permissive-handler
                     (ReqContext/context)
                     "execute"
                     {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))

    (testing "allow execute for user for incorrect function when permissive"
      (is
        (.permit permissive-handler
                 alice-context
                 "execute"
                 {DRPCSimpleACLAuthorizer/FUNCTION_KEY "wrongFunction"})))

    (testing "allow execute to authorized user for correct function"
      (is
        (.permit permissive-handler
                 alice-context
                 "execute"
                 {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))

  (deftest test-invocation-authorization-strict
    (doseq [operation ["fetchRequest" "failRequest" "result"]]

      (testing (str "deny " operation
                    " to unauthorized user for correct function")
        (is (not
              (.permit
                strict-handler
                alice-context
                operation
                {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))

      (testing (str "deny " operation
                    " to user for incorrect function when strict")
        (is (not
              (.permit
                strict-handler
                bob-context
                operation
                {DRPCSimpleACLAuthorizer/FUNCTION_KEY "wrongFunction"}))))

      (testing (str "allow " operation
                    " to authorized user for correct function")
        (is
          (.permit
            strict-handler
            bob-context
            operation
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))))

  (deftest test-invocation-authorization-permissive
    (doseq [operation ["fetchRequest" "failRequest" "result"]]

      (testing (str "deny " operation
                    " to unauthorized user for correct function")
        (is (not
              (.permit
                permissive-handler
                alice-context
                operation
                {DRPCSimpleACLAuthorizer/FUNCTION_KEY function}))))

        (testing (str "allow " operation
                      " to user for incorrect function when permissive")
          (is
            (.permit
              permissive-handler
              bob-context
              operation
              {DRPCSimpleACLAuthorizer/FUNCTION_KEY "wrongFunction"})))

      (testing (str operation " is allowed for authorized user")
        (is
          (.permit
            permissive-handler
            bob-context
            operation
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))))

  (deftest test-deny-when-no-function-given
    (is (not
         (.permit strict-handler alice-context "execute" {})))

    (is (not
         (.permit
           strict-handler
           alice-context
           "execute"
           {DRPCSimpleACLAuthorizer/FUNCTION_KEY nil})))

    (is (not
         (.permit permissive-handler alice-context "execute" {})))

    (is (not
         (.permit
           permissive-handler
           alice-context
           "execute"
           {DRPCSimpleACLAuthorizer/FUNCTION_KEY nil}))))

  (deftest test-deny-when-invalid-user-given
    (is (not
          (.permit
            strict-handler
            (Mockito/mock ReqContext)
            "execute"
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))

    (is (not
          (.permit
            strict-handler
            nil
            "execute"
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))

    (is (not
          (.permit
            permissive-handler
            (Mockito/mock ReqContext)
            "execute"
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))

    (is (not
          (.permit
            permissive-handler
            nil
            "execute"
            {DRPCSimpleACLAuthorizer/FUNCTION_KEY function})))))
