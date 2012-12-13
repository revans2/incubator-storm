(ns backtype.storm.AnonymousAuthenticationProvider-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth AnonymousAuthenticationProvider$SaslAnonymousFactory])
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

(deftest test-factory-returns-correct-client-for-mechanism
   (println (map #(.getName %) (.getMethods AnonymousAuthenticationProvider$SaslAnonymousFactory)))
;   (AnonymousAuthenticationProvider$SaslAnonymousFactory/foo 42)
;   (AnonymousAuthenticationProvider$SaslAnonymousFactory/createSaslClient [""] "" "" "" {} nil)
;  (is
;    (nil?
;      (AnonymousAuthenticationProvider$SaslAnonymousFactory/createSaslClient
;         ["NOTANONYMOUS" "foobar"] nil nil nil nil nil)))
)

;(deftest test-ctor-throws-if-port-invalid
;  (is (thrown? java.lang.IllegalArgumentException
;    (unpack-runtime-exception
;      '(ThriftClient. "bogushost" -1 "Fake Service Name"))))
;  (is
;    (thrown? java.lang.IllegalArgumentException
;      (unpack-runtime-exception
;        '(ThriftClient. "bogushost" 0 "Fake Service Name"))))
;)
