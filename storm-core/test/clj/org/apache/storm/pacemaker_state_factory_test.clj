(ns org.apache.storm.pacemaker-state-factory-test
  (:require [clojure.test :refer :all]
            [conjure.core :refer :all]
            [org.apache.storm.pacemaker [pacemaker-state-factory :as psf]])
  (:import [backtype.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType HBMessage HBMessageData HBPulse]
           [backtype.storm.cluster ClusterStateContext]
           [org.mockito Mockito Matchers]))

(defn- string-to-bytes [string]
  (byte-array (map int string)))

(defn- bytes-to-string [bytez]
  (apply str (map char bytez)))

(defprotocol send-capture
  (send [this something])
  (check-captured [this]))

(defn- make-send-capture [response]
  (let [captured (atom nil)]
    (reify send-capture
      (send [this something] (reset! captured something) response)
      (check-captured [this] @captured))))

(defmacro with-mock-pacemaker-client-and-state [client state conf response & body]
  `(let [~client (make-send-capture ~response)]
     (stubbing [psf/makeZKState nil
                psf/launch-cleanup-hb-thread nil
                psf/clojurify-details {:time-secs 1056}
                psf/try-reconnect nil
                psf/get-clients [~client]]
               (let [~conf {"pacemaker.servers" ["host"]}
                     ~state (psf/-mkState nil ~conf nil nil (ClusterStateContext.))]
                 ~@body))))

(deftest pacemaker_state_set_worker_hb
  (testing "set_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/SEND_PULSE_RESPONSE nil)

      (.set_worker_hb state "/foo" (string-to-bytes "data") nil)
      (let [sent (.check-captured client)
            pulse (.get_pulse (.get_data sent))]
        (is (= (.get_type sent) HBServerMessageType/SEND_PULSE))
        (is (= (.get_id pulse) "/foo"))
        (is (= (bytes-to-string (.get_details pulse)) "data")))))

  (testing "set_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/SEND_PULSE nil)

      (is (thrown? HBExecutionException
                   (.set_worker_hb state "/foo" (string-to-bytes "data") nil))))))

(deftest pacemaker_state_delete_worker_hb
  (testing "delete_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/DELETE_PATH_RESPONSE nil)

      (.delete_worker_hb state "/foo/bar")
      (let [sent (.check-captured client)]
        (is (= (.get_type sent) HBServerMessageType/DELETE_PATH))
        (is (= (.get_path (.get_data sent)) "/foo/bar")))))

    (testing "delete_worker_hb"
      (with-mock-pacemaker-client-and-state
        client state conf
        (HBMessage. HBServerMessageType/DELETE_PATH nil)

        (is (thrown? HBExecutionException
                     (.delete_worker_hb state "/foo/bar"))))))

(deftest pacemaker_state_get_worker_hb
  (testing "get_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE
                (HBMessageData/pulse
                 (doto (HBPulse.)
                   (.set_id "/foo")
                   (.set_details (string-to-bytes "some data")))))

      (.get_worker_hb state "/foo" false)
      (let [sent (.check-captured client)]
        (is (= (.get_type sent) HBServerMessageType/GET_PULSE))
        (is (= (.get_path (.get_data sent)) "/foo")))))

  (testing "get_worker_hb - fail (bad response)"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/GET_PULSE nil)

      (is (thrown? HBExecutionException
                   (.get_worker_hb state "/foo" false)))))

  (testing "get_worker_hb - fail (bad data)"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE nil)

      (is (thrown? HBExecutionException
                   (.get_worker_hb state "/foo" false))))))

(deftest pacemaker_state_get_worker_hb_children
  (testing "get_worker_hb_children"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE
                (HBMessageData/nodes
                 (HBNodes. [])))

      (.get_worker_hb_children state "/foo" false)
      (let [sent (.check-captured client)]
        (is (= (.get_type sent) HBServerMessageType/GET_ALL_NODES_FOR_PATH))
        (is (= (.get_path (.get_data sent)) "/foo")))))

  (testing "get_worker_hb_children - fail (bad response)"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/DELETE_PATH nil)

      (is (thrown? HBExecutionException
                   (.get_worker_hb_children state "/foo" false)))))

    (testing "get_worker_hb_children - fail (bad data)"
    (with-mock-pacemaker-client-and-state
      client state conf
      (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE nil)

      (is (thrown? HBExecutionException
                   (.get_worker_hb_children state "/foo" false))))))

(deftest get_worker_hb_time_secs
  (testing "get_worker_hb_time_secs"
    (stubbing [psf/clojurify-details {:time-secs 1056}]
      (let [list (psf/get-wk-hb-time-secs-pair #{"details"})]
        (is (= list [[1056 "details"]]))))))
