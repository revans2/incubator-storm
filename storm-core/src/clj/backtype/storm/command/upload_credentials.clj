(ns backtype.storm.command.upload-credentials
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated Credentials])
  (:gen-class))

;;TODO this is not tail recursive, we should make it that way
(defn to-map [[a b & more]]
  (if (empty? more)
    (if (or (nil? a) (nil? b))
      (throw (RuntimeException.  "Need an even number of arguments."))
      {a b})
    (if (even? (.size more))
      (assoc (to-map more) a b)
      (throw (RuntimeException.  "Need an even number of arguments.")))))

(defn -main [& args]
  (let [[name & rawCreds] args
        mapping (to-map rawCreds)
        creds (Credentials. )
        _ (.set_creds creds mapping)]
    (with-configured-nimbus-connection nimbus
      (.uploadNewCredentials nimbus name creds)
      (log-message "Uploaded new creds to topology: " name)
      )))
