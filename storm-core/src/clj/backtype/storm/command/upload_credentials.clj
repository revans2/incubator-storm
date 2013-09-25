(ns backtype.storm.command.upload-credentials
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log util])
  (:import [backtype.storm.generated Credentials])
  (:import [java.util Properties])
  (:import [java.io FileReader])
  (:gen-class))

(defn to-map 
  ([vect] (to-map vect {}))
  ([[a b & more] ret]
    (if (empty? more)
      (assoc ret a b)
      (recur more (assoc ret a b)))))

(defn read-map [file-name]
  (let [props (Properties. )
        _ (.load props (FileReader. file-name))]
    (clojurify-structure props)))

(defn -main [& args]
  (let [[{cred-file :file} [name & rawCreds]] (cli args ["-f" "--file" :default nil])
        _ (when (not (even? (.size rawCreds))) (throw (RuntimeException.  "Need an even number of arguments to make a map")))
        mapping (to-map rawCreds)
        file-mapping (if (nil? cred-file) {} (read-map cred-file))
        creds (Credentials. )
        _ (.set_creds creds (merge file-mapping mapping))]
    (with-configured-nimbus-connection nimbus
      (.uploadNewCredentials nimbus name creds)
      (log-message "Uploaded new creds to topology: " name)
      )))
