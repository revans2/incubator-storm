(ns backtype.storm.command.upload-credentials
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm log util])
  (:import [backtype.storm StormSubmitter])
  (:import [java.util Properties])
  (:import [java.io FileReader])
  (:gen-class))

(defn read-map [file-name]
  (let [props (Properties. )
        _ (.load props (FileReader. file-name))]
    (clojurify-structure props)))

(defn -main [& args]
  (let [[{cred-file :file} [name & rawCreds]] (cli args ["-f" "--file" :default nil])
        _ (when (and rawCreds (not (even? (.size rawCreds)))) (throw (RuntimeException.  "Need an even number of arguments to make a map")))
        mapping (if rawCreds (apply assoc {} rawCreds) {})
        file-mapping (if (nil? cred-file) {} (read-map cred-file))]
      (StormSubmitter/pushCredentials name {} (merge file-mapping mapping))
      (log-message "Uploaded new creds to topology: " name)))
