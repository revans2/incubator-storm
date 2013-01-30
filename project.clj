(def lein-version (System/getenv "LEIN_VERSION"))
(if-not (re-find #"^2\..*$" lein-version)
  (do (println (str "ERROR: requires Leiningen 2.x but you are using " lein-version))
    (System/exit 1)))

(defproject storm/storm "0.8.2"
  :source-paths ["src/clj" "src/clj/backtype/storm/"]
  :test-paths ["test/clj"]
  :java-source-paths ["src/jvm" "test/jvm"]
  :javac-options {:debug "true"}
  :resource-paths ["conf"]
  :repositories {"sonatype"
                 "http://oss.sonatype.org/content/groups/public/"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [commons-io "1.4"]
                 [org.apache.commons/commons-exec "1.1"]
                 [org.apache.zookeeper/zookeeper "3.4.5"]
                 [storm/libthrift7 "0.7.0"]
                 [clj-time "0.4.1"]
                 [log4j/log4j "1.2.16"]
                 [com.netflix.curator/curator-framework "1.0.1"]
                 [backtype/jzmq "2.1.0"]
                 [com.googlecode.json-simple/json-simple "1.1"]
                 [compojure "1.1.3"]
                 [hiccup "0.3.6"]
                 [ring/ring-jetty-adapter "0.3.11"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [org.slf4j/slf4j-log4j12 "1.6.1"]
                 [storm/carbonite "1.5.0"]
                 [org.yaml/snakeyaml "1.9"]
                 [org.apache.httpcomponents/httpclient "4.1.1"]
                 [storm/tools.cli "0.2.2"]
                 [com.googlecode.disruptor/disruptor "2.10.1"]
                 [storm/jgrapht "0.8.3"]
                 [com.google.guava/guava "13.0"]
                 ]
  :plugins [
            [lein-swank "1.4.1"]
            [lein-junit "1.0.3"]
           ]
  :junit ["test/jvm"]
  :profiles {:dev {:resource-paths ["src/ui" "src/dev"]
                   :dependencies [[junit/junit "4.10"]]
                  }}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib:/home/y/lib64"]
  :aot :all
  :min-lein-version "2.0.0")
