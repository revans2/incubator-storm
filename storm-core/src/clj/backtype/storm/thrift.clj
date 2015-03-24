;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns backtype.storm.thrift
  (:import [java.util HashMap])
  (:import [backtype.storm.generated JavaObject Grouping Nimbus StormTopology
            StormTopology$_Fields Bolt Nimbus$Client Nimbus$Iface
            ComponentCommon Grouping$_Fields SpoutSpec NullStruct StreamInfo
            GlobalStreamId ComponentObject ComponentObject$_Fields
            ShellComponent InvalidTopologyException])
  (:import [backtype.storm.utils Utils NimbusClient])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.grouping CustomStreamGrouping])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.clojure RichShellBolt RichShellSpout])
  (:import [org.apache.thrift.transport TTransport])
  (:import [backtype.storm.metric SystemBolt])
  (:require [clojure.set :as set])  
  (:use [backtype.storm util config log]
        [backtype.storm.daemon common]))

(defn instantiate-java-object
  [^JavaObject obj]
  (let [name (symbol (.get_full_class_name obj))
        args (map (memfn getFieldValue) (.get_args_list obj))]
    (eval `(new ~name ~@args))))

(def grouping-constants
  {Grouping$_Fields/FIELDS :fields
   Grouping$_Fields/SHUFFLE :shuffle
   Grouping$_Fields/ALL :all
   Grouping$_Fields/NONE :none
   Grouping$_Fields/CUSTOM_SERIALIZED :custom-serialized
   Grouping$_Fields/CUSTOM_OBJECT :custom-object
   Grouping$_Fields/DIRECT :direct
   Grouping$_Fields/LOCAL_OR_SHUFFLE :local-or-shuffle})

(defn grouping-type
  [^Grouping grouping]
  (grouping-constants (.getSetField grouping)))

(defn field-grouping
  [^Grouping grouping]
  (when-not (= (grouping-type grouping) :fields)
    (throw (IllegalArgumentException. "Tried to get grouping fields from non fields grouping")))
  (.get_fields grouping))

(defn global-grouping?
  [^Grouping grouping]
  (and (= :fields (grouping-type grouping))
       (empty? (field-grouping grouping))))

(defn parallelism-hint
  [^ComponentCommon component-common]
  (let [phint (.get_parallelism_hint component-common)]
    (if-not (.is_set_parallelism_hint component-common) 1 phint)))

(defn nimbus-client-and-conn
  [host port]
  (log-message "Connecting to Nimbus at " host ":" port)
  (let [conf (read-storm-config)
        nimbusClient (NimbusClient. conf host port nil)
        client (.getClient nimbusClient)
        transport (.transport nimbusClient)]
        [client transport] ))

(defmacro with-nimbus-connection
  [[client-sym host port] & body]
  `(let [[^Nimbus$Client ~client-sym ^TTransport conn#] (nimbus-client-and-conn ~host ~port)]
    (try
      ~@body
    (finally (.close conn#)))))

(defmacro with-configured-nimbus-connection
  [client-sym & body]
  `(let [conf# (read-storm-config)
         host# (conf# NIMBUS-HOST)
         port# (conf# NIMBUS-THRIFT-PORT)]
    (with-nimbus-connection [~client-sym host# port#]
      ~@body)))

(defn direct-output-fields
  [fields]
  (StreamInfo. fields true))

(defn output-fields
  [fields]
  (StreamInfo. fields false))

(defn mk-output-spec
  [output-spec]
  (let [output-spec (if (map? output-spec)
                      output-spec
                      {Utils/DEFAULT_STREAM_ID output-spec})]
    (map-val
      (fn [out]
        (if (instance? StreamInfo out)
          out
          (StreamInfo. out false)))
      output-spec)))

(defnk mk-plain-component-common
  [inputs output-spec parallelism-hint :conf nil]
  (let [ret (ComponentCommon. (HashMap. inputs) (HashMap. (mk-output-spec output-spec)))]
    (when parallelism-hint
      (.set_parallelism_hint ret parallelism-hint))
    (when conf
      (.set_json_conf ret (to-json conf)))
    ret))

(defnk mk-spout-spec*
  [spout outputs :p nil :conf nil]
  (SpoutSpec. (ComponentObject/serialized_java (Utils/serialize spout))
              (mk-plain-component-common {} outputs p :conf conf)))

(defn mk-shuffle-grouping
  []
  (Grouping/shuffle (NullStruct.)))

(defn mk-local-or-shuffle-grouping
  []
  (Grouping/local_or_shuffle (NullStruct.)))

(defn mk-fields-grouping
  [fields]
  (Grouping/fields fields))

(defn mk-global-grouping
  []
  (mk-fields-grouping []))

(defn mk-direct-grouping
  []
  (Grouping/direct (NullStruct.)))

(defn mk-all-grouping
  []
  (Grouping/all (NullStruct.)))

(defn mk-none-grouping
  []
  (Grouping/none (NullStruct.)))

(defn deserialized-component-object
  [^ComponentObject obj]
  (when (not= (.getSetField obj) ComponentObject$_Fields/SERIALIZED_JAVA)
    (throw (RuntimeException. "Cannot deserialize non-java-serialized object")))
  (Utils/deserialize (.get_serialized_java obj)))

(defn serialize-component-object
  [obj]
  (ComponentObject/serialized_java (Utils/serialize obj)))

(defn- mk-grouping
  [grouping-spec]
  (cond (nil? grouping-spec)
        (mk-none-grouping)

        (instance? Grouping grouping-spec)
        grouping-spec

        (instance? CustomStreamGrouping grouping-spec)
        (Grouping/custom_serialized (Utils/serialize grouping-spec))

        (instance? JavaObject grouping-spec)
        (Grouping/custom_object grouping-spec)

        (sequential? grouping-spec)
        (mk-fields-grouping grouping-spec)

        (= grouping-spec :shuffle)
        (mk-shuffle-grouping)

        (= grouping-spec :local-or-shuffle)
        (mk-local-or-shuffle-grouping)
        (= grouping-spec :none)
        (mk-none-grouping)

        (= grouping-spec :all)
        (mk-all-grouping)

        (= grouping-spec :global)
        (mk-global-grouping)

        (= grouping-spec :direct)
        (mk-direct-grouping)

        true
        (throw (IllegalArgumentException.
                 (str grouping-spec " is not a valid grouping")))))

(defn- mk-inputs
  [inputs]
  (into {} (for [[stream-id grouping-spec] inputs]
             [(if (sequential? stream-id)
                (GlobalStreamId. (first stream-id) (second stream-id))
                (GlobalStreamId. stream-id Utils/DEFAULT_STREAM_ID))
              (mk-grouping grouping-spec)])))

(defnk mk-bolt-spec*
  [inputs bolt outputs :p nil :conf nil]
  (let [common (mk-plain-component-common (mk-inputs inputs) outputs p :conf conf)]
    (Bolt. (ComponentObject/serialized_java (Utils/serialize bolt))
           common)))

(defnk mk-spout-spec
  [spout :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj spout :p parallelism-hint :conf conf}))

(defn- shell-component-params
  [command script-or-output-spec kwargs]
  (if (string? script-or-output-spec)
    [(into-array String [command script-or-output-spec])
     (first kwargs)
     (rest kwargs)]
    [(into-array String command)
     script-or-output-spec
     kwargs]))

(defnk mk-bolt-spec
  [inputs bolt :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj bolt :inputs inputs :p parallelism-hint :conf conf}))

(defn mk-shell-bolt-spec
  [inputs command script-or-output-spec & kwargs]
  (let [[command output-spec kwargs]
        (shell-component-params command script-or-output-spec kwargs)]
    (apply mk-bolt-spec inputs
           (RichShellBolt. command (mk-output-spec output-spec)) kwargs)))

(defn mk-shell-spout-spec
  [command script-or-output-spec & kwargs]
  (let [[command output-spec kwargs]
        (shell-component-params command script-or-output-spec kwargs)]
    (apply mk-spout-spec
           (RichShellSpout. command (mk-output-spec output-spec)) kwargs)))

(defn- add-inputs
  [declarer inputs]
  (doseq [[id grouping] (mk-inputs inputs)]
    (.grouping declarer id grouping)))

(defn mk-topology
  ([spout-map bolt-map]
   (let [builder (TopologyBuilder.)]
     (doseq [[name {spout :obj p :p conf :conf}] spout-map]
       (-> builder (.setSpout name spout (if-not (nil? p) (int p) p)) (.addConfigurations conf)))
     (doseq [[name {bolt :obj p :p conf :conf inputs :inputs}] bolt-map]
       (-> builder (.setBolt name bolt (if-not (nil? p) (int p) p)) (.addConfigurations conf) (add-inputs inputs)))
     (.createTopology builder)))
  ([spout-map bolt-map state-spout-map]
   (mk-topology spout-map bolt-map)))

;; clojurify-structure is needed or else every element becomes the same after successive calls
;; don't know why this happens
(def STORM-TOPOLOGY-FIELDS
  (-> StormTopology/metaDataMap clojurify-structure keys))

(def SPOUT-FIELDS
  [StormTopology$_Fields/SPOUTS
   StormTopology$_Fields/STATE_SPOUTS])


;; Moved from daemon/common.clj
(defn- validate-ids! [^StormTopology topology]
  (let [sets (map #(.getFieldValue topology %) STORM-TOPOLOGY-FIELDS)
        offending (apply any-intersection sets)]
    (if-not (empty? offending)
      (throw (InvalidTopologyException.
              (str "Duplicate component ids: " offending))))
    (doseq [f STORM-TOPOLOGY-FIELDS
            :let [obj-map (.getFieldValue topology f)]]
      (doseq [id (keys obj-map)]
        (if (system-id? id)
          (throw (InvalidTopologyException.
                  (str id " is not a valid component id")))))
      (doseq [obj (vals obj-map)
              id (-> obj .get_common .get_streams keys)]
        (if (system-id? id)
          (throw (InvalidTopologyException.
                  (str id " is not a valid stream id"))))))))

(defn all-components [^StormTopology topology]
  (apply merge {}
         (for [f STORM-TOPOLOGY-FIELDS]
           (.getFieldValue topology f))))

(defn validate-basic! [^StormTopology topology]
  (validate-ids! topology)
  (doseq [f SPOUT-FIELDS
          obj (->> f (.getFieldValue topology) vals)]
    (if-not (empty? (-> obj .get_common .get_inputs))
      (throw (InvalidTopologyException. "May not declare inputs for a spout"))))
  (doseq [[comp-id comp] (all-components topology)
          :let [conf (component-conf comp)
                p (-> comp .get_common parallelism-hint)]]
    (when (and (> (conf TOPOLOGY-TASKS) 0)
               p
               (<= p 0))
      (throw (InvalidTopologyException. "Number of executors must be greater than 0 when number of tasks is greater than 0")))))


(defn validate-structure! [^StormTopology topology]
  ;; validate all the component subscribe from component+stream which actually exists in the topology
  ;; and if it is a fields grouping, validate the corresponding field exists  
  (let [all-components (all-components topology)]
    (doseq [[id comp] all-components
            :let [inputs (.. comp get_common get_inputs)]]
      (doseq [[global-stream-id grouping] inputs
              :let [source-component-id (.get_componentId global-stream-id)
                    source-stream-id    (.get_streamId global-stream-id)]]
        (if-not (contains? all-components source-component-id)
          (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent component [" source-component-id "]")))
          (let [source-streams (-> all-components (get source-component-id) .get_common .get_streams)]
            (if-not (contains? source-streams source-stream-id)
              (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent stream: [" source-stream-id "] of component [" source-component-id "]")))
              (if (= :fields (grouping-type grouping))
                (let [grouping-fields (set (.get_fields grouping))
                      source-stream-fields (-> source-streams (get source-stream-id) .get_output_fields set)
                      diff-fields (set/difference grouping-fields source-stream-fields)]
                  (when-not (empty? diff-fields)
                    (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from stream: [" source-stream-id "] of component [" source-component-id "] with non-existent fields: " diff-fields)))))))))))))

(defn acker-inputs [^StormTopology topology]
  (let [bolt-ids (.. topology get_bolts keySet)
        spout-ids (.. topology get_spouts keySet)
        spout-inputs (apply merge
                            (for [id spout-ids]
                              {[id ACKER-INIT-STREAM-ID] ["id"]}
                              ))
        bolt-inputs (apply merge
                           (for [id bolt-ids]
                             {[id ACKER-ACK-STREAM-ID] ["id"]
                              [id ACKER-FAIL-STREAM-ID] ["id"]}
                             ))]
    (merge spout-inputs bolt-inputs)))

(defn add-acker! [storm-conf ^StormTopology ret]
  (let [num-executors (if (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (storm-conf TOPOLOGY-WORKERS) (storm-conf TOPOLOGY-ACKER-EXECUTORS))
        acker-bolt (mk-bolt-spec* (acker-inputs ret)
                                         (new backtype.storm.daemon.acker)
                                         {ACKER-ACK-STREAM-ID (direct-output-fields ["id"])
                                          ACKER-FAIL-STREAM-ID (direct-output-fields ["id"])
                                          }
                                         :p num-executors
                                         :conf {TOPOLOGY-TASKS num-executors
                                                TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]
    (dofor [[_ bolt] (.get_bolts ret)
            :let [common (.get_common bolt)]]
           (do
             (.put_to_streams common ACKER-ACK-STREAM-ID (output-fields ["id" "ack-val"]))
             (.put_to_streams common ACKER-FAIL-STREAM-ID (output-fields ["id"]))
             ))
    (dofor [[_ spout] (.get_spouts ret)
            :let [common (.get_common spout)
                  spout-conf (merge
                               (component-conf spout)
                               {TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]]
      (do
        ;; this set up tick tuples to cause timeouts to be triggered
        (.set_json_conf common (to-json spout-conf))
        (.put_to_streams common ACKER-INIT-STREAM-ID (output-fields ["id" "init-val" "spout-task"]))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-ACK-STREAM-ID)
                        (mk-direct-grouping))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-FAIL-STREAM-ID)
                        (mk-direct-grouping))
        ))
    (.put_to_bolts ret "__acker" acker-bolt)
    ))

(defn add-metric-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common METRICS-STREAM-ID
                     (output-fields ["task-info" "data-points"]))))

(defn add-system-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common SYSTEM-STREAM-ID (output-fields ["event"]))))

(defn metrics-consumer-bolt-specs [storm-conf topology]
  (let [component-ids-that-emit-metrics (cons SYSTEM-COMPONENT-ID (keys (all-components topology)))
        inputs (->> (for [comp-id component-ids-that-emit-metrics]
                      {[comp-id METRICS-STREAM-ID] :shuffle})
                    (into {}))
        
        mk-bolt-spec (fn [class arg p]
                       (mk-bolt-spec*
                        inputs
                        (backtype.storm.metric.MetricsConsumerBolt. class arg)
                        {} :p p :conf {TOPOLOGY-TASKS p}))]
    
    (map
     (fn [component-id register]           
       [component-id (mk-bolt-spec (get register "class")
                                   (get register "argument")
                                   (or (get register "parallelism.hint") 1))])
     
     (metrics-consumer-register-ids storm-conf)
     (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER))))

(defn add-metric-components! [storm-conf ^StormTopology topology]  
  (doseq [[comp-id bolt-spec] (metrics-consumer-bolt-specs storm-conf topology)]
    (.put_to_bolts topology comp-id bolt-spec)))

(defn add-system-components! [conf ^StormTopology topology]
  (let [system-bolt-spec (mk-bolt-spec*
                          {}
                          (SystemBolt.)
                          {SYSTEM-TICK-STREAM-ID (output-fields ["rate_secs"])
                           METRICS-TICK-STREAM-ID (output-fields ["interval"])
                           CREDENTIALS-CHANGED-STREAM-ID (output-fields ["creds"])}
                          :p 0
                          :conf {TOPOLOGY-TASKS 0})]
    (.put_to_bolts topology SYSTEM-COMPONENT-ID system-bolt-spec)))

(defn system-topology! [storm-conf ^StormTopology topology]
  (validate-basic! topology)
  (let [ret (.deepCopy topology)]
    (add-acker! storm-conf ret)
    (add-metric-components! storm-conf ret)    
    (add-system-components! storm-conf ret)
    (add-metric-streams! ret)
    (add-system-streams! ret)
    (validate-structure! ret)
    ret))

(defn num-start-executors [component]
  (parallelism-hint (.get_common component)))

(defn storm-task-info
  "Returns map from task -> component id"
  [^StormTopology user-topology storm-conf]
  (->> (system-topology! storm-conf user-topology)
       all-components
       (map-val (comp #(get % TOPOLOGY-TASKS) component-conf))
       (sort-by first)
       (mapcat (fn [[c num-tasks]] (repeat num-tasks c)))
       (map (fn [id comp] [id comp]) (iterate (comp int inc) (int 1)))
       (into {})))
