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

(ns backtype.storm.stats
  (:import [backtype.storm.generated Nimbus Nimbus$Processor Nimbus$Iface StormTopology ShellComponent
            NotAliveException AlreadyAliveException InvalidTopologyException GlobalStreamId
            ClusterSummary TopologyInfo TopologySummary ExecutorInfo ExecutorSummary ExecutorStats
            ExecutorSpecificStats SpoutStats BoltStats ErrorInfo
            SupervisorSummary BoltAggregateStats SpoutAggregateStats])
  (:use [backtype.storm log util])
  (:use [backtype.storm.daemon [common :only [system-id?]]]))

;;TODO: consider replacing this with some sort of RRD

(defn curr-time-bucket
  [^Integer time-secs ^Integer bucket-size-secs]
  (* bucket-size-secs (unchecked-divide-int time-secs bucket-size-secs)))

(defrecord RollingWindow
  [updater merger extractor bucket-size-secs num-buckets buckets])

(defn rolling-window
  [updater merger extractor bucket-size-secs num-buckets]
  (RollingWindow. updater merger extractor bucket-size-secs num-buckets {}))

(defn update-rolling-window
  ([^RollingWindow rw time-secs & args]
   ;; this is 2.5x faster than using update-in...
   (let [time-bucket (curr-time-bucket time-secs (:bucket-size-secs rw))
         buckets (:buckets rw)
         curr (get buckets time-bucket)
         curr (apply (:updater rw) curr args)]
     (assoc rw :buckets (assoc buckets time-bucket curr)))))

(defn value-rolling-window
  [^RollingWindow rw]
  ((:extractor rw)
   (let [values (vals (:buckets rw))]
     (apply (:merger rw) values))))

(defn cleanup-rolling-window
  [^RollingWindow rw]
  (let [buckets (:buckets rw)
        cutoff (- (current-time-secs)
                  (* (:num-buckets rw)
                     (:bucket-size-secs rw)))
        to-remove (filter #(< % cutoff) (keys buckets))
        buckets (apply dissoc buckets to-remove)]
    (assoc rw :buckets buckets)))

(defn rolling-window-size
  [^RollingWindow rw]
  (* (:bucket-size-secs rw) (:num-buckets rw)))

(defrecord RollingWindowSet [updater extractor windows all-time])

(defn rolling-window-set [updater merger extractor num-buckets & bucket-sizes]
  (RollingWindowSet. updater extractor (dofor [s bucket-sizes] (rolling-window updater merger extractor s num-buckets)) nil)
  )

(defn update-rolling-window-set
  ([^RollingWindowSet rws & args]
   (let [now (current-time-secs)
         new-windows (dofor [w (:windows rws)]
                            (apply update-rolling-window w now args))]
     (assoc rws
       :windows new-windows
       :all-time (apply (:updater rws) (:all-time rws) args)))))

(defn cleanup-rolling-window-set
  ([^RollingWindowSet rws]
   (let [windows (:windows rws)]
     (assoc rws :windows (map cleanup-rolling-window windows)))))

(defn value-rolling-window-set
  [^RollingWindowSet rws]
  (merge
    (into {}
          (for [w (:windows rws)]
            {(rolling-window-size w) (value-rolling-window w)}
            ))
    {:all-time ((:extractor rws) (:all-time rws))}))

(defn- incr-val
  ([amap key]
   (incr-val amap key 1))
  ([amap key amt]
   (let [val (get amap key (long 0))]
     (assoc amap key (+ val amt)))))

(defn- update-avg
  [curr val]
  (if curr
    [(+ (first curr) val) (inc (second curr))]
    [val (long 1)]))

(defn- merge-avg
  [& avg]
  [(apply + (map first avg))
   (apply + (map second avg))
   ])

(defn- extract-avg
  [pair]
  (double (/ (first pair) (second pair))))

(defn- update-keyed-avg
  [amap key val]
  (assoc amap key (update-avg (get amap key) val)))

(defn- merge-keyed-avg [& vals]
  (apply merge-with merge-avg vals))

(defn- extract-keyed-avg [vals]
  (map-val extract-avg vals))

(defn- counter-extract [v]
  (if v v {}))

(defn keyed-counter-rolling-window-set
  [num-buckets & bucket-sizes]
  (apply rolling-window-set incr-val (partial merge-with +) counter-extract num-buckets bucket-sizes))

(defn avg-rolling-window-set
  [num-buckets & bucket-sizes]
  (apply rolling-window-set update-avg merge-avg extract-avg num-buckets bucket-sizes))

(defn keyed-avg-rolling-window-set
  [num-buckets & bucket-sizes]
  (apply rolling-window-set update-keyed-avg merge-keyed-avg extract-keyed-avg num-buckets bucket-sizes))

;; (defn choose-bucket [val buckets]
;;   (let [ret (find-first #(<= val %) buckets)]
;;     (if ret
;;       ret
;;       (* 10 (first buckets)))
;;     ))

;; ;; buckets must be between 1 and 9
;; (defn to-proportional-bucket
;;   "Maps to a bucket in the values order of magnitude. So if buckets are [1 2 5],
;;    3 -> 5
;;    7 -> 10
;;    1234 -> 2000
;;    etc."
;;   [val buckets]
;;   (cond (= 0 val) 0
;;         (between? val 1 9) (choose-bucket val buckets)
;;         :else (* 10 (to-proportional-bucket (ceil (/ val 10))
;;                                             buckets))))

(def COMMON-FIELDS [:emitted :transferred])
(defrecord CommonStats [emitted transferred rate])

(def BOLT-FIELDS [:acked :failed :process-latencies :executed :execute-latencies])
;;acked and failed count individual tuples
(defrecord BoltExecutorStats [common acked failed process-latencies executed execute-latencies])

(def SPOUT-FIELDS [:acked :failed :complete-latencies])
;;acked and failed count tuple completion
(defrecord SpoutExecutorStats [common acked failed complete-latencies])

(def NUM-STAT-BUCKETS 20)
;; 10 minutes, 3 hours, 1 day
(def STAT-BUCKETS [30 540 4320])

(defn- mk-common-stats
  [rate]
  (CommonStats.
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    rate))

(defn mk-bolt-stats
  [rate]
  (BoltExecutorStats.
    (mk-common-stats rate)
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))))

(defn mk-spout-stats
  [rate]
  (SpoutExecutorStats.
    (mk-common-stats rate)
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
    (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))))

(defmacro update-executor-stat!
  [stats path & args]
  (let [path (collectify path)]
    `(swap! (-> ~stats ~@path) update-rolling-window-set ~@args)))

(defmacro stats-rate
  [stats]
  `(-> ~stats :common :rate))

(defn emitted-tuple!
  [stats stream]
  (update-executor-stat! stats [:common :emitted] stream (stats-rate stats)))

(defn transferred-tuples!
  [stats stream amt]
  (update-executor-stat! stats [:common :transferred] stream (* (stats-rate stats) amt)))

(defn bolt-execute-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :executed key (stats-rate stats))
    (update-executor-stat! stats :execute-latencies key latency-ms)))

(defn bolt-acked-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :acked key (stats-rate stats))
    (update-executor-stat! stats :process-latencies key latency-ms)))

(defn bolt-failed-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :failed key (stats-rate stats))))

(defn spout-acked-tuple!
  [^SpoutExecutorStats stats stream latency-ms]
  (update-executor-stat! stats :acked stream (stats-rate stats))
  (update-executor-stat! stats :complete-latencies stream latency-ms))

(defn spout-failed-tuple!
  [^SpoutExecutorStats stats stream latency-ms]
  (update-executor-stat! stats :failed stream (stats-rate stats))
  )

(defn- cleanup-stat! [stat]
  (swap! stat cleanup-rolling-window-set))

(defn- cleanup-common-stats!
  [^CommonStats stats]
  (doseq [f COMMON-FIELDS]
    (cleanup-stat! (f stats))))

(defn cleanup-bolt-stats!
  [^BoltExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f BOLT-FIELDS]
    (cleanup-stat! (f stats))))

(defn cleanup-spout-stats!
  [^SpoutExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f SPOUT-FIELDS]
    (cleanup-stat! (f stats))))

(defn- value-stats
  [stats fields]
  (into {} (dofor [f fields]
                  [f (value-rolling-window-set @(f stats))])))

(defn- value-common-stats
  [^CommonStats stats]
  (merge
    (value-stats stats COMMON-FIELDS)
    {:rate (:rate stats)}))

(defn value-bolt-stats!
  [^BoltExecutorStats stats]
  (cleanup-bolt-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats BOLT-FIELDS)
         {:type :bolt}))

(defn value-spout-stats!
  [^SpoutExecutorStats stats]
  (cleanup-spout-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats SPOUT-FIELDS)
         {:type :spout}))

(defmulti render-stats! class-selector)

(defmethod render-stats! SpoutExecutorStats
  [stats]
  (value-spout-stats! stats))

(defmethod render-stats! BoltExecutorStats
  [stats]
  (value-bolt-stats! stats))

(defmulti thriftify-specific-stats :type)

(defn window-set-converter
  ([stats key-fn]
   ;; make the first key a string,
   (into {}
         (for [[k v] stats]
           [(str k)
            (into {} (for [[k2 v2] v]
                       [(key-fn k2) v2]))])))
  ([stats]
   (window-set-converter stats identity)))

(defn to-global-stream-id
  [[component stream]]
  (GlobalStreamId. component stream))

(defmethod thriftify-specific-stats :bolt
  [stats]
  (ExecutorSpecificStats/bolt
    (BoltStats.
      (window-set-converter (:acked stats) to-global-stream-id)
      (window-set-converter (:failed stats) to-global-stream-id)
      (window-set-converter (:process-latencies stats) to-global-stream-id)
      (window-set-converter (:executed stats) to-global-stream-id)
      (window-set-converter (:execute-latencies stats) to-global-stream-id))))

(defmethod thriftify-specific-stats :spout
  [stats]
  (ExecutorSpecificStats/spout
    (SpoutStats. (window-set-converter (:acked stats))
                 (window-set-converter (:failed stats))
                 (window-set-converter (:complete-latencies stats)))))

(defn thriftify-executor-stats
  [stats]
  (let [specific-stats (thriftify-specific-stats stats)]
    (ExecutorStats. (window-set-converter (:emitted stats))
                    (window-set-converter (:transferred stats))
                    specific-stats)))

(defn- to-tasks [^ExecutorInfo e]
  (let [start (.get_task_start e)
        end (.get_task_end e)]
    (range start (inc end))))

(defn sum-tasks [executors]
  (reduce + (->> executors
                 (map #(.get_executor_info ^ExecutorSummary %))
                 (map to-tasks)
                 (map count))))

(defn- agg-bolt-lat-and-count
  "Aggregates number executed and process & execute latencies across all
  streams."
  ;; Return statkey->number for :exec-avg :proc-avg :num-executed
  [stream-id->exec-avg stream-id->proc-avg stream-id->num-executed]
  {:pre (apply = (map #(set (keys %))
                      [stream-id->exec-avg
                       stream-id->proc-avg
                       stream-id->num-executed]))}
  (letfn [(weight-avg [[id avg]] (let [num-e (stream-id->num-executed id)]
                                   (if (and avg num-e)
                                     (* avg (stream-id->num-executed id))
                                     0)))]
    {:executeLatencyTotal (reduce + (map weight-avg stream-id->exec-avg))
     :processLatencyTotal (reduce + (map weight-avg stream-id->proc-avg))
     :executed (reduce + (vals stream-id->num-executed))}))

(defn- agg-spout-lat-and-count
  "Aggregates number acked and complete latencies across all streams."
  [stream-id->comp-avg stream-id->num-acked]
  {:pre (apply = (map #(set (keys %))
                      [stream-id->comp-avg
                       stream-id->num-acked]))}
  (letfn [(weight-avg [[id avg]] (* avg (stream-id->num-acked id)))]
    {:compLatWgtAvg (reduce + (map weight-avg stream-id->comp-avg))
     :acked (reduce + (vals stream-id->num-acked))}))

(defn add-pairs
  ([] [0 0])
  ([[a1 a2] [b1 b2]]
   [(+ a1 b1) (+ a2 b2)]))

(defn mk-include-sys-fn
  [include-sys?]
  (if include-sys?
    (fn [_] true)
    (fn [stream] (and (string? stream) (not (system-id? stream))))))

(defn mk-include-sys-filter
  "Returns a function that includes or excludes map entries whose keys are
  system ids."
  [include-sys?]
  (if include-sys?
    identity
    (partial filter-key (mk-include-sys-fn false))))

(defn extract-bolt-stats-from-exec-stats
  "Pulls out the needed bolt's stats from the thrift object and returns a
  mapping from bolt id to its stats."
  [{id :id
    num-tasks :num-tasks
    statk->w->sid->num :stats
    uptime :uptime :as m}
   ^StormTopology topology
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components (mk-include-sys-filter include-sys?)]
    {id
     (merge
       (agg-bolt-lat-and-count (-> statk->w->sid->num
                                   :execute-latencies
                                   str-key
                                   (get window))
                               (-> statk->w->sid->num
                                   :process-latencies
                                   str-key
                                   (get window))
                               (-> statk->w->sid->num
                                   :executed
                                   str-key
                                   (get window)))
       {:numExecutors 1
        :numTasks num-tasks
        :emitted (-> statk->w->sid->num
                     :emitted
                     str-key
                     (get window)
                     handle-sys-components
                     vals
                     sum)
        :transferred (-> statk->w->sid->num
                         :transferred
                         str-key
                         (get window)
                         handle-sys-components
                         vals
                         sum)
        :capacity (->>
                    ;; For each stream, create weighted averages and counts.
                    (merge-with (fn weighted-avg+count-fn
                                  [avg cnt]
                                  [(* avg cnt) cnt])
                                (get (:execute-latencies statk->w->sid->num)
                                     600)
                                (get (:executed statk->w->sid->num) 600))
                    vals ;; Ignore the stream ids.
                    (reduce add-pairs
                            [0. 0]) ;; Combine weighted averages and counts.
                    ((fn [[weighted-avg cnt]]
                      (div weighted-avg (* 1000 (min uptime 600))))))
        :acked (-> statk->w->sid->num
                   :acked
                   str-key
                   (get window)
                   vals
                   sum)
        :failed (-> statk->w->sid->num
                    :failed
                    str-key
                    (get window)
                    vals
                    sum)})}))
  
(defn extract-spout-stats-from-exec-stats
  "Pulls out the needed spout's stats from the thrift object and returns a
  mapping from spout id to its stats."
  [{id :id
    num-tasks :num-tasks
    statk->w->sid->num :stats}
   ^StormTopology topology
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components (mk-include-sys-filter include-sys?)]
    {id
     (merge
       (agg-spout-lat-and-count (-> statk->w->sid->num
                                    :complete-latencies
                                    str-key
                                    (get window))
                                (-> statk->w->sid->num
                                    :acked
                                    str-key
                                    (get window)))
       {:numExecutors 1
        :numTasks num-tasks
        :emitted (-> statk->w->sid->num
                     :emitted
                     str-key
                     (get window)
                     handle-sys-components
                     vals
                     sum)
        :transferred (-> statk->w->sid->num
                         :transferred
                         str-key
                         (get window)
                         handle-sys-components
                         vals
                         sum)
        :failed (-> statk->w->sid->num
                    :failed
                    str-key
                    (get window)
                    vals
                    sum)})}))

(defn- merge-bolt-executor-stats
  "Merges all bolt stats from one executor with the given accumulated stats."
  [acc-bolt-stats bolt-stats]
  {:numExecutors (inc (or (:numExecutors acc-bolt-stats) 0))
   :numTasks (+ (or (:numTasks acc-bolt-stats) 0) (:numTasks bolt-stats))
   :emitted (+ (or (:emitted acc-bolt-stats) 0) (:emitted bolt-stats))
   :transferred (+ (or (:transferred acc-bolt-stats) 0)
                   (:transferred bolt-stats))
   :capacity (max (or (:capacity acc-bolt-stats) 0) (:capacity bolt-stats))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :executeLatencyTotal (+ (or (:executeLatencyTotal acc-bolt-stats) 0)
                           (:executeLatencyTotal bolt-stats))
   :processLatencyTotal (+ (or (:processLatencyTotal acc-bolt-stats) 0)
                           (:processLatencyTotal bolt-stats))
   :executed (+ (or (:executed acc-bolt-stats) 0) (:executed bolt-stats))
   :acked (+ (or (:acked acc-bolt-stats) 0) (:acked bolt-stats))
   :failed (+ (or (:failed acc-bolt-stats) 0) (:failed bolt-stats))})

(defn- merge-spout-executor-stats
  "Merges all spout stats from one executor with the given accumulated stats."
  [acc-spout-stats spout-stats]
  {:numExecutors (inc (or (:numExecutors acc-spout-stats) 0))
   :numTasks (+ (or (:numTasks acc-spout-stats) 0) (:numTasks spout-stats))
   :emitted (+ (or (:emitted acc-spout-stats) 0) (:emitted spout-stats))
   :transferred (+ (or (:transferred acc-spout-stats) 0) (:transferred spout-stats))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :compLatWgtAvg (+ (or (:compLatWgtAvg acc-spout-stats) 0)
                     (:compLatWgtAvg spout-stats))
   :acked (+ (or (:acked acc-spout-stats) 0) (:acked spout-stats))
   :failed (+ (or (:failed acc-spout-stats) 0) (:failed spout-stats))})

(defn aggregate-count-streams
  [stats]
  (->> stats
       (map-val #(reduce + (vals %)))))

(defn swap-map-order
  "{:a {:A 3, :B 5}, :b {:A 1, :B 2}}
    -> {:A {:b 1, :a 3}, :B {:b 2, :a 5}}"
  [m]
  (apply merge-with
         merge
         (map (fn [[k v]]
                (into {}
                      (for [[k2 v2] v]
                        [k2 {k v2}])))
              m)))

(defn- agg-executor-stats-common
  "A helper function parameterized for the component type that does the common
  work to aggregate stats of one executor with the given map."
  [^StormTopology topology
   window
   include-sys?
   {:keys [workers-set
           bolt-id->stats
           spout-id->stats
           window->emitted
           window->transferred
           window->comp-lat-wgt-avg
           window->acked
           window->failed] :as acc-stats}
   {:keys [id stats] :as new-data}
   extract-fn
   merge-fn
   comp-key]
  (let [cid->statk->num (extract-fn new-data topology window include-sys?)
        {w->compLatWgtAvg :compLatWgtAvg
         w->acked :acked}
          (if (:complete-latencies stats)
            (swap-map-order
              (into {}
                    (for [w (keys (:acked stats))]
                         [w (agg-spout-lat-and-count
                              ((:complete-latencies stats) w)
                              ((:acked stats) w))])))
            {:compLatWgtAvg nil
             :acks (aggregate-count-streams (:acked stats))})
        handle-sys-components (mk-include-sys-filter include-sys?)]
    (assoc {:workers-set (conj workers-set
                               [(:host new-data) (:port new-data)])
            :bolt-id->stats bolt-id->stats
            :spout-id->stats spout-id->stats
            :window->emitted (->> (:emitted stats)
                                  (map-val handle-sys-components)
                                  aggregate-count-streams
                                  (merge-with + window->emitted))
            :window->transferred (->> (:transferred stats)
                                      (map-val handle-sys-components)
                                      aggregate-count-streams
                                      (merge-with + window->transferred))
            :window->comp-lat-wgt-avg (merge-with +
                                                  window->comp-lat-wgt-avg
                                                  w->compLatWgtAvg)
            :window->acked (if (= :spout (:type stats))
                             (merge-with + window->acked w->acked)
                             window->acked)
            :window->failed (if (= :spout (:type stats))
                              (->> (:failed stats)
                                   aggregate-count-streams
                                   (merge-with + window->failed))
                              window->failed)}
           comp-key (merge-with merge-fn
                                (acc-stats comp-key)
                                cid->statk->num))))

(defmulti agg-executor-stats 
  "Combines the aggregate stats of one executor with the given map, selecting
  the appropriate window and including system components as specified."
  (fn dispatch-fn [& args] (:type (:stats (last args)))))

(defmethod agg-executor-stats :bolt
  [topology window include-sys? acc-stats new-data]
  (agg-executor-stats-common topology
                             window
                             include-sys?
                             acc-stats
                             new-data
                             extract-bolt-stats-from-exec-stats
                             merge-bolt-executor-stats
                             :bolt-id->stats))

(defmethod agg-executor-stats :spout
  [topology window include-sys? acc-stats new-data]
  (agg-executor-stats-common topology
                             window
                             include-sys?
                             acc-stats
                             new-data
                             extract-spout-stats-from-exec-stats
                             merge-spout-executor-stats
                             :spout-id->stats))

(defn get-last-error
  [storm-cluster-state storm-id component-id]
  (if-let [e (.last-error storm-cluster-state storm-id component-id)]
    (ErrorInfo. (:error e) (:time-secs e))))

(defn agg-executors-stats
  "Aggregate various statistics for the given executors from the given
  heartbeats."
  [exec->node+port
   task->component
   beats
   ^StormTopology
   topology
   window
   include-sys?
   last-err-fn]
  (let [data (for [[[start end :as executor] [node port]] exec->node+port
                   :let [beat (beats executor)
                         id (task->component start)]
                   :when (and (:type (:stats beat))
                              (or include-sys? (not (system-id? id))))]
               {:id id
                :num-tasks (count (range start (inc end)))
                :host node
                :port port
                :uptime (:uptime beat)
                :stats (:stats beat)})
        reducer-fn (partial agg-executor-stats
                            topology
                            window
                            include-sys?)
        acc-data (reduce reducer-fn
                         {:workers-set #{}
                          :bolt-id->stats {} 
                          :spout-id->stats {}
                          :window->emitted {}
                          :window->transferred {}
                          :window->comp-lat-wgt-avg {}
                          :window->acked {}
                          :window->failed {}}
                          data)
        nil-or-zero? #(or (nil? %) (zero? %))]
    {:num-tasks (count task->component)
     :num-workers (count (:workers-set acc-data))
     :num-executors (count exec->node+port)
     :bolt-id->stats
       (into {} (for [[id m] (:bolt-id->stats acc-data)
                      :let [executed (:executed m)]]
                       [id (-> m
                               (assoc :executeLatency
                                      (if-not (nil-or-zero? executed)
                                        (div (or (:executeLatencyTotal m) 0)
                                             (:executed m))
                                        0)
                                      :processLatency
                                      (if-not (nil-or-zero? executed)
                                        (div (or (:processLatencyTotal m) 0)
                                             (:executed m))
                                        0))
                               (dissoc :executeLatencyTotal
                                       :processLatencyTotal)
                               (assoc :lastError (last-err-fn id)))]))
     :spout-id->stats
       (into {} (for [[id m] (:spout-id->stats acc-data)
                      :let [acked (:acked m)]]
                      [id (-> m
                              (assoc :completeLatency
                                     (if-not (nil-or-zero? acked)
                                       (div (:compLatWgtAvg m)
                                            (:acked m))
                                       0))
                              (dissoc :compLatWgtAvg)
                              (assoc :lastError (last-err-fn id)))]))
     :window->emitted (map-key str (:window->emitted acc-data))
     :window->transferred (map-key str (:window->transferred acc-data))
     :window->complete-latency
       (into {} (for [[window wgt-avg] (:window->comp-lat-wgt-avg acc-data)
                      :let [acked ((:window->acked acc-data) window)]
                      :when (not (nil-or-zero? acked))]
                  [(str window) (div wgt-avg acked)]))
     :window->acked (map-key str (:window->acked acc-data))
     :window->failed (map-key str (:window->failed acc-data))}))

(defn thriftify-spout-agg-stats
  [id m]
  (doto (SpoutAggregateStats. id)
    (.set_num_executors (:numExecutors m))
    (.set_num_tasks (:numTasks m))
    (.set_num_emitted (:emitted m))
    (.set_num_transferred (:transferred m))
    (.set_num_acked (:acked m))
    (.set_num_failed (:failed m))
    (.set_last_error (:lastError m))
    (.set_complete_latency (:completeLatency m))))

(defn thriftify-bolt-agg-stats
  [id m]
  (doto (BoltAggregateStats. id)
    (.set_num_executors (:numExecutors m))
    (.set_num_tasks (:numTasks m))
    (.set_num_emitted (:emitted m))
    (.set_num_transferred (:transferred m))
    (.set_num_acked (:acked m))
    (.set_num_failed (:failed m))
    (.set_last_error (:lastError m))
    (.set_execute_latency (:executeLatency m))
    (.set_process_latency (:processLatency m))
    (.set_num_executed (:executed m))
    (.set_capacity (:capacity m))))
