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
            ClusterSummary TopologyInfo TopologySummary ExecutorInfo ExecutorSummary ExecutorStats ExecutorSpecificStats
            SpoutAggregateStats SpoutStats BoltAggregateStats BoltStats ErrorInfo SupervisorSummary])
  (:use [backtype.storm util log])
  (:use [backtype.storm.daemon [common :only [system-id?]]])
  (:use [clojure.math.numeric-tower :only [ceil]]))

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
(defmulti clojurify-specific-stats class-selector)

(defn window-set-converter
  ([stats key-fn first-key-fun]
    (into {}
      (for [[k v] stats]
        ;apply the first-key-fun only to first key.
        [(first-key-fun k)
         (into {} (for [[k2 v2] v]
                    [(key-fn k2) v2]))])))
  ([stats first-key-fun]
    (window-set-converter stats identity first-key-fun)))

(defn to-global-stream-id
  [[component stream]]
  (GlobalStreamId. component stream))

(defn from-global-stream-id [global-stream-id]
  [(.get_componentId global-stream-id) (.get_streamId global-stream-id)])

(defmethod clojurify-specific-stats BoltStats [^BoltStats stats]
  [(window-set-converter (.get_acked stats) from-global-stream-id symbol)
   (window-set-converter (.get_failed stats) from-global-stream-id symbol)
   (window-set-converter (.get_process_ms_avg stats) from-global-stream-id symbol)
   (window-set-converter (.get_executed stats) from-global-stream-id symbol)
   (window-set-converter (.get_execute_ms_avg stats) from-global-stream-id symbol)])

(defmethod clojurify-specific-stats SpoutStats [^SpoutStats stats]
  [(window-set-converter (.get_acked stats) symbol)
   (window-set-converter (.get_failed stats) symbol)
   (window-set-converter (.get_complete_ms_avg stats) symbol)])


(defn clojurify-executor-stats
  [^ExecutorStats stats]
  (let [ specific-stats (.get_specific stats)
         is_bolt? (.is_set_bolt specific-stats)
         specific-stats (if is_bolt? (.get_bolt specific-stats) (.get_spout specific-stats))
         specific-stats (clojurify-specific-stats specific-stats)
         common-stats (CommonStats. (window-set-converter (.get_emitted stats) symbol) (window-set-converter (.get_transferred stats) symbol) (.get_rate stats))]
    (if is_bolt?
      ; worker heart beat does not store the BoltExecutorStats or SpoutExecutorStats , instead it stores the result returned by render-stats!
      ; which flattens the BoltExecutorStats/SpoutExecutorStats by extracting values from all atoms and merging all values inside :common to top
      ;level map we are pretty much doing the same here.
      (dissoc (merge common-stats {:type :bolt}  (apply ->BoltExecutorStats (into [nil] specific-stats))) :common)
      (dissoc (merge common-stats {:type :spout} (apply ->SpoutExecutorStats (into [nil] specific-stats))) :common)
      )))

(defmethod thriftify-specific-stats :bolt
  [stats]
  (ExecutorSpecificStats/bolt
    (BoltStats.
      (window-set-converter (:acked stats) to-global-stream-id str)
      (window-set-converter (:failed stats) to-global-stream-id str)
      (window-set-converter (:process-latencies stats) to-global-stream-id str)
      (window-set-converter (:executed stats) to-global-stream-id str)
      (window-set-converter (:execute-latencies stats) to-global-stream-id str))))

(defmethod thriftify-specific-stats :spout
  [stats]
  (ExecutorSpecificStats/spout
    (SpoutStats. (window-set-converter (:acked stats) str)
      (window-set-converter (:failed stats) str)
      (window-set-converter (:complete-latencies stats) str))))

(defn thriftify-executor-stats
  [stats]
  (let [specific-stats (thriftify-specific-stats stats)
        rate (:rate stats)]
    (ExecutorStats. (window-set-converter (:emitted stats) str)
      (window-set-converter (:transferred stats) str)
      specific-stats
      rate)))

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
        :capacity (if (and uptime (pos? uptime))
                    (->>
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
                        (div weighted-avg (* 1000 (min uptime 600)))))))
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
   :numTasks (+ (or (:numTasks acc-bolt-stats) 0)
                (or (:numTasks bolt-stats) 0))
   :emitted (+ (or (:emitted acc-bolt-stats) 0)
               (or (:emitted bolt-stats) 0))
   :transferred (+ (or (:transferred acc-bolt-stats) 0)
                   (or (:transferred bolt-stats) 0))
   :capacity (max (or (:capacity acc-bolt-stats) 0)
                  (or (:capacity bolt-stats) 0))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :executeLatencyTotal (+ (or (:executeLatencyTotal acc-bolt-stats) 0)
                           (or (:executeLatencyTotal bolt-stats) 0))
   :processLatencyTotal (+ (or (:processLatencyTotal acc-bolt-stats) 0)
                           (or (:processLatencyTotal bolt-stats) 0))
   :executed (+ (or (:executed acc-bolt-stats) 0)
                (or (:executed bolt-stats) 0))
   :acked (+ (or (:acked acc-bolt-stats) 0)
             (or (:acked bolt-stats) 0))
   :failed (+ (or (:failed acc-bolt-stats) 0)
              (or (:failed bolt-stats) 0))})

(defn- merge-spout-executor-stats
  "Merges all spout stats from one executor with the given accumulated stats."
  [acc-spout-stats spout-stats]
  {:numExecutors (inc (or (:numExecutors acc-spout-stats) 0))
   :numTasks (+ (or (:numTasks acc-spout-stats) 0)
                (or (:numTasks spout-stats) 0))
   :emitted (+ (or (:emitted acc-spout-stats) 0)
               (or (:emitted spout-stats) 0))
   :transferred (+ (or (:transferred acc-spout-stats) 0)
                   (or (:transferred spout-stats) 0))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :compLatWgtAvg (+ (or (:compLatWgtAvg acc-spout-stats) 0)
                     (or (:compLatWgtAvg spout-stats) 0))
   :acked (+ (or (:acked acc-spout-stats) 0)
             (or (:acked spout-stats) 0))
   :failed (+ (or (:failed acc-spout-stats) 0)
              (or (:failed spout-stats) 0))})

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
  (fn dispatch-fn [& args] (:type (last args))))

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

(defn component-type
  "Returns the component type (either :bolt or :spout) for a given
  topology and component id. Returns nil if not found."
  [^StormTopology topology id]
  (let [bolts (.get_bolts topology)
        spouts (.get_spouts topology)]
    (cond
      (.containsKey bolts id) :bolt
      (.containsKey spouts id) :spout)))

(defn agg-executors-stats
  "Aggregate various statistics for the given executors from the given
  heartbeats."
  [exec->node+port
   task->component
   beats
   ^StormTopology topology
   window
   include-sys?
   last-err-fn]
  (let [data (for [[[start end :as executor] [node port]] exec->node+port
                   :let [beat (beats executor)
                         id (task->component start)]
                   :when (or include-sys? (not (system-id? id)))]
               {:id id
                :num-tasks (count (range start (inc end)))
                :host node
                :port port
                :uptime (:uptime beat)
                :stats (:stats beat)
                :type (component-type topology id)})
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

(defn expand-averages
  [avg counts]
  (let [avg (clojurify-structure avg)
        counts (clojurify-structure counts)]
    (into {}
          (for [[slice streams] counts]
            [slice
             (into {}
                   (for [[stream c] streams]
                     [stream
                      [(* c (get-in avg [slice stream]))
                       c]]
                     ))]))))

(defn expand-averages-seq
  [average-seq counts-seq]
  (->> (map vector average-seq counts-seq)
       (map #(apply expand-averages %))
       (apply merge-with (fn [s1 s2] (merge-with add-pairs s1 s2)))))

(defn- val-avg
  [[t c]]
  (if (= t 0) 0
    (double (/ t c))))

(defn aggregate-averages
  [average-seq counts-seq]
  (->> (expand-averages-seq average-seq counts-seq)
       (map-val
         (fn [s]
           (map-val val-avg s)))))

(defn aggregate-avg-streams
  [avg counts]
  (let [expanded (expand-averages avg counts)]
    (->> expanded
         (map-val #(reduce add-pairs (vals %)))
         (map-val val-avg))))

(defn pre-process
  [stream-summary include-sys?]
  (let [filter-fn (mk-include-sys-fn include-sys?)
        emitted (:emitted stream-summary)
        emitted (into {} (for [[window stat] emitted]
                           {window (filter-key filter-fn stat)}))
        transferred (:transferred stream-summary)
        transferred (into {} (for [[window stat] transferred]
                               {window (filter-key filter-fn stat)}))
        stream-summary (-> stream-summary (dissoc :emitted) (assoc :emitted emitted))
        stream-summary (-> stream-summary (dissoc :transferred) (assoc :transferred transferred))]
    stream-summary))

(defn aggregate-counts
  [counts-seq]
  (->> counts-seq
       (map clojurify-structure)
       (apply merge-with
              (fn [s1 s2]
                (merge-with + s1 s2)))))

(defn aggregate-common-stats
  [stats-seq]
  {:emitted (aggregate-counts (map #(.get_emitted ^ExecutorStats %) stats-seq))
   :transferred (aggregate-counts (map #(.get_transferred ^ExecutorStats %) stats-seq))})

(defn aggregate-bolt-stats
  [stats-seq include-sys?]
  (let [stats-seq (collectify stats-seq)]
    (merge (pre-process (aggregate-common-stats stats-seq) include-sys?)
           {:acked
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_failed)
                                   stats-seq))
            :executed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_executed)
                                   stats-seq))
            :process-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_bolt get_process_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_bolt get_acked)
                                     stats-seq))
            :execute-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_bolt get_execute_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_bolt get_executed)
                                     stats-seq))})))

(defn aggregate-spout-stats
  [stats-seq include-sys?]
  (let [stats-seq (collectify stats-seq)]
    (merge (pre-process (aggregate-common-stats stats-seq) include-sys?)
           {:acked
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_spout get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_spout get_failed)
                                   stats-seq))
            :complete-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_spout get_complete_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_spout get_acked)
                                     stats-seq))})))

(defn get-filled-stats
  [summs]
  (->> summs
       (map #(.get_stats ^ExecutorSummary %))
       (filter not-nil?)))

(defn aggregate-spout-streams
  [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :complete-latencies (aggregate-avg-streams (:complete-latencies stats)
                                              (:acked stats))})

(defn spout-streams-stats
  [summs include-sys?]
  (let [stats-seq (get-filled-stats summs)]
    (aggregate-spout-streams
      (aggregate-spout-stats
        stats-seq include-sys?))))

(defn aggregate-bolt-streams
  [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :process-latencies (aggregate-avg-streams (:process-latencies stats)
                                             (:acked stats))
   :executed (aggregate-count-streams (:executed stats))
   :execute-latencies (aggregate-avg-streams (:execute-latencies stats)
                                             (:executed stats))})

(defn compute-executor-capacity
  [^ExecutorSummary e]
  (let [stats (.get_stats e)
        stats (if stats
                (-> stats
                    (aggregate-bolt-stats true)
                    (aggregate-bolt-streams)
                    swap-map-order
                    (get "600")))
        uptime (nil-to-zero (.get_uptime_secs e))
        window (if (< uptime 600) uptime 600)
        executed (-> stats :executed nil-to-zero)
        latency (-> stats :execute-latencies nil-to-zero)]
    (if (> window 0)
      (div (* executed latency) (* 1000 window)))))

(defn bolt-streams-stats
  [summs include-sys?]
  (let [stats-seq (get-filled-stats summs)]
    (aggregate-bolt-streams
      (aggregate-bolt-stats
        stats-seq include-sys?))))

(defn total-aggregate-stats
  [spout-summs bolt-summs include-sys?]
  (let [spout-stats (get-filled-stats spout-summs)
        bolt-stats (get-filled-stats bolt-summs)
        agg-spout-stats (-> spout-stats
                            (aggregate-spout-stats include-sys?)
                            aggregate-spout-streams)
        agg-bolt-stats (-> bolt-stats
                           (aggregate-bolt-stats include-sys?)
                           aggregate-bolt-streams)]
    (merge-with
      (fn [s1 s2]
        (merge-with + s1 s2))
      (select-keys
        agg-bolt-stats
        ;; Include only keys that will be used.  We want to count acked and
        ;; failed only for the "tuple trees," so we do not include those keys
        ;; from the bolt executors.
        [:emitted :transferred])
      agg-spout-stats)))

(defn error-subset
  [error-str]
  (apply str (take 200 error-str)))

(defn most-recent-error
  [errors-list]
  (let [error (->> errors-list
                   (sort-by #(.get_error_time_secs ^ErrorInfo %))
                   reverse
                   first)]
    (if error
      (error-subset (.get_error ^ErrorInfo error))
      "")))

(defn float-str [n]
  (if n
    (format "%.3f" (float n))
    "0"))

(defn compute-bolt-capacity
  [executors]
  (->> executors
       (map compute-executor-capacity)
       (map nil-to-zero)
       (apply max)))

(defn spout-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-spout-streams
                     (aggregate-spout-stats
                      stats-seq include-sys?))]]
    {"spoutId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "transferred" (get-in stats [:transferred window])
     "completeLatency" (float-str (get-in stats [:complete-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "lastError" (most-recent-error (get errors id))}))

(defn bolt-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-bolt-streams
                     (aggregate-bolt-stats
                      stats-seq include-sys?))]]
    {"boltId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "transferred" (get-in stats [:transferred window])
     "capacity" (float-str (nil-to-zero (compute-bolt-capacity summs)))
     "executeLatency" (float-str (get-in stats [:execute-latencies window]))
     "executed" (get-in stats [:executed window])
     "processLatency" (float-str (get-in stats [:process-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "lastError" (most-recent-error (get errors id))}))

