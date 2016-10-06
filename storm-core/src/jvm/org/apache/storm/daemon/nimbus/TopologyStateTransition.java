/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.daemon.nimbus;

import java.util.Collections;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyActionOptions;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.utils.Utils;

/**
 * A transition from one state to another
 */
public interface TopologyStateTransition {
    //[TopologyActions/KILL wait-amt]
    //[TopologyActions/REBALANCE wait-amt num-workers executor-overrides]
    public static StormBase make(TopologyStatus status) {
        StormBase ret = new StormBase();
        ret.set_status(status);
        //The following are required for backwards compatibility with clojure code
        ret.set_component_executors(Collections.emptyMap());
        ret.set_component_debug(Collections.emptyMap());
        return ret;
    }
    //TODO make the exception correct
    public StormBase transition(Object argument, Nimbus nimbus, String topoId, StormBase base) throws Exception;
    
    public static final TopologyStateTransition NOOP = (arg, nimbus, topoId, base) -> null;
    public static final TopologyStateTransition INACTIVE = (arg, nimbus, topoId, base) -> make(TopologyStatus.INACTIVE);
    public static final TopologyStateTransition ACTIVE = (arg, nimbus, topoId, base) -> make(TopologyStatus.ACTIVE);
    public static final TopologyStateTransition KILL = (killTime, nimbus, topoId, base) -> {
        //TODO replace the killTIme with an actual class (possibly just the original KillOpts)
        int delay = 0;
        if (killTime != null) {
            delay = ((Number)killTime).intValue();
        } else {
            delay = Utils.getInt(Nimbus.readTopoConf(nimbus.getConf(), topoId, nimbus.getBlobStore()).get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        }
        nimbus.delayEvent(topoId, delay, TopologyActions.REMOVE, null);
        StormBase sb = new StormBase();
        sb.set_status(TopologyStatus.KILLED);
        TopologyActionOptions tao = new TopologyActionOptions();
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(delay);
        tao.set_kill_options(opts);
        sb.set_topology_action_options(tao);
        sb.set_component_executors(Collections.emptyMap());
        sb.set_component_debug(Collections.emptyMap());
        return sb;
    };
    
//    @SuppressWarnings("unchecked")
//    public static final TopologyStateTransition REBALANCE = (rebalanceOpts, nimbus, topoId, base) -> {
//        //TODO replace the list with an actual class (possibly just the RebalanceOpts)
//        List<Object> trasitionOpts = (List<Object>) rebalanceOpts;
//        int delay = 0;
//        if (trasitionOpts.get(0) != null) {
//            delay = ((Number)trasitionOpts.get(0)).intValue();
//        } else {
//            delay = Utils.getInt(Nimbus.readTopoConf(nimbus.getConf(), topoId, nimbus.getBlobStore()).get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
//        }
//        Integer numWorkers = (Integer)trasitionOpts.get(1);
//        Integer  = (Integer)trasitionOpts.get(1);
//        nimbus.delayEvent(topoId, delay, TopologyActions.DO_REBALANCE, null);
//        StormBase sb = new StormBase();
//        sb.set_status(TopologyStatus.KILLED);
//        TopologyActionOptions tao = new TopologyActionOptions();
//        KillOptions opts = new KillOptions();
//        opts.set_wait_secs(delay);
//        tao.set_kill_options(opts);
//        sb.set_topology_action_options(tao);
//        sb.set_component_executors(Collections.emptyMap());
//        sb.set_component_debug(Collections.emptyMap());
//        return sb;
//    };
//    
    
    
//    (defn rebalance-transition []
//            (reify TopologyStateTransition
//              (transition [this [time num-workers executor-overrides] nimbus storm-id storm-base]
//                (let [delay (if time
//                              time
//                              (get (clojurify-structure (Nimbus/readTopoConf (.getConf nimbus) storm-id (.getBlobStore nimbus)))
//                                   TOPOLOGY-MESSAGE-TIMEOUT-SECS))
//                      rbo (doto (RebalanceOptions.) (.set_wait_secs (int delay)))]
//                  (.delayEvent nimbus
//                               storm-id
//                               delay
//                               TopologyActions/DO_REBALANCE
//                               nil)
//
//                  (if num-workers (.set_num_workers rbo (int num-workers)))
//                  (if executor-overrides (.set_num_executors rbo (map-val int executor-overrides)))
//
//                  (doto (org.apache.storm.generated.StormBase.)
//                     (.set_status TopologyStatus/REBALANCING)
//                     (.set_prev_status (.get_status storm-base))
//                     (.set_topology_action_options
//                       (doto (TopologyActionOptions.)
//                         (.set_rebalance_options rbo)))
//                     (.set_component_executors {})
//                     (.set_component_debug {}))))))
}
