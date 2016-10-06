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

import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyStatus;

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
    
    public StormBase transition(Object argument, Nimbus nimbus, String topoId, TopologyStatus status);
    
    public static final TopologyStateTransition NOOP = (arg, nimbus, topoId, status) -> null;
    public static final TopologyStateTransition INACTIVE = (arg, nimbus, topoId, status) -> make(TopologyStatus.INACTIVE);
    public static final TopologyStateTransition ACTIVE = (arg, nimbus, topoId, status) -> make(TopologyStatus.ACTIVE);
}
