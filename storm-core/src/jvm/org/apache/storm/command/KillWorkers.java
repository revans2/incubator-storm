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
package org.apache.storm.command;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import org.apache.storm.daemon.supervisor.StandaloneSupervisor;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.utils.ConfigUtils;

public class KillWorkers {
    public static void main(String [] args) throws Exception {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        if (Utils.getBoolean(conf.get("storm.use.lagacy.supervisor"), false)) {
            Class<?> kw = Class.forName("backtype.storm.command.kill_workers");
            Method main = kw.getMethod("main", String[].class);
            main.invoke(null, (Object)args);
        } else {
            conf.put(Config.STORM_LOCAL_DIR, new File((String)conf.get(Config.STORM_LOCAL_DIR)).getCanonicalPath());
            try (Supervisor supervisor = new Supervisor(conf, null, new StandaloneSupervisor())) {
                supervisor.shutdownAllWorkers();
            }
        }
    }
}
