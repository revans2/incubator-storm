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
package backtype.storm.scheduler.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchedulerUtils {
    public static IConfigLoader getConfigLoader(Map conf, String loaderClassConfig) {
        if (conf.get(loaderClassConfig) != null) {
            try {
                String clazz = (String)conf.get(loaderClassConfig);
                if (clazz != null) {
                    IConfigLoader loader = (IConfigLoader)Class.forName(clazz).newInstance();
                    loader.prepare(conf);
                    return loader;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
