/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.metrics2.store;

import java.util.Map;

/**
 * This class defines the method to generate and configure the MetricStore
 * class based on the configuration map
 *
 * @author Austin Chung <achung13@illinois.edu>
 * @author Abhishek Deep Nigam <adn5327@gmail.com>
 * @author Naren Dasan <naren@narendasan.com>
 */

public class MetricStoreConfig {

    /**
     * Configures metrics store to use the class specified in the conf map
     * @param conf Storm config map
     * @return MetricStore prepared store
     */
    public static MetricStore configure(Map conf) {

        try {
            validateConfig(conf);
        } catch (MetricException e){
            System.out.println(e);
        }

        //Uses Utils.getString in storm
        String storeClass = conf.get("storm.metrics2.store.class").toString();
        //Replace with Utils.newInstance
        MetricStore store;
        try {
            Class<?> klass = (Class.forName(storeClass));
            store = (MetricStore)(Class.forName(storeClass)).newInstance();
            store.prepare(conf);
            return store;

        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    /**
     * Validates top level configurations of the metrics conf map
     * @param conf Storm config map
     * @throws MetricException
     */
    private static void validateConfig(Map conf) throws MetricException{
        if (!(conf.containsKey("storm.metrics2.store.class"))) {
            throw new MetricException("Not a valid metrics configuration - Missing store type");
        }
    }
}
