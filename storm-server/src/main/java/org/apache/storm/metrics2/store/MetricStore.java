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

import java.util.HashMap;
import java.util.Map;

public interface MetricStore {

    /**
     * Create RocksDB instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     * @return this for invocation chaining
     * @throws MetricException on preparation error
     */
    MetricStore prepare(Map config) throws MetricException;

    /**
     * Stores metrics in the store
     *
     * @param metric Metric to store
     */
    void insert(Metric metric);

    /**
     * Scans all metrics in the store
     *
     * @return List<Double> metrics in store
     */
    void scan(IAggregator agg);

    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     *
     * @param settings map of settings to search by
     * @return List<Double> metrics in store
     */
    void scan(HashMap<String, Object> settings, IAggregator agg);

    // get by matching a key exactly
    boolean populateValue(Metric metric);

    // remove things matching settings, kind of like a scan but much scarier
    void remove(HashMap<String, Object> settings);
}
