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

package org.apache.storm.starter.loadgen;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.storm.utils.ObjectReader;

/**
 * Stats related to something with a normal distribution, and a way to randomly simulate it.
 */
public class NormalDistStats implements Serializable {
    public final double mean;
    public final double stdev;
    public final double min;
    public final double max;

    /**
     * Read the stats from a config.
     * @param conf the config.
     * @return the corresponding stats.
     */
    public static NormalDistStats fromConf(Map<String, Object> conf) {
        return fromConf(conf, null);
    }

    /**
     * Read the stats from a config.
     * @param conf the config.
     * @param def the default mean.
     * @return the corresponding stats.
     */
    public static NormalDistStats fromConf(Map<String, Object> conf, Double def) {
        if (conf == null) {
            conf = Collections.emptyMap();
        }
        double mean = ObjectReader.getDouble(conf.get("mean"), def);
        double stddev = ObjectReader.getDouble(conf.get("stddev"), mean/4);
        double min = ObjectReader.getDouble(conf.get("min"), 0.0);
        double max = ObjectReader.getDouble(conf.get("max"), Double.MAX_VALUE);
        return new NormalDistStats(mean, stddev, min, max);
    }

    public NormalDistStats(double mean, double stdev, double min, double max) {
        this.mean = mean;
        this.stdev = stdev;
        this.min = min;
        this.max = max;
    }

    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("mean", mean);
        ret.put("stddev", stdev);
        ret.put("min", min);
        ret.put("max", max);
        return ret;
    }

    /**
     * Generate a random number that follows the statistical distribution
     * @param rand the random number generator to use
     * @return the next number that should follow the statistical distribution.
     */
    public double nextRandom(Random rand) {
        return Math.max(Math.min((rand.nextGaussian() * stdev) + mean, max), min);
    }
}
