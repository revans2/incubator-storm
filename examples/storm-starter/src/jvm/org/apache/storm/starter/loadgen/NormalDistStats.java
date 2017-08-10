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
import java.util.Random;

/**
 * Stats related to something with a normal distribution, and a way to randomly simulate it.
 */
public class NormalDistStats implements Serializable {
    public final double mean;
    public final double stdev;
    public final double min;
    public final double max;

    public NormalDistStats(double mean, double stdev, double min, double max) {
        this.mean = mean;
        this.stdev = stdev;
        this.min = min;
        this.max = max;
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
